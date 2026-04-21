// Promise + AsyncIterable adapters over @grpc/grpc-js callback/stream APIs.
// Keep this file boring — it's the single place where we translate grpc-js shapes into
// the rest of the SDK's async contract.

import {
  status,
  type Client as GrpcClient,
  type Metadata,
  type MethodDefinition,
  type ServiceError,
} from '@grpc/grpc-js';

import { AbortedError, TimeoutError, TransportError } from '../errors.js';

export interface CallOptions {
  readonly metadata: Metadata;
  readonly deadlineMs?: number;
  readonly signal?: AbortSignal;
}

function toDeadline(deadlineMs: number | undefined): Date | undefined {
  if (deadlineMs === undefined) return undefined;
  return new Date(Date.now() + deadlineMs);
}

function toTransportError(err: ServiceError): TransportError | TimeoutError {
  if (err.code === status.DEADLINE_EXCEEDED) return new TimeoutError(err.message, err);
  return new TransportError(err.message, err.code, err);
}

/**
 * grpc-js streams emit errors typed as `Error`, but in practice the runtime object
 * carries the same `code`/`details`/`metadata` shape as `ServiceError`. Probe before
 * casting so we preserve the real status code (UNAVAILABLE, RESOURCE_EXHAUSTED, ...)
 * instead of flattening to UNKNOWN.
 */
function streamErrorToTransport(err: Error): TransportError | TimeoutError {
  const maybe = err as Partial<ServiceError>;
  if (typeof maybe.code === 'number') return toTransportError(maybe as ServiceError);
  return new TransportError(err.message, status.UNKNOWN, err);
}

/**
 * Promise-shaped wrapper around `Writable.write(chunk, cb)` that honors backpressure:
 * resolves only after the per-chunk callback fires AND, if the call signaled buffer
 * pressure, after the next 'drain' event. This is the canonical Node stream contract;
 * the previous "attach a noop drain listener" implementation looked correct but did
 * not actually slow the producer.
 */
function writeWithBackpressure<Req>(
  call: {
    write(req: Req, cb: (err?: Error | null) => void): boolean;
    once(ev: 'drain' | 'close', cb: () => void): unknown;
    once(ev: 'error', cb: (err: Error) => void): unknown;
    removeListener(ev: 'drain' | 'close', cb: () => void): unknown;
    removeListener(ev: 'error', cb: (err: Error) => void): unknown;
  },
  req: Req,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let cbDone = false;
    let drainDone = true;
    let settled = false;
    let drainHandler: (() => void) | undefined;
    let errorHandler: ((err: Error) => void) | undefined;
    let closeHandler: (() => void) | undefined;
    const detachDrain = (): void => {
      if (drainHandler !== undefined) {
        call.removeListener('drain', drainHandler);
        drainHandler = undefined;
      }
    };
    const detachTerminal = (): void => {
      if (errorHandler !== undefined) {
        call.removeListener('error', errorHandler);
        errorHandler = undefined;
      }
      if (closeHandler !== undefined) {
        call.removeListener('close', closeHandler);
        closeHandler = undefined;
      }
    };
    const settleReject = (err: Error): void => {
      if (settled) return;
      settled = true;
      detachDrain();
      detachTerminal();
      reject(streamErrorToTransport(err));
    };
    const tryResolve = (): void => {
      if (settled || !cbDone || !drainDone) return;
      settled = true;
      detachDrain();
      detachTerminal();
      resolve();
    };
    errorHandler = (err: Error): void => {
      settleReject(err);
    };
    closeHandler = (): void => {
      settleReject(new Error('stream closed before write completed'));
    };
    call.once('error', errorHandler);
    call.once('close', closeHandler);
    const ok = call.write(req, (err?: Error | null) => {
      if (settled) return;
      if (err) {
        settleReject(err);
        return;
      }
      cbDone = true;
      tryResolve();
    });
    if (!ok) {
      drainDone = false;
      drainHandler = (): void => {
        if (settled) return;
        drainHandler = undefined;
        drainDone = true;
        tryResolve();
      };
      call.once('drain', drainHandler);
    }
  });
}

/** Unary RPC wrapper. Resolves with the response or rejects with a TransportError/TimeoutError. */
export function unaryCall<Req, Res>(
  client: GrpcClient,
  method: MethodDefinition<Req, Res>,
  request: Req,
  opts: CallOptions,
): Promise<Res> {
  return new Promise<Res>((resolve, reject) => {
    if (opts.signal?.aborted === true) {
      reject(new AbortedError('call aborted before dispatch'));
      return;
    }
    const signal = opts.signal;
    let onAbort: (() => void) | undefined;
    const detach = (): void => {
      if (signal !== undefined && onAbort !== undefined) {
        signal.removeEventListener('abort', onAbort);
      }
    };
    const deadline = toDeadline(opts.deadlineMs);
    const call = client.makeUnaryRequest<Req, Res>(
      method.path,
      method.requestSerialize,
      method.responseDeserialize,
      request,
      opts.metadata,
      deadline !== undefined ? { deadline } : {},
      (err, res) => {
        detach();
        if (err) {
          reject(toTransportError(err));
          return;
        }
        if (res === undefined) {
          reject(new TransportError('unary call resolved with no response', status.UNKNOWN));
          return;
        }
        resolve(res);
      },
    );
    if (signal !== undefined) {
      onAbort = (): void => {
        call.cancel();
        reject(new AbortedError('call aborted'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}

/** Handle for a client-streaming call (`HandleRequests`). */
export interface ClientStreamingCall<Req, Res> {
  write(req: Req): Promise<void>;
  finish(): Promise<Res>;
  cancel(reason?: unknown): void;
}

export function clientStreamingCall<Req, Res>(
  client: GrpcClient,
  method: MethodDefinition<Req, Res>,
  opts: CallOptions,
): ClientStreamingCall<Req, Res> {
  const deadline = toDeadline(opts.deadlineMs);
  const signal = opts.signal;
  let finishResolve!: (r: Res) => void;
  let finishReject!: (e: unknown) => void;
  const finalPromise = new Promise<Res>((res, rej) => {
    finishResolve = res;
    finishReject = rej;
  });
  // Pre-abort: if the signal already fired, fail the call before ever opening the
  // socket-level RPC. Matches `unaryCall`'s contract.
  if (signal?.aborted === true) {
    finishReject(new AbortedError('call aborted before dispatch'));
    finalPromise.catch(() => {
      /* swallow unobserved rejection */
    });
    return {
      write: () => Promise.reject(new AbortedError('call aborted before dispatch')),
      finish: () => finalPromise,
      cancel: () => {
        /* already aborted */
      },
    };
  }
  // Attach a no-op rejection handler so `finalPromise` is never "unhandled" if the
  // caller cancels and then never invokes `finish()`. Callers that DO await `finish()`
  // still observe the original rejection unchanged.
  finalPromise.catch(() => {
    /* swallow unobserved rejection */
  });
  let onAbort: (() => void) | undefined;
  const detach = (): void => {
    if (signal !== undefined && onAbort !== undefined) {
      signal.removeEventListener('abort', onAbort);
    }
  };
  const call = client.makeClientStreamRequest<Req, Res>(
    method.path,
    method.requestSerialize,
    method.responseDeserialize,
    opts.metadata,
    deadline !== undefined ? { deadline } : {},
    (err, res) => {
      detach();
      if (err) {
        finishReject(toTransportError(err));
        return;
      }
      if (res === undefined) {
        finishReject(
          new TransportError('client-streaming call ended with no response', status.UNKNOWN),
        );
        return;
      }
      finishResolve(res);
    },
  );
  if (signal !== undefined) {
    onAbort = (): void => {
      call.cancel();
      finishReject(new AbortedError('call aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  }

  return {
    write: (req: Req) => writeWithBackpressure(call, req),
    finish(): Promise<Res> {
      call.end();
      return finalPromise;
    },
    cancel(_reason?: unknown): void {
      detach();
      call.cancel();
    },
  };
}

/** Handle for a bidi-streaming call (`DoPut`). Responses delivered as AsyncIterable. */
export interface BidiStreamingCall<Req, Res> {
  write(req: Req): Promise<void>;
  end(): void;
  responses(): AsyncIterable<Res>;
  cancel(reason?: unknown): void;
}

export function bidiStreamingCall<Req, Res>(
  client: GrpcClient,
  method: MethodDefinition<Req, Res>,
  opts: CallOptions,
): BidiStreamingCall<Req, Res> {
  const deadline = toDeadline(opts.deadlineMs);
  const signal = opts.signal;
  // Pre-abort: short-circuit before opening the gRPC call so already-cancelled signals
  // don't trigger a wasted round-trip. Match unary semantics.
  if (signal?.aborted === true) {
    const aborted = new AbortedError('call aborted before dispatch');
    return {
      write: () => Promise.reject(aborted),
      end: () => {
        /* nothing to do */
      },
      responses: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(aborted),
        }),
      }),
      cancel: () => {
        /* already aborted */
      },
    };
  }
  const call = client.makeBidiStreamRequest<Req, Res>(
    method.path,
    method.requestSerialize,
    method.responseDeserialize,
    opts.metadata,
    deadline !== undefined ? { deadline } : {},
  );
  let onAbort: (() => void) | undefined;
  const detachAbort = (): void => {
    if (signal !== undefined && onAbort !== undefined) {
      signal.removeEventListener('abort', onAbort);
      onAbort = undefined;
    }
  };
  if (signal !== undefined) {
    onAbort = (): void => {
      call.cancel();
    };
    signal.addEventListener('abort', onAbort, { once: true });
  }

  // Buffer incoming responses so `responses()` is a plain AsyncIterable consumers can
  // loop over without worrying about event-emitter semantics.
  const buffer: Res[] = [];
  const pullers: {
    resolve: (value: IteratorResult<Res>) => void;
    reject: (err: unknown) => void;
  }[] = [];
  let ended = false;
  let streamErr: unknown;

  call.on('data', (res: Res) => {
    const p = pullers.shift();
    if (p !== undefined) p.resolve({ value: res, done: false });
    else buffer.push(res);
  });
  call.on('error', (err: Error) => {
    detachAbort();
    streamErr = streamErrorToTransport(err);
    const waiters = pullers.splice(0);
    for (const p of waiters) p.reject(streamErr);
  });
  call.on('end', () => {
    detachAbort();
    ended = true;
    const waiters = pullers.splice(0);
    for (const p of waiters) p.resolve({ value: undefined, done: true });
  });

  const responses = (): AsyncIterable<Res> => ({
    [Symbol.asyncIterator](): AsyncIterator<Res> {
      return {
        next(): Promise<IteratorResult<Res>> {
          if (buffer.length > 0) {
            const v = buffer.shift() as Res;
            return Promise.resolve({ value: v, done: false });
          }
          if (streamErr !== undefined) {
            return Promise.reject(
              streamErr instanceof Error
                ? streamErr
                : new TransportError('bidi stream errored', status.UNKNOWN),
            );
          }
          if (ended) return Promise.resolve({ value: undefined as unknown as Res, done: true });
          return new Promise<IteratorResult<Res>>((resolve, reject) => {
            pullers.push({ resolve, reject });
          });
        },
        // Honor `for await`'s early-exit contract: cancelling the underlying call when
        // the consumer breaks out of the loop prevents leaking gRPC resources.
        return(): Promise<IteratorResult<Res>> {
          detachAbort();
          call.cancel();
          return Promise.resolve({ value: undefined as unknown as Res, done: true });
        },
      };
    },
  });

  return {
    write: (req: Req) => writeWithBackpressure(call, req),
    end(): void {
      call.end();
    },
    responses,
    cancel(_reason?: unknown): void {
      detachAbort();
      call.cancel();
    },
  };
}
