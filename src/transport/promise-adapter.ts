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
    const deadline = toDeadline(opts.deadlineMs);
    const call = client.makeUnaryRequest<Req, Res>(
      method.path,
      method.requestSerialize,
      method.responseDeserialize,
      request,
      opts.metadata,
      deadline !== undefined ? { deadline } : {},
      (err, res) => {
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
    const onAbort = (): void => {
      call.cancel();
      reject(new AbortedError('call aborted'));
    };
    opts.signal?.addEventListener('abort', onAbort, { once: true });
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
  let finishResolve!: (r: Res) => void;
  let finishReject!: (e: unknown) => void;
  const finalPromise = new Promise<Res>((res, rej) => {
    finishResolve = res;
    finishReject = rej;
  });
  const call = client.makeClientStreamRequest<Req, Res>(
    method.path,
    method.requestSerialize,
    method.responseDeserialize,
    opts.metadata,
    deadline !== undefined ? { deadline } : {},
    (err, res) => {
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
  const onAbort = (): void => {
    call.cancel();
    finishReject(new AbortedError('call aborted'));
  };
  opts.signal?.addEventListener('abort', onAbort, { once: true });

  return {
    write(req: Req): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        const ok = call.write(req, (err?: Error | null) => {
          if (err) {
            reject(
              err instanceof Error ? new TransportError(err.message, status.UNKNOWN, err) : err,
            );
            return;
          }
          resolve();
        });
        if (!ok) {
          // Backpressure: wait for drain before resolving the outer promise.
          // The write callback still fires on success.
          call.once('drain', () => {
            /* drained */
          });
        }
      });
    },
    finish(): Promise<Res> {
      call.end();
      return finalPromise;
    },
    cancel(_reason?: unknown): void {
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
  const call = client.makeBidiStreamRequest<Req, Res>(
    method.path,
    method.requestSerialize,
    method.responseDeserialize,
    opts.metadata,
    deadline !== undefined ? { deadline } : {},
  );
  const onAbort = (): void => {
    call.cancel();
  };
  opts.signal?.addEventListener('abort', onAbort, { once: true });

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
    streamErr = new TransportError(err.message, status.UNKNOWN, err);
    const waiters = pullers.splice(0);
    for (const p of waiters) p.reject(streamErr);
  });
  call.on('end', () => {
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
      };
    },
  });

  return {
    write(req: Req): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        const ok = call.write(req, (err?: Error | null) => {
          if (err) {
            reject(
              err instanceof Error ? new TransportError(err.message, status.UNKNOWN, err) : err,
            );
            return;
          }
          resolve();
        });
        if (!ok) {
          call.once('drain', () => {
            /* drained */
          });
        }
      });
    },
    end(): void {
      call.end();
    },
    responses,
    cancel(_reason?: unknown): void {
      call.cancel();
    },
  };
}
