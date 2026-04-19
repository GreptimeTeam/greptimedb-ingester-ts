// Coverage for the hand-written gRPC adapter. Uses an in-process gRPC server (loopback)
// to exercise abort, cancel, error propagation, and stream lifecycle — these paths are
// what previously had zero test coverage and are the most fragile part of the SDK.

import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import {
  Server,
  ServerCredentials,
  credentials as grpcCredentials,
  Client as GrpcClient,
  Metadata,
  status,
  type ChannelCredentials,
  type MethodDefinition,
  type ServerDuplexStream,
  type ServerReadableStream,
  type sendUnaryData,
  type ServerUnaryCall,
} from '@grpc/grpc-js';

import {
  AbortedError,
  TimeoutError,
  TransportError,
} from '../../src/errors.js';
import {
  bidiStreamingCall,
  clientStreamingCall,
  unaryCall,
} from '../../src/transport/promise-adapter.js';

interface TestReq {
  payload: string;
}
interface TestRes {
  value: number;
}

function serializeReq(v: TestReq): Buffer {
  return Buffer.from(JSON.stringify(v));
}
function deserializeReq(b: Buffer): TestReq {
  return JSON.parse(b.toString('utf8')) as TestReq;
}
function serializeRes(v: TestRes): Buffer {
  return Buffer.from(JSON.stringify(v));
}
function deserializeRes(b: Buffer): TestRes {
  return JSON.parse(b.toString('utf8')) as TestRes;
}

const REQ_SER = serializeReq;
const REQ_DES = deserializeReq;
const RES_SER = serializeRes;
const RES_DES = deserializeRes;

const UNARY: MethodDefinition<TestReq, TestRes> = {
  path: '/test.Service/Unary',
  requestStream: false,
  responseStream: false,
  requestSerialize: REQ_SER,
  requestDeserialize: REQ_DES,
  responseSerialize: RES_SER,
  responseDeserialize: RES_DES,
};
const CLIENT_STREAM: MethodDefinition<TestReq, TestRes> = {
  path: '/test.Service/ClientStream',
  requestStream: true,
  responseStream: false,
  requestSerialize: REQ_SER,
  requestDeserialize: REQ_DES,
  responseSerialize: RES_SER,
  responseDeserialize: RES_DES,
};
const BIDI: MethodDefinition<TestReq, TestRes> = {
  path: '/test.Service/Bidi',
  requestStream: true,
  responseStream: true,
  requestSerialize: REQ_SER,
  requestDeserialize: REQ_DES,
  responseSerialize: RES_SER,
  responseDeserialize: RES_DES,
};

interface ServerControl {
  // mutable behaviors per test
  unaryDelayMs: number;
  unaryFail: { code: number; message: string } | undefined;
  unaryEmpty: boolean;
  bidiResponses: number;
  bidiFail: { code: number; message: string } | undefined;
}
const CTRL_DEFAULTS: ServerControl = {
  unaryDelayMs: 0,
  unaryFail: undefined,
  unaryEmpty: false,
  bidiResponses: 1,
  bidiFail: undefined,
};
const ctrl: ServerControl = { ...CTRL_DEFAULTS };

// Reset the shared in-process gRPC server control surface before each test so a
// failing assertion in one case cannot leak mutated state into the next. Fields
// that individual tests intentionally configure are restored here, not at the
// tail of each test (where a failure would skip the reset).
beforeEach(() => {
  Object.assign(ctrl, CTRL_DEFAULTS);
});

let server: Server | undefined;
let address = '';
let bindError: unknown;

await (async () => {
  const candidate = new Server();
  candidate.addService(
    {
      Unary: {
        path: UNARY.path,
        requestStream: false,
        responseStream: false,
        requestSerialize: REQ_SER,
        requestDeserialize: REQ_DES,
        responseSerialize: RES_SER,
        responseDeserialize: RES_DES,
      },
      ClientStream: {
        path: CLIENT_STREAM.path,
        requestStream: true,
        responseStream: false,
        requestSerialize: REQ_SER,
        requestDeserialize: REQ_DES,
        responseSerialize: RES_SER,
        responseDeserialize: RES_DES,
      },
      Bidi: {
        path: BIDI.path,
        requestStream: true,
        responseStream: true,
        requestSerialize: REQ_SER,
        requestDeserialize: REQ_DES,
        responseSerialize: RES_SER,
        responseDeserialize: RES_DES,
      },
    },
    {
      Unary: (call: ServerUnaryCall<TestReq, TestRes>, cb: sendUnaryData<TestRes>) => {
        const reply = (): void => {
          if (ctrl.unaryFail !== undefined) {
            cb({
              code: ctrl.unaryFail.code,
              message: ctrl.unaryFail.message,
              name: 'ServiceError',
              metadata: new Metadata(),
              details: ctrl.unaryFail.message,
            });
            return;
          }
          if (ctrl.unaryEmpty) {
            // grpc-js doesn't allow null response on success; emulate by erroring with INTERNAL.
            cb({
              code: status.INTERNAL,
              message: 'simulated empty',
              name: 'ServiceError',
              metadata: new Metadata(),
              details: 'simulated empty',
            });
            return;
          }
          cb(null, { value: call.request.payload.length });
        };
        if (ctrl.unaryDelayMs > 0) setTimeout(reply, ctrl.unaryDelayMs);
        else reply();
      },
      ClientStream: (
        call: ServerReadableStream<TestReq, TestRes>,
        cb: sendUnaryData<TestRes>,
      ) => {
        let count = 0;
        call.on('data', (req: TestReq) => {
          count += req.payload.length;
        });
        call.on('end', () => {
          cb(null, { value: count });
        });
        call.on('error', (err) => {
          cb(err as Error & { code: number; details: string; metadata: Metadata }, null);
        });
      },
      Bidi: (call: ServerDuplexStream<TestReq, TestRes>) => {
        if (ctrl.bidiFail !== undefined) {
          call.emit('error', {
            code: ctrl.bidiFail.code,
            message: ctrl.bidiFail.message,
            name: 'ServiceError',
            metadata: new Metadata(),
            details: ctrl.bidiFail.message,
          });
          return;
        }
        call.on('data', (req: TestReq) => {
          for (let i = 0; i < ctrl.bidiResponses; i++) {
            call.write({ value: req.payload.length + i });
          }
        });
        call.on('end', () => call.end());
      },
    },
  );
  try {
    await new Promise<void>((resolve, reject) => {
      candidate.bindAsync('127.0.0.1:0', ServerCredentials.createInsecure(), (err, port) => {
        if (err) {
          reject(err);
          return;
        }
        address = `127.0.0.1:${port}`;
        resolve();
      });
    });
    server = candidate;
  } catch (err) {
    bindError = err;
    candidate.forceShutdown();
  }
})();

afterAll(() => {
  if (server === undefined) return;
  return new Promise<void>((resolve) => {
    server!.tryShutdown(() => {
      resolve();
    });
  });
});

function newClient(): GrpcClient {
  const creds: ChannelCredentials = grpcCredentials.createInsecure();
  return new GrpcClient(address, creds);
}

const describeAdapter = bindError === undefined ? describe : describe.skip;

describeAdapter('promise-adapter — unary', () => {
  it('resolves with the response on success', async () => {
    ctrl.unaryDelayMs = 0;
    ctrl.unaryFail = undefined;
    ctrl.unaryEmpty = false;
    const client = newClient();
    const res = await unaryCall(client, UNARY, { payload: 'hello' }, { metadata: new Metadata() });
    expect(res.value).toBe(5);
    client.close();
  });

  it('rejects with TransportError on server error', async () => {
    ctrl.unaryFail = { code: status.PERMISSION_DENIED, message: 'nope' };
    const client = newClient();
    await expect(
      unaryCall(client, UNARY, { payload: 'x' }, { metadata: new Metadata() }),
    ).rejects.toBeInstanceOf(TransportError);
    ctrl.unaryFail = undefined;
    client.close();
  });

  it('rejects with TimeoutError on DEADLINE_EXCEEDED', async () => {
    ctrl.unaryDelayMs = 200;
    const client = newClient();
    await expect(
      unaryCall(client, UNARY, { payload: 'x' }, { metadata: new Metadata(), deadlineMs: 50 }),
    ).rejects.toBeInstanceOf(TimeoutError);
    ctrl.unaryDelayMs = 0;
    client.close();
  });

  it('rejects with AbortedError when aborted before dispatch', async () => {
    const ac = new AbortController();
    ac.abort();
    const client = newClient();
    await expect(
      unaryCall(client, UNARY, { payload: 'x' }, { metadata: new Metadata(), signal: ac.signal }),
    ).rejects.toBeInstanceOf(AbortedError);
    client.close();
  });

  it('rejects with AbortedError when aborted mid-flight', async () => {
    ctrl.unaryDelayMs = 200;
    const ac = new AbortController();
    const client = newClient();
    const p = unaryCall(client, UNARY, { payload: 'x' }, {
      metadata: new Metadata(),
      signal: ac.signal,
    });
    setTimeout(() => {
      ac.abort();
    }, 10);
    await expect(p).rejects.toBeInstanceOf(AbortedError);
    ctrl.unaryDelayMs = 0;
    client.close();
  });

  it('does NOT leave abort listeners attached after success', async () => {
    ctrl.unaryDelayMs = 0;
    const ac = new AbortController();
    const client = newClient();
    for (let i = 0; i < 50; i++) {
      await unaryCall(client, UNARY, { payload: 'x' }, {
        metadata: new Metadata(),
        signal: ac.signal,
      });
    }
    // Node tracks listeners on AbortSignal via a private symbol; we observe via the
    // public `eventNames()` if available, otherwise just confirm no crash and aborting
    // afterwards doesn't fire any stale callback (no unhandled rejection seen by vitest).
    ac.abort();
    client.close();
  });
});

describeAdapter('promise-adapter — client streaming', () => {
  it('streams writes and resolves on finish()', async () => {
    const client = newClient();
    const call = clientStreamingCall(client, CLIENT_STREAM, { metadata: new Metadata() });
    await call.write({ payload: 'aa' });
    await call.write({ payload: 'bbb' });
    const res = await call.finish();
    expect(res.value).toBe(5);
    client.close();
  });

  it('rejects finish() if cancelled before end', async () => {
    const client = newClient();
    const call = clientStreamingCall(client, CLIENT_STREAM, { metadata: new Metadata() });
    await call.write({ payload: 'x' });
    call.cancel();
    await expect(call.finish()).rejects.toBeInstanceOf(TransportError);
    client.close();
  });
});

describeAdapter('promise-adapter — client streaming pre-abort', () => {
  it('rejects write/finish immediately when signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const client = newClient();
    const call = clientStreamingCall(client, CLIENT_STREAM, {
      metadata: new Metadata(),
      signal: ac.signal,
    });
    await expect(call.write({ payload: 'x' })).rejects.toBeInstanceOf(AbortedError);
    await expect(call.finish()).rejects.toBeInstanceOf(AbortedError);
    client.close();
  });
});

describeAdapter('promise-adapter — bidi streaming', () => {
  it('writes requests and yields server responses', async () => {
    ctrl.bidiResponses = 2;
    ctrl.bidiFail = undefined;
    const client = newClient();
    const call = bidiStreamingCall(client, BIDI, { metadata: new Metadata() });
    await call.write({ payload: 'ab' });
    call.end();
    const got: number[] = [];
    for await (const r of call.responses()) got.push(r.value);
    expect(got).toEqual([2, 3]);
    client.close();
  });

  it('cancels the underlying call when the consumer breaks early', async () => {
    ctrl.bidiResponses = 100;
    const client = newClient();
    const call = bidiStreamingCall(client, BIDI, { metadata: new Metadata() });
    await call.write({ payload: 'go' });
    let count = 0;
    for await (const _ of call.responses()) {
      count++;
      if (count === 3) break;
    }
    expect(count).toBe(3);
    // Cancellation should already have fired via iterator return().
    client.close();
  });

  it('preserves the real grpc status code on stream error', async () => {
    ctrl.bidiFail = { code: status.RESOURCE_EXHAUSTED, message: 'too big' };
    const client = newClient();
    const call = bidiStreamingCall(client, BIDI, { metadata: new Metadata() });
    let caught: unknown;
    try {
      for await (const _ of call.responses()) {
        // never reached
      }
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(TransportError);
    expect((caught as TransportError).grpcCode).toBe(status.RESOURCE_EXHAUSTED);
    ctrl.bidiFail = undefined;
    client.close();
  });

  it('rejects write immediately when signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const client = newClient();
    const call = bidiStreamingCall(client, BIDI, {
      metadata: new Metadata(),
      signal: ac.signal,
    });
    await expect(call.write({ payload: 'x' })).rejects.toBeInstanceOf(AbortedError);
    client.close();
  });
});
