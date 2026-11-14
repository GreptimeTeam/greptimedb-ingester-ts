import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  AbortedError,
  Client,
  ConfigError,
  DataType,
  Precision,
  ServerError,
  StateError,
  Table,
  TimeoutError,
  TransportError,
  ValueError,
} from '../../src/web/index.js';
import {
  GreptimeRequestSchema,
  GreptimeResponseSchema,
} from '../../src/generated/greptime/v1/database_pb.js';

function table(): Table {
  return Table.new('web_test')
    .addFieldColumn('value', DataType.Binary)
    .addTimestampColumn('ts', Precision.Millisecond)
    .addRow([new Uint8Array([1, 2]), 123]);
}

function frame(flag: number, body: Uint8Array): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(5 + body.length);
  bytes[0] = flag;
  new DataView(bytes.buffer).setUint32(1, body.length);
  bytes.set(body, 5);
  return bytes;
}

function response(status = 0): Response {
  const message = toBinary(
    GreptimeResponseSchema,
    create(GreptimeResponseSchema, {
      header: { status: { statusCode: status, errMsg: status ? 'rejected' : '' } },
      response: { case: 'affectedRows', value: { value: 1 } },
    }),
  );
  const data = frame(0, message);
  const trailer = frame(128, new TextEncoder().encode('grpc-status: 0\r\n'));
  const body = new Uint8Array(data.length + trailer.length);
  body.set(data);
  body.set(trailer, data.length);
  return new Response(body, { headers: { 'content-type': 'application/grpc-web+proto' } });
}

const config = { endpoint: 'http://localhost:4001', retry: { maxAttempts: 1 } };

afterEach(() => vi.unstubAllGlobals());

describe('gRPC-Web client', () => {
  it('encodes the request, credentials and hints and reads framed responses', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(response());
    vi.stubGlobal('fetch', fetchMock);
    const client = new Client({
      ...config,
      endpoint: 'https://example.com/proxy/',
      database: 'metrics',
      auth: { kind: 'basic', username: 'u', password: 'p' },
    });
    expect(await client.write(table(), { hints: { append_mode: 'true' } })).toEqual({ value: 1 });
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe('https://example.com/proxy/greptime.v1.GreptimeDatabase/Handle');
    expect(new Headers(init?.headers).get('x-greptime-hints')).toBe('append_mode=true');
    expect(new Headers(init?.headers).get('content-type')).toBe('application/grpc-web+proto');
    const request = fromBinary(GreptimeRequestSchema, (init?.body as Uint8Array).subarray(5));
    expect(request.header?.dbname).toBe('metrics');
    expect(request.header?.authorization?.authScheme).toMatchObject({
      case: 'basic',
      value: { username: 'u', password: 'p' },
    });
    expect(request.request.case).toBe('rowInserts');
    if (request.request.case !== 'rowInserts') throw new Error('expected row inserts');
    const insert = request.request.value.inserts[0]!;
    expect(insert.tableName).toBe('web_test');
    expect(insert.rows?.rows[0]?.values.map((value) => value.valueData)).toEqual([
      { case: 'binaryValue', value: new Uint8Array([1, 2]) },
      { case: 'timestampMillisecondValue', value: 123n },
    ]);
  });

  it('maps trailers-only gRPC failures and retries transient failures', async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(null, {
          headers: {
            'content-type': 'application/grpc-web+proto',
            'grpc-status': '14',
            'grpc-message': 'unavailable',
          },
        }),
      )
      .mockResolvedValueOnce(response());
    vi.stubGlobal('fetch', fetchMock);
    const client = new Client({
      ...config,
      retry: { maxAttempts: 2, initialBackoffMs: 0, mode: 'conservative' },
    });
    expect(await client.write(table())).toEqual({ value: 1 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('preserves server business errors without retrying invalid arguments', async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockImplementation(() => Promise.resolve(response(1004)));
    vi.stubGlobal('fetch', fetchMock);
    await expect(new Client({ endpoint: config.endpoint }).write(table())).rejects.toMatchObject({
      constructor: ServerError,
      statusCode: 1004,
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('rejects malformed responses as transport errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn<typeof fetch>().mockResolvedValue(
        new Response(new Uint8Array([0, 0, 0]), {
          headers: { 'content-type': 'application/grpc-web+proto' },
        }),
      ),
    );
    await expect(new Client(config).write(table())).rejects.toBeInstanceOf(TransportError);
  });

  it('validates hints and aborts before sending', async () => {
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal('fetch', fetchMock);
    const client = new Client(config);
    await expect(client.write(table(), { hints: { a: 'b,c' } })).rejects.toBeInstanceOf(ValueError);
    await expect(client.write(table(), { signal: AbortSignal.abort() })).rejects.toBeInstanceOf(
      AbortedError,
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each(['abort', 'timeout', 'close'] as const)(
    'cancels in-flight fetch on %s without retrying',
    async (action) => {
      let started!: () => void;
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      const fetchMock = vi.fn<typeof fetch>().mockImplementation(
        (_url, init) =>
          new Promise((_resolve, reject) => {
            init?.signal?.addEventListener(
              'abort',
              () => {
                reject(new DOMException('aborted', 'AbortError'));
              },
              {
                once: true,
              },
            );
            started();
          }),
      );
      vi.stubGlobal('fetch', fetchMock);
      const client = new Client({ endpoint: config.endpoint });
      const controller = new AbortController();
      const pending = client.write(table(), {
        signal: controller.signal,
        timeoutMs: action === 'timeout' ? 20 : 5000,
      });
      const assertion = expect(pending).rejects.toBeInstanceOf(
        action === 'timeout' ? TimeoutError : AbortedError,
      );
      await ready;
      if (action === 'abort') controller.abort();
      if (action === 'close') await client.close();
      await assertion;
      expect(fetchMock).toHaveBeenCalledTimes(1);
      if (action === 'close') {
        await expect(client.write(table())).rejects.toBeInstanceOf(StateError);
        await expect(client.writeObject([])).rejects.toBeInstanceOf(StateError);
      }
    },
  );

  it.each([
    'localhost:4001',
    'ftp://example.com',
    'http://user:pw@example.com',
    'http://example.com?db=x',
  ])('rejects invalid endpoint %s', (endpoint) => {
    expect(() => new Client({ endpoint })).toThrow(ConfigError);
  });
});
