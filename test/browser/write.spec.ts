import { createServer, type Server } from 'node:http';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { build } from 'esbuild';
import { test, expect } from '@playwright/test';

// Resolve the published entry point so this also checks package exports and build output.
import type * as Ingester from '@greptime/ingester/web';

declare global {
  interface Window {
    ingester: typeof Ingester;
  }
}

let server: Server | undefined;
let container: StartedTestContainer | undefined;
let origin: string;
let endpoint: string;
let httpEndpoint: string;

test.beforeAll(async () => {
  const bundle = await build({
    stdin: { contents: "export * from '@greptime/ingester/web';", resolveDir: process.cwd() },
    bundle: true,
    platform: 'browser',
    format: 'iife',
    globalName: 'ingester',
    write: false,
    metafile: true,
  });
  expect(
    Object.keys(bundle.metafile.inputs).filter((path) =>
      /grpc-js|apache-arrow|lz4-napi|mongodb-js/.test(path),
    ),
  ).toEqual([]);
  const script = bundle.outputFiles[0]!.text;
  server = createServer((req, res) => {
    res.setHeader('content-type', req.url === '/sdk.js' ? 'application/javascript' : 'text/html');
    res.end(
      req.url === '/sdk.js'
        ? script
        : '<!doctype html><title>Ingester browser test</title><script src="/sdk.js"></script>',
    );
  });
  await new Promise<void>((resolve, reject) => {
    server!.once('error', reject);
    server!.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('missing HTTP server port');
  origin = `http://127.0.0.1:${address.port}`;
  // v1.3.1 has gRPC-Web but not the CORS configuration added in server PR #9374.
  container = await new GenericContainer('greptime/greptimedb:v1.4.0-nightly-20261109')
    .withExposedPorts(4000, 4001)
    .withCopyContentToContainer([
      {
        target: '/etc/greptimedb.toml',
        content: `[grpc]\nbind_addr = "0.0.0.0:4001"\nenable_cors = true\ncors_allowed_origins = ["${origin}"]\n[http]\naddr = "0.0.0.0:4000"\n`,
      },
    ])
    .withCommand([
      'standalone',
      'start',
      '-c',
      '/etc/greptimedb.toml',
      '--user-provider',
      'static_user_provider:cmd:web=test',
    ])
    .withWaitStrategy(Wait.forHttp('/health', 4000).forStatusCode(200))
    .withStartupTimeout(120_000)
    .start();
  endpoint = `http://${container.getHost()}:${container.getMappedPort(4001)}`;
  httpEndpoint = `http://${container.getHost()}:${container.getMappedPort(4000)}`;
});

test.beforeEach(({ page }) => {
  page.on('console', (message) => {
    if (message.type() === 'error') console.error(message.text());
  });
});

test.afterAll(async () => {
  await container?.stop();
  if (server)
    await new Promise<void>((resolve, reject) =>
      server!.close((err) => {
        if (err) reject(err);
        else resolve();
      }),
    );
});

test('writes JSON2 and binary rows across origins with auth and hints', async ({ page }) => {
  await page.goto(origin);
  const tableName = `web_int_${Date.now()}`;
  const result = await page.evaluate(
    async ({ endpoint, tableName }) => {
      const { Client, Table, DataType, Precision } = window.ingester;
      const client = new Client({
        endpoint,
        auth: { kind: 'basic', username: 'web', password: 'test' },
      });
      try {
        return await client.write(
          Table.new(tableName)
            .addFieldColumn('payload', DataType.Json2)
            .addFieldColumn('bytes', DataType.Binary)
            .addTimestampColumn('ts', Precision.Millisecond)
            .addRow([{ source: 'browser' }, new Uint8Array([1, 2, 3]), 123]),
          { hints: { append_mode: 'true' } },
        );
      } finally {
        await client.close();
      }
    },
    { endpoint, tableName },
  );
  expect(result.value).toBe(1);
  const res = await fetch(`${httpEndpoint}/v1/sql?db=public`, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      authorization: `Basic ${Buffer.from('web:test').toString('base64')}`,
    },
    body: new URLSearchParams({ sql: `SELECT COUNT(*) FROM ${tableName}` }),
  });
  const json = (await res.json()) as { output: { records: { rows: number[][] } }[] };
  expect(json.output[0]?.records.rows).toEqual([[1]]);
});

test('surfaces authentication failures in the browser', async ({ page }) => {
  await page.goto(origin);
  const error = await page.evaluate(async (endpoint) => {
    const { Client, Table, Precision, IngesterError } = window.ingester;
    const client = new Client({
      endpoint,
      auth: { kind: 'basic', username: 'web', password: 'wrong' },
      retry: { maxAttempts: 1 },
    });
    try {
      await client.write(
        Table.new('web_bad_auth').addTimestampColumn('ts', Precision.Millisecond).addRow([123]),
      );
      return null;
    } catch (err) {
      if (!(err instanceof IngesterError)) throw err;
      return { kind: err.kind, message: err.message };
    } finally {
      await client.close();
    }
  }, endpoint);
  expect(error?.kind).toBe('transport');
  expect(error?.message).toMatch(/password|auth/i);
});
