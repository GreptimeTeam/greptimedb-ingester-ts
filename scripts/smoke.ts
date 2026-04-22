/*
 * Cross-runtime smoke test — runs on Node, Bun, and Deno.
 *
 * Exercises the critical path that defines "can you even use this SDK on this
 * runtime": construct a Client, do one unary write, query the row count back
 * via the GreptimeDB HTTP SQL API, close the client. Any runtime-level
 * incompatibility in @grpc/grpc-js shows up here as a connection/stream
 * failure before the write resolves.
 *
 * Intentionally uses no `node:*` imports — only the SDK plus the global
 * `fetch` (available on all three runtimes).
 *
 * Env:
 *   GREPTIMEDB_ENDPOINT (default: localhost:4001)  gRPC target
 *   GREPTIMEDB_HTTP     (default: http://127.0.0.1:4000)  HTTP base URL
 *
 * Run:
 *   bun  run scripts/smoke.ts
 *   deno run -A scripts/smoke.ts
 *   pnpm tsx scripts/smoke.ts      # Node path (via tsx)
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

const ROW_COUNT = 5;

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const httpBase = process.env.GREPTIMEDB_HTTP ?? 'http://127.0.0.1:4000';
  const tableName = `smoke_test_${Date.now()}`;

  const client = new Client(Client.create(endpoint).withDatabase('public').build());

  try {
    const now = Date.now();
    const table = Table.new(tableName)
      .addTagColumn('host', DataType.String)
      .addFieldColumn('usage', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond);

    for (let i = 0; i < ROW_COUNT; i++) {
      table.addRow([`server-${i}`, 10 + i, now + i]);
    }

    const result = await client.write(table);
    if (result.value !== ROW_COUNT) {
      throw new Error(`write returned ${result.value} rows, expected ${ROW_COUNT}`);
    }

    const observed = await queryCount(httpBase, tableName);
    if (observed !== ROW_COUNT) {
      throw new Error(`SELECT COUNT(*) returned ${observed}, expected ${ROW_COUNT}`);
    }

    console.log(`smoke ok: wrote ${ROW_COUNT} rows to ${tableName}, verified via HTTP SQL`);
  } finally {
    await client.close();
  }
}

// GreptimeDB HTTP SQL API: POST /v1/sql?db=<db> with form-encoded `sql=...`.
async function queryCount(httpBase: string, table: string): Promise<number> {
  const url = `${httpBase.replace(/\/$/, '')}/v1/sql?db=public`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ sql: `SELECT COUNT(*) FROM ${table}` }),
  });
  if (!res.ok) {
    throw new Error(`HTTP SQL ${res.status}: ${await res.text()}`);
  }
  const body = (await res.json()) as {
    output?: { records?: { rows?: unknown[][] } }[];
  };
  const cell = body.output?.[0]?.records?.rows?.[0]?.[0];
  if (typeof cell !== 'number' && typeof cell !== 'bigint') {
    throw new Error(`unexpected SQL response shape: ${JSON.stringify(body)}`);
  }
  return Number(cell);
}

main().catch((err: unknown) => {
  console.error('smoke failed:', err);
  process.exit(1);
});
