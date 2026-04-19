/*
 * Example 01 — simple unary insert via Client.write().
 *
 * Purpose: smallest possible example. Build a Table with one tag, one field, and the
 *   timestamp column; add two rows; send in a single unary `Handle` RPC; print the number
 *   of rows the server accepted.
 *
 * Required env: a GreptimeDB reachable at the configured endpoint (default localhost:4001).
 *   Start one via: ./scripts/run-greptimedb.sh
 *
 * Run: pnpm example 01-simple-insert
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const client = new Client(Client.create(endpoint).withDatabase('public').build());

  try {
    const table = Table.new('cpu_usage_demo')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('usage', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['server-01', 75.3, Date.now()])
      .addRow(['server-02', 82.1, Date.now()]);

    const result = await client.write(table);
    console.log(`inserted ${result.value} rows into cpu_usage_demo`);
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('insert failed:', err);
  process.exit(1);
});
