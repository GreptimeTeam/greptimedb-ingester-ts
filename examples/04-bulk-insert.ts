/*
 * Example 04 — bulk insert via Arrow Flight DoPut.
 *
 * Bulk path prerequisites (enforced by the server, not the client):
 *   (a) The table must already exist.
 *   (b) The passed schema must exactly match the server-side table schema.
 *
 * We first send a single unary insert (which supports auto-create-table) with a sample
 * row, which guarantees the table exists with the right schema. Then we switch to bulk
 * for the high-throughput portion.
 *
 * Required env: GreptimeDB at localhost:4001.
 *
 * Run: pnpm example 04-bulk-insert
 */

import { Client, DataType, Precision, Table, type TableSchema } from '../src/index.js';

const TABLE = 'bulk_demo';
const ROWS = 100_000;
const BATCH = 5_000;

function buildTable(): Table {
  return Table.new(TABLE)
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
}

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const client = new Client(Client.create(endpoint).withDatabase('public').build());

  try {
    // Step 1: unary write to ensure the table exists with the expected schema.
    const probe = buildTable().addRow(['probe', 0, Date.now()]);
    await client.write(probe);
    console.log(`table ${TABLE} ready`);

    // Step 2: open bulk writer with the matching schema.
    const schema: TableSchema = buildTable().schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 8 });

    const startNs = process.hrtime.bigint();
    for (let batch = 0; batch < ROWS / BATCH; batch++) {
      const rows: unknown[][] = [];
      for (let i = 0; i < BATCH; i++) {
        const id = batch * BATCH + i;
        rows.push([`host-${id % 32}`, Math.random() * 100, Date.now() + id]);
      }
      await bulk.writeRows({ kind: 'rows', rows });
    }
    const summary = await bulk.finish();
    const elapsedMs = Number(process.hrtime.bigint() - startNs) / 1e6;
    const rps = summary.totalAffectedRows / (elapsedMs / 1000);
    console.log(
      `bulk: ${summary.totalRequests} reqs, ${summary.totalAffectedRows} rows in ` +
        `${elapsedMs.toFixed(1)}ms (${rps.toFixed(0)} rows/s)`,
    );
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('bulk insert failed:', err);
  process.exit(1);
});
