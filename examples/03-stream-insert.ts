/*
 * Example 03 — streaming insert via HandleRequests.
 *
 * Purpose: open a single client-streaming connection, push many batches through it, then
 *   half-close to collect the aggregate row count. Better throughput than unary for
 *   sustained writes on a stable schema.
 *
 * Required env: GreptimeDB at localhost:4001.
 *
 * Run: pnpm example 03-stream-insert
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

const ROWS = 10_000;
const BATCH = 100;

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const client = new Client(Client.create(endpoint).withDatabase('public').build());

  const stream = client.createStreamWriter();
  const startNs = process.hrtime.bigint();
  try {
    for (let batch = 0; batch < ROWS / BATCH; batch++) {
      const t = Table.new('stream_demo')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('value', DataType.Float64)
        .addTimestampColumn('ts', Precision.Millisecond);
      for (let i = 0; i < BATCH; i++) {
        const id = batch * BATCH + i;
        t.addRow([`host-${id % 8}`, Math.random() * 100, Date.now() + id]);
      }
      await stream.write(t);
    }
    const result = await stream.finish();
    const elapsedMs = Number(process.hrtime.bigint() - startNs) / 1e6;
    const rps = result.value / (elapsedMs / 1000);
    console.log(
      `streamed ${result.value} rows in ${elapsedMs.toFixed(1)}ms (${rps.toFixed(0)} rows/s)`,
    );
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('stream failed:', err);
  process.exit(1);
});
