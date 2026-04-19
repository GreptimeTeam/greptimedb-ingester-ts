/*
 * Example 08 — AbortSignal + custom retry policy.
 *
 * Purpose: demonstrate aborting a slow write via AbortSignal, and customizing the retry
 *   policy (switching to conservative mode + tightening backoff).
 *
 * Required env: GreptimeDB at localhost:4001.
 *
 * Run: pnpm example 08-abort-and-retry
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const client = new Client(
    Client.create(endpoint)
      .withDatabase('public')
      .withRetry({ mode: 'conservative', maxAttempts: 2, initialBackoffMs: 50, maxBackoffMs: 200 })
      .build(),
  );

  const table = Table.new('abort_demo')
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
  for (let i = 0; i < 100; i++) {
    table.addRow([`h-${i}`, i * 0.5, Date.now() + i]);
  }

  try {
    // Scenario A: normal write with a per-call 5s deadline via AbortSignal
    const ac1 = AbortSignal.timeout(5_000);
    const res = await client.write(table, { signal: ac1 });
    console.log(`wrote ${res.value} rows within 5s deadline`);

    // Scenario B: force an abort during the call and observe it surface as AbortedError
    const ac2 = new AbortController();
    setTimeout(() => {
      ac2.abort();
    }, 1);
    try {
      await client.write(table, { signal: ac2.signal });
      console.log('unexpected success');
    } catch (err: unknown) {
      console.log(`aborted as expected:`, err instanceof Error ? err.message : err);
    }
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('example failed:', err);
  process.exit(1);
});
