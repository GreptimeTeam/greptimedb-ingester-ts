/*
 * Example 07 — multiple endpoints with pluggable selection and failover.
 *
 * The ingester keeps one channel per peer and routes each unary call through the configured
 * EndpointSelector. The default is random; here we use a health-aware selector that ejects an
 * endpoint after repeated transport failures and re-admits it after a back-off window. On
 * retry, a peer that just failed is excluded so a single dead endpoint can't burn the budget.
 *
 * Streaming and bulk are not auto-retried: on a transport error, "rebuild" simply means calling
 * createStreamWriter() / createBulkStreamWriter() again — the selector re-picks a healthy peer.
 *
 * Run: pnpm example 07-multi-endpoint-lb
 *   (configure GREPTIMEDB_ENDPOINTS="host1:4001,host2:4001,host3:4001" — the default
 *    demonstrates the API with three copies of the same local endpoint.)
 */

import {
  Client,
  DataType,
  outlierDetectingSelector,
  Precision,
  Table,
  type StreamWriter,
} from '../src/index.js';

function parseEndpoints(): string[] {
  const raw = process.env.GREPTIMEDB_ENDPOINTS ?? 'localhost:4001,localhost:4001,localhost:4001';
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

function row(i: number): Table {
  return Table.new('lb_demo')
    .addTagColumn('host', DataType.String)
    .addFieldColumn('n', DataType.Int64)
    .addTimestampColumn('ts', Precision.Millisecond)
    .addRow([`call-${i}`, BigInt(i), Date.now() + i]);
}

async function main(): Promise<void> {
  const endpoints = parseEndpoints();
  const client = new Client(
    Client.create(endpoints[0] ?? 'localhost:4001')
      .withEndpoints(...endpoints.slice(1))
      .withDatabase('public')
      .withEndpointSelector(outlierDetectingSelector({ consecutiveFailures: 3 }))
      .build(),
  );

  try {
    for (let i = 0; i < 10; i++) {
      const r = await client.write(row(i));
      console.log(`unary call ${i}: ${r.value} rows`);
    }

    // Streaming rebuild pattern: on a transport error the session is dead — open a fresh writer
    // (the selector steers away from the failed endpoint) and resume from the next batch.
    let stream: StreamWriter = client.createStreamWriter();
    try {
      await stream.write(row(100));
    } catch {
      stream = client.createStreamWriter();
      await stream.write(row(100));
    }
    const summary = await stream.finish();
    console.log(`stream finished on ${stream.endpoint}: ${summary.value} rows`);
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('lb example failed:', err);
  process.exit(1);
});
