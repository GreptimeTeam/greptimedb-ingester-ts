/*
 * Example 07 — multiple endpoints with random load balancing.
 *
 * The ingester keeps one channel per peer and picks a peer at random for each unary call
 * (matches Rust `src/load_balance.rs`). Unhealthy peers are skipped automatically by
 * gRPC's own connection state machine on subsequent retries.
 *
 * Run: pnpm example 07-multi-endpoint-lb
 *   (configure GREPTIMEDB_ENDPOINTS="host1:4001,host2:4001,host3:4001" — the default
 *    demonstrates the API with three copies of the same local endpoint.)
 */

import { Client, DataType, Precision, Table } from '../src/index.js';

function parseEndpoints(): string[] {
  const raw = process.env.GREPTIMEDB_ENDPOINTS ?? 'localhost:4001,localhost:4001,localhost:4001';
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

async function main(): Promise<void> {
  const endpoints = parseEndpoints();
  const client = new Client(
    Client.create(endpoints[0] ?? 'localhost:4001')
      .withEndpoints(...endpoints.slice(1))
      .withDatabase('public')
      .build(),
  );

  try {
    for (let i = 0; i < 10; i++) {
      const t = Table.new('lb_demo')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('n', DataType.Int64)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow([`call-${i}`, BigInt(i), Date.now() + i]);
      const r = await client.write(t);
      console.log(`call ${i}: ${r.value} rows`);
    }
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('lb example failed:', err);
  process.exit(1);
});
