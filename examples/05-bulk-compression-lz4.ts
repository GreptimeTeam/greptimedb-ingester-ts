/*
 * Example 05 — request LZ4_FRAME body compression on the bulk path.
 *
 * The SDK injects Arrow IPC BodyCompression on every RecordBatch and the server
 * decompresses transparently via arrow-ipc. LZ4 typically delivers 2–4x bandwidth
 * savings on repetitive time-series data with negligible CPU cost.
 *
 * Requires the optional `lz4-napi` native dependency. It ships prebuilt binaries for
 * common platforms and is listed in `optionalDependencies`, so `pnpm install` picks it
 * up automatically on supported hosts. If missing, the SDK throws a ConfigError
 * pointing you to install it.
 *
 * Run: pnpm example 05-bulk-compression-lz4
 */

import {
  BulkCompression,
  Client,
  DataType,
  Precision,
  Table,
  type TableSchema,
} from '../src/index.js';

const TABLE = 'bulk_compress_demo';

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
    // Bootstrap: ensure the table exists with the expected schema.
    await client.write(buildTable().addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable().schema();
    const bulk = await client.createBulkStreamWriter(schema, {
      compression: BulkCompression.Lz4,
    });

    const rows: unknown[][] = [];
    for (let i = 0; i < 10_000; i++) {
      rows.push([`host-${i % 16}`, Math.sin(i / 10), Date.now() + i]);
    }
    const resp = await bulk.writeRows({ kind: 'rows', rows });
    console.log(`compressed (lz4) rows acked: ${resp.affectedRows}`);

    const summary = await bulk.finish();
    console.log(
      `total requests: ${summary.totalRequests}, total rows: ${summary.totalAffectedRows}`,
    );
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('example failed:', err);
  process.exit(1);
});
