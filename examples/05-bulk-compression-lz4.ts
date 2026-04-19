/*
 * Example 05 — request LZ4 body compression on the bulk path.
 *
 * NOTE: v0.1 limitation. apache-arrow JS 18.x does not implement IPC record batch body
 *   compression, so the SDK currently rejects BulkCompression.Lz4 / Zstd with a
 *   ConfigError. This example demonstrates both the intended API and the expected error.
 *
 * Run: pnpm example 05-bulk-compression-lz4
 */

import {
  BulkCompression,
  Client,
  ConfigError,
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
    await client.write(buildTable().addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable().schema();
    try {
      await client.createBulkStreamWriter(schema, { compression: BulkCompression.Lz4 });
      console.log('unexpected: LZ4 accepted — apache-arrow JS may have added support');
    } catch (err) {
      if (err instanceof ConfigError) {
        console.log('as expected (v0.1 limitation):', err.message);
      } else {
        throw err;
      }
    }

    // Fall back to None — still delivers six-figure throughput.
    const bulk = await client.createBulkStreamWriter(schema, { compression: BulkCompression.None });
    await bulk.writeRows({ kind: 'rows', rows: [['h', 1.5, Date.now()]] });
    const summary = await bulk.finish();
    console.log(`compression-none fallback: ${summary.totalAffectedRows} rows`);
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('example failed:', err);
  process.exit(1);
});
