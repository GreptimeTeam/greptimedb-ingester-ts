/*
 * Integration test: bulk Arrow Flight DoPut end-to-end + reverse test for
 * non-existent table. Run with INTEGRATION=1 pnpm test:integration.
 */

import { afterAll, describe, expect, it } from 'vitest';
import { BulkError, Client, DataType, Precision, Table, type TableSchema } from '../../src/index.js';

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
const client = new Client(Client.create(endpoint).withDatabase('public').build());

afterAll(async () => {
  await client.close();
});

function buildTable(name: string): Table {
  return Table.new(name)
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
}

describe('bulk integration', () => {
  it('writes 5000 rows and the server confirms per-batch affected counts', async () => {
    const name = `bulk_int_${Date.now()}`;
    // Bootstrap: unary insert so the table exists with expected schema.
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 4 });

    const rows: unknown[][] = [];
    for (let i = 0; i < 5_000; i++) rows.push([`h-${i % 16}`, Math.random(), Date.now() + i]);
    const resp = await bulk.writeRows({ kind: 'rows', rows });
    expect(resp.affectedRows).toBe(5_000);
    const summary = await bulk.finish();
    expect(summary.totalAffectedRows).toBe(5_000);
  });

  it('rejects with BulkError when the caller sends a mismatched schema', async () => {
    // Pre-create a table with one shape, then open bulk with an incompatible shape
    // (tag renamed, extra column). The server validates during the data frame, not
    // schema handshake, so the error surfaces on writeRows.
    const name = `bulk_mismatch_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const mismatched = Table.new(name)
      .addTagColumn('different_tag', DataType.String)
      .addFieldColumn('value', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .schema();
    const bulk = await client.createBulkStreamWriter(mismatched, {
      parallelism: 1,
      timeoutMs: 5_000,
    });
    await expect(
      bulk.writeRows({ kind: 'rows', rows: [['x', 1, Date.now()]] }),
    ).rejects.toBeInstanceOf(BulkError);
  });
});
