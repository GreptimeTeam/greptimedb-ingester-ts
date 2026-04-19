/*
 * Integration test: bulk Arrow Flight DoPut happy path + client-side state machine
 * checks. Run with INTEGRATION=1 pnpm test:integration.
 *
 * No server-side schema-rejection assertions live here. We empirically verified that
 * GreptimeDB's bulk path validation is inconsistent across server versions: v1.0.0
 * rejects mismatched-type writes on existing columns, but `latest` accepts them and
 * the data ends up in an inconsistent state (write returns ack=1, table later
 * non-queryable). Asserting "server should reject X" would be a flaky test of server
 * internals rather than client-side behavior. We assert client-side guarantees only.
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

  it('rejects writes after cancel() with BulkError (client state machine)', async () => {
    const name = `bulk_cancel_${Date.now()}`;
    // Bootstrap so the bulk handshake succeeds.
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 1 });

    bulk.cancel();
    await expect(
      bulk.writeRows({ kind: 'rows', rows: [['x', 1, Date.now()]] }),
    ).rejects.toBeInstanceOf(BulkError);
  });

  it('rejects writes after finish() with BulkError (client state machine)', async () => {
    const name = `bulk_finish_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 1 });

    await bulk.writeRows({ kind: 'rows', rows: [['x', 1, Date.now()]] });
    await bulk.finish();
    await expect(
      bulk.writeRows({ kind: 'rows', rows: [['y', 2, Date.now()]] }),
    ).rejects.toBeInstanceOf(BulkError);
  });

});
