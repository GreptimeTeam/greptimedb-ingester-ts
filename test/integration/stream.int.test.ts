/*
 * Integration: HandleRequests client-streaming end-to-end. Pushes many batches through
 * a single stream, then half-closes and verifies the aggregate AffectedRows.
 * Run: INTEGRATION=1 pnpm test:integration
 */

import { afterAll, describe, expect, it } from 'vitest';
import { Client, DataType, Precision, SchemaError, Table } from '../../src/index.js';

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
const httpEndpoint = process.env.GREPTIMEDB_HTTP ?? 'http://127.0.0.1:4000';

const client = new Client(Client.create(endpoint).withDatabase('public').build());

afterAll(async () => {
  await client.close();
});

async function queryCount(table: string): Promise<number> {
  const res = await fetch(`${httpEndpoint}/v1/sql?db=public`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: `sql=${encodeURIComponent(`SELECT COUNT(*) FROM ${table}`)}`,
  });
  const json = (await res.json()) as { output: { records: { rows: number[][] } }[] };
  return json.output[0]?.records.rows[0]?.[0] ?? 0;
}

function buildTable(name: string): Table {
  return Table.new(name)
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
}

describe('HandleRequests streaming integration', () => {
  it('streams many batches and reports the aggregate row count', async () => {
    const tableName = `ts_stream_int_${Date.now()}`;
    const stream = client.createStreamWriter();
    const batches = 10;
    const rowsPerBatch = 250;
    for (let b = 0; b < batches; b++) {
      const t = buildTable(tableName);
      for (let i = 0; i < rowsPerBatch; i++) {
        t.addRow([`h-${i % 4}`, Math.random(), Date.now() + b * rowsPerBatch + i]);
      }
      await stream.write(t);
    }
    const res = await stream.finish();
    expect(res.value).toBe(batches * rowsPerBatch);
    expect(await queryCount(tableName)).toBe(batches * rowsPerBatch);
  });

  it('rejects writes after finish() and after cancel()', async () => {
    const tableName = `ts_stream_state_${Date.now()}`;
    const stream = client.createStreamWriter();
    await stream.write(buildTable(tableName).addRow(['x', 1, Date.now()]));
    await stream.finish();
    await expect(
      stream.write(buildTable(tableName).addRow(['y', 2, Date.now()])),
    ).rejects.toBeInstanceOf(SchemaError);

    const stream2 = client.createStreamWriter();
    stream2.cancel();
    await expect(
      stream2.write(buildTable(tableName).addRow(['z', 3, Date.now()])),
    ).rejects.toBeInstanceOf(SchemaError);
  });
});
