/*
 * Integration test: unary write end-to-end against a real GreptimeDB.
 * Run with: INTEGRATION=1 pnpm test:integration
 * Requires GREPTIMEDB_ENDPOINT env (defaults to localhost:4001).
 */

import { afterAll, describe, expect, it } from 'vitest';
import { Client, DataType, Precision, Table } from '../../src/index.js';

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
  const json = (await res.json()) as {
    output: { records: { rows: number[][] } }[];
  };
  return json.output[0]?.records.rows[0]?.[0] ?? 0;
}

describe('unary write integration', () => {
  it('roundtrips a small batch through Handle', async () => {
    const tableName = `ts_ingester_int_${Date.now()}`;
    const table = Table.new(tableName)
      .addTagColumn('host', DataType.String)
      .addFieldColumn('value', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond);
    for (let i = 0; i < 100; i++) {
      table.addRow([`h-${i % 5}`, Math.random() * 100, Date.now() + i]);
    }
    const res = await client.write(table);
    expect(res.value).toBe(100);
    const count = await queryCount(tableName);
    expect(count).toBe(100);
  });
});
