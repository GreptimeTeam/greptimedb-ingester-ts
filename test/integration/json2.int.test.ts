/*
 * Integration: JSON2 row writes. Requires GreptimeDB 1.2.1+.
 * Run with: INTEGRATION=1 pnpm test:integration
 */

import { afterAll, describe, expect, it } from 'vitest';
import { Client, DataType, Precision, Table } from '../../src/index.js';

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
const httpEndpoint = process.env.GREPTIMEDB_HTTP ?? 'http://127.0.0.1:4000';

const client = new Client(Client.create(endpoint).withDatabase('public').build());

afterAll(async () => {
  await client.close();
});

async function sql(query: string): Promise<unknown[][]> {
  const res = await fetch(`${httpEndpoint}/v1/sql?db=public`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: `sql=${encodeURIComponent(query)}`,
  });
  const json = (await res.json()) as { output: { records: { rows: unknown[][] } }[] };
  return json.output[0]?.records.rows ?? [];
}

describe('Json2 integration', () => {
  it('auto-creates a JSON2 column and round-trips payloads', async () => {
    const tableName = `ts_ingester_json2_${Date.now()}`;
    const payloads = [
      'null',
      '{"nested":{"items":[1,"two",null,{"ok":false}]},"value":42}',
      '{"nested":{"other":true},"value":"changed"}',
      '{}',
      '{"value":null}',
    ];
    // The first request is NULL-only, so auto-create must rely on the schema marker.
    for (const [i, payload] of payloads.entries()) {
      const table = Table.new(tableName)
        .addFieldColumn('payload', DataType.Json2)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow([payload, i]);
      const res = await client.write(table, i === 0 ? { hints: { append_mode: 'true' } } : {});
      expect(res.value).toBe(1);
    }

    const create = await sql(`SHOW CREATE TABLE ${tableName}`);
    expect(String(create[0]?.[1])).toContain('JSON2');

    const rows = await sql(`SELECT json_get(payload, '') FROM ${tableName} ORDER BY ts`);
    expect(
      rows.map((r) => (typeof r[0] === 'string' ? (JSON.parse(r[0]) as unknown) : r[0])),
    ).toEqual(payloads.map((p) => JSON.parse(p) as unknown));
  });
});
