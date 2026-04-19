/*
 * Integration: end-to-end verification that `hints` reach the server in the wire
 * format the server actually parses. We rely on `auto_create_table=false` because
 * its effect is binary and unambiguous: if the hint reaches the server, writing to
 * a non-existent table fails with a server error; if the hint is silently dropped,
 * GreptimeDB falls back to its default of auto-creating the table and the write
 * succeeds. Either outcome distinguishes a correct from a broken implementation.
 *
 * Run: INTEGRATION=1 pnpm test:integration
 */

import { afterAll, describe, expect, it } from 'vitest';
import {
  Client,
  DataType,
  IngesterError,
  Precision,
  Table,
  TransportError,
  ValueError,
} from '../../src/index.js';

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
const httpEndpoint = process.env.GREPTIMEDB_HTTP ?? 'http://127.0.0.1:4000';

const client = new Client(Client.create(endpoint).withDatabase('public').build());

afterAll(async () => {
  await client.close();
});

function buildSampleTable(name: string): Table {
  return Table.new(name)
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond)
    .addRow(['probe', 0, Date.now()]);
}

async function tableExists(table: string): Promise<boolean> {
  const res = await fetch(`${httpEndpoint}/v1/sql?db=public`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: `sql=${encodeURIComponent(`SHOW TABLES LIKE '${table}'`)}`,
  });
  const json = (await res.json()) as { output: { records: { rows: unknown[][] } }[] };
  const rows = json.output[0]?.records.rows ?? [];
  return rows.length > 0;
}

describe('hints integration (server wire-format verification)', () => {
  it('with auto_create_table=false the server rejects writes to a non-existent table', async () => {
    const tableName = `hints_no_autocreate_${Date.now()}`;
    // Sanity: the table truly does not exist beforehand.
    expect(await tableExists(tableName)).toBe(false);

    let caught: unknown;
    try {
      await client.write(buildSampleTable(tableName), {
        hints: { auto_create_table: 'false' },
      });
    } catch (err) {
      caught = err;
    }
    // The server may surface this either as a proto-level ServerError (header.status
    // non-zero) or as a gRPC INVALID_ARGUMENT TransportError; both prove the hint
    // reached the server. Accept any IngesterError, then assert it's not a silent
    // success.
    expect(caught).toBeDefined();
    expect(caught).toBeInstanceOf(IngesterError);
    if (caught instanceof TransportError) {
      // 3 == INVALID_ARGUMENT — what GreptimeDB raises for "table not found, no
      // autocreate".
      expect(caught.grpcCode).toBe(3);
    }

    // And it must NOT have been auto-created behind our backs — proving the hint
    // actually reached the server.
    expect(await tableExists(tableName)).toBe(false);
  });

  it('without the hint, the same write auto-creates the table (control case)', async () => {
    const tableName = `hints_autocreate_default_${Date.now()}`;
    expect(await tableExists(tableName)).toBe(false);
    const res = await client.write(buildSampleTable(tableName));
    expect(res.value).toBe(1);
    expect(await tableExists(tableName)).toBe(true);
  });

  it('rejects hint key/value containing reserved , or = before any RPC', async () => {
    await expect(
      client.write(buildSampleTable(`hints_invalid_${Date.now()}`), {
        hints: { 'a,b': 'v' },
      }),
    ).rejects.toBeInstanceOf(ValueError);
    await expect(
      client.write(buildSampleTable(`hints_invalid_${Date.now()}`), {
        hints: { k: 'a=b' },
      }),
    ).rejects.toBeInstanceOf(ValueError);
  });
});
