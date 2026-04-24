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
import {
  BulkCompression,
  BulkError,
  Client,
  ConfigBuilder,
  DataType,
  type LogLevel,
  type Logger,
  Precision,
  Table,
  type TableSchema,
} from '../../src/index.js';

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
const httpEndpoint = process.env.GREPTIMEDB_HTTP ?? 'http://127.0.0.1:4000';
const client = new Client(Client.create(endpoint).withDatabase('public').build());

async function queryCount(table: string): Promise<number> {
  const res = await fetch(`${httpEndpoint}/v1/sql?db=public`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: `sql=${encodeURIComponent(`SELECT COUNT(*) FROM ${table}`)}`,
  });
  const json = (await res.json()) as { output: { records: { rows: number[][] } }[] };
  return json.output[0]?.records.rows[0]?.[0] ?? 0;
}

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

  // Regression: cancel() must not eagerly clear groups/completed before the
  // tracker rejections propagate. Previously this produced a microtask race
  // where a synchronous waitForResponse(id) call between cancel() returning
  // and the reject microtask firing would falsely report "no pending response"
  // instead of the actual cancellation error.
  it('round-trips 1000 rows with LZ4_FRAME body compression', async () => {
    const name = `bulk_lz4_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, {
      compression: BulkCompression.Lz4,
      parallelism: 2,
    });

    const rows: unknown[][] = [];
    for (let i = 0; i < 1_000; i++) rows.push([`h-${i % 8}`, Math.random(), Date.now() + i]);
    const resp = await bulk.writeRows({ kind: 'rows', rows });
    expect(resp.affectedRows).toBe(1_000);
    const summary = await bulk.finish();
    expect(summary.totalAffectedRows).toBe(1_000);
  });

  // Regression S4: finish() must await in-flight writeRowsAsync groups. Fire-and-forget
  // users who never claim acks used to race call.end() against their last write, with
  // the stream half-closing mid-frame. After the fix, all rows are guaranteed to land.
  it('finish() awaits in-flight writeRowsAsync without await-per-call', async () => {
    const name = `bulk_finish_awaits_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 4 });

    const batches = 20;
    const rowsPerBatch = 500;
    for (let b = 0; b < batches; b++) {
      const rows: unknown[][] = [];
      for (let i = 0; i < rowsPerBatch; i++) {
        rows.push([`h-${i % 8}`, Math.random(), Date.now() + b * rowsPerBatch + i]);
      }
      // Intentionally no await — fire and forget.
      void bulk.writeRowsAsync({ kind: 'rows', rows });
    }

    const summary = await bulk.finish();
    expect(summary.totalAffectedRows).toBe(batches * rowsPerBatch);
    // Real HTTP verification: the server got every row, not just ack counts.
    expect(await queryCount(name)).toBe(batches * rowsPerBatch + 1); // +1 from probe
  });

  // Regression S2: the completed map caps so a fire-and-forget producer cannot grow
  // it unboundedly. Hitting the cap logs one warn and keeps the stream usable.
  it('caps the completed-response cache and warns once', async () => {
    const name = `bulk_completed_cap_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    // Per-test logger so we don't rely on console mocks or global state.
    const warnings: { msg: string }[] = [];
    const logger: Logger = {
      log(level: LogLevel, msg: string): void {
        if (level === 'warn') warnings.push({ msg });
      },
    };
    const scopedClient = new Client(
      ConfigBuilder.create(endpoint).withDatabase('public').withLogger(logger).build(),
    );
    try {
      const schema: TableSchema = buildTable(name).schema();
      const bulk = await scopedClient.createBulkStreamWriter(schema, {
        parallelism: 4,
        maxUnclaimedResponses: 3,
      });
      // 8 fire-and-forget batches with a cap of 3 forces at least 5 evictions.
      for (let b = 0; b < 8; b++) {
        void bulk.writeRowsAsync({
          kind: 'rows',
          rows: [[`h-${b}`, b, Date.now() + b]],
        });
      }
      await bulk.finish();
      // Exactly one warning — repeated evictions must not spam the log.
      const capWarnings = warnings.filter((w) => w.msg.includes('completed-response cache full'));
      expect(capWarnings.length).toBe(1);
    } finally {
      await scopedClient.close();
    }
  });

  // Regression P1: fire-and-forget writeRowsAsync rejections must not disappear
  // into a clean finish(). The caller `void`-ed the promise, so finish() is the
  // only channel through which they can learn one (or more) batches failed.
  it('finish() surfaces fire-and-forget rejections as BulkError', async () => {
    const name = `bulk_faf_reject_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 2 });

    // Bad batch: string where a Float64 cell is expected. Encode rejects synchronously
    // inside _writeRowsAsync, so the top-level promise is rejected and we verify it
    // surfaces via finish(). One good batch sent alongside to confirm partial success
    // doesn't mask the failure.
    void bulk.writeRowsAsync({ kind: 'rows', rows: [['h', 'not-a-number', Date.now()]] });
    void bulk.writeRowsAsync({ kind: 'rows', rows: [['h', 1.5, Date.now()]] });

    await expect(bulk.finish()).rejects.toBeInstanceOf(BulkError);
  });

  it('rejects invalid maxUnclaimedResponses at construction', async () => {
    const name = `bulk_cap_validation_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));
    const schema: TableSchema = buildTable(name).schema();
    // 0 / negative / NaN must all fail fast with BulkError before any gRPC work.
    for (const bad of [0, -1, Number.NaN, 1.5]) {
      await expect(
        client.createBulkStreamWriter(schema, { maxUnclaimedResponses: bad }),
      ).rejects.toBeInstanceOf(BulkError);
    }
  });

  it('waitForResponse after synchronous cancel reports the cancellation error', async () => {
    const name = `bulk_cancel_race_${Date.now()}`;
    await client.write(buildTable(name).addRow(['probe', 0, Date.now()]));

    const schema: TableSchema = buildTable(name).schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism: 1 });

    const id = await bulk.writeRowsAsync({ kind: 'rows', rows: [['x', 1, Date.now()]] });
    // Synchronous cancel + waitForResponse — no await between them.
    bulk.cancel();
    const waitP = bulk.waitForResponse(id);
    await expect(waitP).rejects.toBeInstanceOf(BulkError);
    await expect(waitP).rejects.not.toMatchObject({
      message: expect.stringContaining('no pending response'),
    });
  });
});
