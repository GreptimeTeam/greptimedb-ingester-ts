import { afterAll, describe, expect, it } from 'vitest';
import { Client, StateError, Table, DataType, Precision } from '../../src/index.js';

// Client.close() is documented as terminal. These tests lock down that contract:
// every public entry point must reject with StateError after close, close is
// idempotent, and an already-closed client never transparently rebuilds channels.

const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';

function newClient(): Client {
  return new Client(Client.create(endpoint).withDatabase('public').build());
}

function sampleTable(name: string): Table {
  return Table.new(name)
    .addTagColumn('host', DataType.String)
    .addFieldColumn('value', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond)
    .addRow(['h1', 1.0, Date.now()]);
}

describe('Client.close() is terminal', () => {
  afterAll(() => {
    // No shared client.
  });

  it('rejects write() after close with StateError', async () => {
    const c = newClient();
    await c.close();
    await expect(c.write(sampleTable('close_write'))).rejects.toBeInstanceOf(StateError);
  });

  it('rejects writeObject() after close with StateError', async () => {
    const c = newClient();
    await c.close();
    await expect(c.writeObject([{}])).rejects.toBeInstanceOf(StateError);
  });

  it('createStreamWriter() after close throws StateError synchronously', async () => {
    const c = newClient();
    await c.close();
    expect(() => c.createStreamWriter()).toThrow(StateError);
  });

  it('createBulkStreamWriter() after close rejects with StateError', async () => {
    const c = newClient();
    await c.close();
    await expect(
      c.createBulkStreamWriter(sampleTable('close_bulk').schema()),
    ).rejects.toBeInstanceOf(StateError);
  });

  it('close() is idempotent — a second close is a no-op, not an error', async () => {
    const c = newClient();
    await c.close();
    await expect(c.close()).resolves.toBeUndefined();
    expect(c.isClosed()).toBe(true);
  });
});
