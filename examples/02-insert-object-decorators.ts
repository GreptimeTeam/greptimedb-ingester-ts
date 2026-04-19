/*
 * Example 02 — ORM-style insert via Stage-3 decorators.
 *
 * Purpose: declare the schema once on the class, then write instances with
 *   `client.writeObject(items)`. Decorators are native TS 5 Stage-3 — no
 *   experimentalDecorators, no reflect-metadata.
 *
 * Required env: GreptimeDB at localhost:4001 (override with GREPTIMEDB_ENDPOINT).
 *
 * Run: pnpm example 02-insert-object-decorators
 */

import { Client, DataType, Precision, field, tableName, tag, timestamp } from '../src/index.js';

@tableName('cpu_usage_orm_demo')
class CpuMetric {
  @tag(DataType.String) host!: string;
  @field(DataType.Float64) usage!: number;
  @timestamp({ precision: Precision.Millisecond }) ts!: number;
}

async function main(): Promise<void> {
  const endpoint = process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const client = new Client(Client.create(endpoint).withDatabase('public').build());

  const batch: CpuMetric[] = [
    Object.assign(new CpuMetric(), { host: 'server-01', usage: 35.2, ts: Date.now() }),
    Object.assign(new CpuMetric(), { host: 'server-02', usage: 71.5, ts: Date.now() }),
    Object.assign(new CpuMetric(), { host: 'server-03', usage: 12.1, ts: Date.now() }),
  ];

  try {
    const result = await client.writeObject(batch);
    console.log(`inserted ${result.value} rows into cpu_usage_orm_demo`);
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('insert failed:', err);
  process.exit(1);
});
