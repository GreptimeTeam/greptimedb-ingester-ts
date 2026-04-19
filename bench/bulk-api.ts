// End-to-end bulk-insert benchmark (Arrow Flight DoPut).

import { Client } from '../src/index.js';
import { buildSchemaTable, generateBatch } from './log-data-provider.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

async function main(): Promise<void> {
  const args = parseArgs();
  const endpoint = args.endpoint ?? process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const totalRows = numArg(args, 'rows', 2_000_000);
  const batchSize = numArg(args, 'batch-size', 5_000);
  const parallelism = numArg(args, 'parallelism', 8);

  const client = new Client(Client.create(endpoint).withDatabase('public').build());
  const hist = createLatencyHistogram();

  try {
    // Ensure the table exists via unary insert first (bulk does not auto-create).
    const probe = buildSchemaTable();
    for (const r of generateBatch(1)) probe.addRow(r);
    await client.write(probe);

    const schema = buildSchemaTable().schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism });

    const start = process.hrtime.bigint();
    const batches = totalRows / batchSize;
    for (let i = 0; i < batches; i++) {
      const rows = generateBatch(batchSize, i * batchSize);
      const t0 = process.hrtime.bigint();
      await bulk.writeRows({ kind: 'rows', rows });
      hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
    }
    const summary = await bulk.finish();
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    printReport('bulk-api', {
      rows: summary.totalAffectedRows,
      elapsedMs,
      p50Ms: hist.getValueAtPercentile(50),
      p95Ms: hist.getValueAtPercentile(95),
      p99Ms: hist.getValueAtPercentile(99),
    });
  } finally {
    await client.close();
  }
}

main().catch((err: unknown) => {
  console.error('bench failed:', err);
  process.exit(1);
});
