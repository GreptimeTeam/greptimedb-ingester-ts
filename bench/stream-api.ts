// End-to-end streaming-insert benchmark (HandleRequests).
//
// Methodology: rows are pre-generated before the timer starts (matches the Go
// ingestion-benchmark harness). `stream.write()` is async but returns before
// the server commits; `stream.finish()` inside the timer drains the outstanding
// ack so `elapsedMs` is end-to-end.

import { Client } from '../src/index.js';
import { buildSchemaTable, generateBatch } from './log-data-provider.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

async function main(): Promise<void> {
  const args = parseArgs();
  const endpoint = args.endpoint ?? process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const totalRows = numArg(args, 'rows', 1_000_000);
  const batchSize = numArg(args, 'batch-size', 1_000);

  const client = new Client(Client.create(endpoint).withDatabase('public').build());
  const hist = createLatencyHistogram();

  try {
    // Pre-generate all batches outside the timer.
    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: unknown[][][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = generateBatch(batchSize, i * batchSize);
    }

    const stream = client.createStreamWriter();
    const start = process.hrtime.bigint();
    for (let i = 0; i < numBatches; i++) {
      const tbl = buildSchemaTable();
      for (const r of allBatches[i]!) tbl.addRow(r);
      const t0 = process.hrtime.bigint();
      await stream.write(tbl);
      hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
    }
    const res = await stream.finish();
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    printReport('stream-api', {
      rows: res.value,
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
