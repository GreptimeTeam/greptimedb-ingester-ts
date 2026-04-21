// End-to-end bulk-insert benchmark (Arrow Flight DoPut).
//
// Methodology:
//   1. All row batches are pre-generated BEFORE the timer starts — matches the Go
//      ingestion-benchmark harness (GenerateData is called before time.Now()).
//   2. Submission uses `writeRowsAsync()` (fire-and-forget) so the SDK's internal
//      semaphore (default parallelism=8) actually pipelines batches. Using the
//      await-per-batch `writeRows()` forces serialisation at concurrency=1.
//   3. The timer spans submission + drain (`bulk.finish()` awaits all acks). This
//      is end-to-end: client encode + push + server ingest + ack round-trip.
//   4. Per-batch latency = time from "about to submit batch" to "ack received".

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
    // Bootstrap: auto-create the table via one unary write (bulk does not auto-create).
    const probe = buildSchemaTable();
    for (const r of generateBatch(1)) probe.addRow(r);
    await client.write(probe);

    const schema = buildSchemaTable().schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism });

    // Pre-generate all batches outside the timer.
    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: unknown[][][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = generateBatch(batchSize, i * batchSize);
    }

    const start = process.hrtime.bigint();
    const ackPromises: Promise<void>[] = new Array(numBatches);

    for (let i = 0; i < numBatches; i++) {
      const t0 = process.hrtime.bigint();
      // `writeRowsAsync` returns after semaphore acquire + frame writes; ack is
      // collected separately. This lets `parallelism` in-flight batches stack up.
      const id = await bulk.writeRowsAsync({ kind: 'rows', rows: allBatches[i]! });
      // Attach latency recorder at submission time so `hist.recordValue` fires
      // the instant the ack arrives, not whenever we get around to awaiting it.
      ackPromises[i] = bulk.waitForResponse(id).then(() => {
        hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
      });
    }

    // Wait for every ack's latency hook to run so the histogram is complete
    // before we stop the timer.
    await Promise.all(ackPromises);
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
