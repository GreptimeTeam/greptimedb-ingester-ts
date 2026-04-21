// Apples-to-apples comparison bench against the Go ingestion-benchmark's CPU
// schema (10 columns: 4 string tags + 5 Float64 fields + 1 ms timestamp).
//
// Mirrors https://github.com/killme2008/greptimedb-ingestion-benchmark
// (see `benchmark/data.go`). Used alongside `cpu-influxdb` and `cpu-otel` to
// isolate "our SDK is slow" from "our default bench schema has 22 cols full
// of high-entropy strings".

import { Client } from '../src/index.js';
import {
  buildCpuSchemaTable,
  cpuRowsAsArrays,
  cpuSchema,
  generateCpuRows,
} from './cpu-data-provider.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

const TABLE_NAME = 'benchmark_grpc_bulk';

async function main(): Promise<void> {
  const args = parseArgs();
  const endpoint = args.endpoint ?? process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const totalRows = numArg(args, 'rows', 1_000_000);
  const batchSize = numArg(args, 'batch-size', 1_000);
  const parallelism = numArg(args, 'parallelism', 8);
  const numHosts = numArg(args, 'num-hosts', 100);

  const client = new Client(Client.create(endpoint).withDatabase('public').build());
  const hist = createLatencyHistogram();

  try {
    // Auto-create the table via a probe unary write (bulk doesn't auto-create).
    const probe = buildCpuSchemaTable(TABLE_NAME);
    for (const r of cpuRowsAsArrays(generateCpuRows(1, numHosts, 0))) probe.addRow(r);
    await client.write(probe);

    const bulk = await client.createBulkStreamWriter(cpuSchema(TABLE_NAME), { parallelism });

    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: unknown[][][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = cpuRowsAsArrays(generateCpuRows(batchSize, numHosts, i * batchSize));
    }

    const start = process.hrtime.bigint();
    const ackPromises: Promise<void>[] = new Array(numBatches);

    for (let i = 0; i < numBatches; i++) {
      const t0 = process.hrtime.bigint();
      const id = await bulk.writeRowsAsync({ kind: 'rows', rows: allBatches[i]! });
      ackPromises[i] = bulk.waitForResponse(id).then(() => {
        hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
      });
    }

    await Promise.all(ackPromises);
    const summary = await bulk.finish();
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

    printReport('cpu-bulk-api', {
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
