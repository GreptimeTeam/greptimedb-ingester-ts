// End-to-end streaming-insert benchmark (HandleRequests).

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
    const stream = client.createStreamWriter();
    const start = process.hrtime.bigint();
    for (let i = 0; i < totalRows / batchSize; i++) {
      const rows = generateBatch(batchSize, i * batchSize);
      const tbl = buildSchemaTable();
      for (const r of rows) tbl.addRow(r);
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
