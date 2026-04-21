// Apples-to-apples comparison bench against the Go ingestion-benchmark's CPU
// schema (10 columns: 4 string tags + 5 Float64 fields + 1 ms timestamp).
//
// Mirrors `/Users/dennis/programming/go/ingestion-benchmark/benchmark/data.go`:
//   - tags: host, region, datacenter, service
//   - fields: cpu, memory, disk_util, net_in, net_out (all Float64)
//   - timestamp: ts (ms precision)
//   - cardinality: numHosts × 5 regions × 10 datacenters × 20 services
//   - round-robin distribution across series, ms-stepped timestamps
//
// Used to isolate "our SDK is slow" from "our default bench schema has 22 cols
// full of high-entropy strings". Run this alongside the regular `bulk-api` bench.

import { Client, DataType, Precision, Table, type TableSchema } from '../src/index.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

const TABLE_NAME = 'bench_cpu';

const REGIONS = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1', 'ap-northeast-1'];
const DATACENTERS = Array.from({ length: 10 }, (_, i) => `dc-${i}`);
const SERVICES = Array.from({ length: 20 }, (_, i) => `svc-${String(i).padStart(2, '0')}`);

function buildSchemaTable(): Table {
  return Table.new(TABLE_NAME)
    .addTagColumn('host', DataType.String)
    .addTagColumn('region', DataType.String)
    .addTagColumn('datacenter', DataType.String)
    .addTagColumn('service', DataType.String)
    .addFieldColumn('cpu', DataType.Float64)
    .addFieldColumn('memory', DataType.Float64)
    .addFieldColumn('disk_util', DataType.Float64)
    .addFieldColumn('net_in', DataType.Float64)
    .addFieldColumn('net_out', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
}

const BASE_TIME_MS = Date.UTC(2024, 0, 1, 0, 0, 0, 0);

function generateRows(totalRows: number, numHosts: number, offset: number): unknown[][] {
  const seriesCount = numHosts * REGIONS.length * DATACENTERS.length * SERVICES.length;
  const dcServices = DATACENTERS.length * SERVICES.length;
  const regionBlock = REGIONS.length * dcServices;
  const rows: unknown[][] = new Array(totalRows);
  for (let k = 0; k < totalRows; k++) {
    const i = offset + k;
    const seriesIdx = i % seriesCount;
    const hostIdx = Math.floor(seriesIdx / regionBlock);
    let rem = seriesIdx % regionBlock;
    const regionIdx = Math.floor(rem / dcServices);
    rem = rem % dcServices;
    const dcIdx = Math.floor(rem / SERVICES.length);
    const svcIdx = rem % SERVICES.length;
    rows[k] = [
      `host-${hostIdx}`,
      REGIONS[regionIdx],
      DATACENTERS[dcIdx],
      SERVICES[svcIdx],
      Math.random() * 100,
      Math.random() * 100,
      Math.random() * 100,
      Math.random() * 1e9,
      Math.random() * 1e9,
      BASE_TIME_MS + i,
    ];
  }
  return rows;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const endpoint = args.endpoint ?? process.env.GREPTIMEDB_ENDPOINT ?? 'localhost:4001';
  const totalRows = numArg(args, 'rows', 1_000_000);
  const batchSize = numArg(args, 'batch-size', 100);
  const parallelism = numArg(args, 'parallelism', 8);
  const numHosts = numArg(args, 'num-hosts', 100);

  const client = new Client(Client.create(endpoint).withDatabase('public').build());
  const hist = createLatencyHistogram();

  try {
    // Auto-create the table via a probe unary write (bulk doesn't auto-create).
    const probe = buildSchemaTable();
    for (const r of generateRows(1, numHosts, 0)) probe.addRow(r);
    await client.write(probe);

    const schema: TableSchema = buildSchemaTable().schema();
    const bulk = await client.createBulkStreamWriter(schema, { parallelism });

    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: unknown[][][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = generateRows(batchSize, numHosts, i * batchSize);
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
