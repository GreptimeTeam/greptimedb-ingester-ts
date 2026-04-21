// InfluxDB Line Protocol comparison bench against GreptimeDB's /v1/influxdb
// endpoint. Mirrors the Go ingestion-benchmark `writer_influxdb.go`:
//   - client: @influxdata/influxdb-client (the v2 JS SDK), DEFAULT config
//   - token:  "user:password" (GreptimeDB convention)
//   - schema: same CPU model as cpu-bulk-api / cpu-otel
//
// Per-instance config is SDK default (batchSize=1000, flushInterval=60000,
// maxRetries=3). We drive per-batch `writePoints` + `flush` as a public API
// call — not a config override — to get per-batch latency and bounded memory.
//
// Concurrency: N worker tasks, each owning its own `WriteApi`. The JS SDK
// has no blocking-write equivalent of Go's `WriteAPIBlocking`, and a shared
// `WriteApi` races on its internal buffer under concurrent flushes. Creating
// N instances is the minimal idiomatic adaptation to match Go's per-worker
// blocking-write model.

import { InfluxDB, Point, type WriteApi } from '@influxdata/influxdb-client';
import { type CpuRow, generateCpuRows } from './cpu-data-provider.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

const TABLE_NAME = 'benchmark_influxdb';

function buildPoints(rows: readonly CpuRow[]): Point[] {
  const pts: Point[] = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i]!;
    pts[i] = new Point(TABLE_NAME)
      .tag('host', r.host)
      .tag('region', r.region)
      .tag('datacenter', r.datacenter)
      .tag('service', r.service)
      .floatField('cpu', r.cpu)
      .floatField('memory', r.memory)
      .floatField('disk_util', r.diskUtil)
      .floatField('net_in', r.netIn)
      .floatField('net_out', r.netOut)
      .timestamp(new Date(r.tsMs));
  }
  return pts;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const httpEndpoint =
    args['http-endpoint'] ?? process.env.GREPTIMEDB_HTTP_ENDPOINT ?? 'http://localhost:4000';
  const database = args.database ?? process.env.GREPTIMEDB_DATABASE ?? 'public';
  const user = args.user ?? process.env.GREPTIMEDB_USER ?? '';
  const password = args.password ?? process.env.GREPTIMEDB_PASSWORD ?? '';
  const totalRows = numArg(args, 'rows', 1_000_000);
  const batchSize = numArg(args, 'batch-size', 1_000);
  const parallelism = numArg(args, 'parallelism', 8);
  const numHosts = numArg(args, 'num-hosts', 100);

  const url = `${httpEndpoint.replace(/\/$/, '')}/v1/influxdb`;
  const token = user !== '' || password !== '' ? `${user}:${password}` : '';
  const client = new InfluxDB({ url, token });
  const hist = createLatencyHistogram();

  // One WriteApi per worker to avoid shared-buffer races. No config overrides
  // — the SDK defaults (batchSize=1000, flushInterval=60000ms, maxRetries=3)
  // apply. With per-batch sizes ≤ batchSize, `writePoints` just buffers and
  // the explicit `flush()` drives the single HTTP POST.
  const writeApis: WriteApi[] = Array.from({ length: parallelism }, () =>
    client.getWriteApi('', database, 'ms'),
  );

  try {
    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: Point[][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = buildPoints(generateCpuRows(batchSize, numHosts, i * batchSize));
    }

    let nextIdx = 0;
    let writtenRows = 0;
    const start = process.hrtime.bigint();

    await Promise.all(
      writeApis.map(async (api) => {
        // nextIdx++ is safe: JS is single-threaded, each worker's check-and-
        // increment runs atomically before the next `await` yields.
        while (nextIdx < numBatches) {
          const i = nextIdx++;
          const pts = allBatches[i]!;
          const t0 = process.hrtime.bigint();
          api.writePoints(pts);
          await api.flush();
          hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
          writtenRows += pts.length;
        }
      }),
    );

    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    printReport('cpu-influxdb', {
      rows: writtenRows,
      elapsedMs,
      p50Ms: hist.getValueAtPercentile(50),
      p95Ms: hist.getValueAtPercentile(95),
      p99Ms: hist.getValueAtPercentile(99),
    });
  } finally {
    await Promise.all(writeApis.map((api) => api.close().catch(() => undefined)));
  }
}

main().catch((err: unknown) => {
  console.error('bench failed:', err);
  process.exit(1);
});
