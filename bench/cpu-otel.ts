// OTLP Logs (HTTP+Protobuf) comparison bench against GreptimeDB's
// /v1/otlp/v1/logs endpoint. Mirrors the Go ingestion-benchmark
// `writer_otel.go`:
//   - exporter: @opentelemetry/exporter-logs-otlp-proto, DEFAULT config
//   - headers:  X-Greptime-DB-Name, X-Greptime-Log-Table-Name, optional Basic auth
//   - records:  one log per CPU datapoint, tags/fields carried as attributes,
//               body = "benchmark" (string literal matches Go version)
//
// We use the low-level exporter directly (not LoggerProvider + processor).
// Rationale: this is what the Go reference does (`exporter.Export(ctx, recs)`),
// and the JS `BatchLogRecordProcessor` serialises exports internally so it
// cannot drive N concurrent HTTP POSTs like the Go benchmark does. The
// exporter API is public; `ReadableLogRecord` is exported from sdk-logs.
//
// Note: this exercises GreptimeDB's OTLP log ingestion pathway, which is
// distinct from the metric/row store path used by the gRPC and InfluxDB
// benches. Results reflect protocol+pathway, not a schema-identical comparison.

import { SeverityNumber } from '@opentelemetry/api-logs';
import type { HrTime } from '@opentelemetry/api';
import { ExportResultCode, type InstrumentationScope } from '@opentelemetry/core';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-proto';
import { type Resource, emptyResource } from '@opentelemetry/resources';
import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import { type CpuRow, generateCpuRows } from './cpu-data-provider.js';
import { createLatencyHistogram, numArg, parseArgs, printReport } from './report.js';

const TABLE_NAME = 'benchmark_otel';

function msToHrTime(ms: number): HrTime {
  const sec = Math.floor(ms / 1000);
  const nanos = (ms - sec * 1000) * 1_000_000;
  return [sec, nanos];
}

function buildRecords(
  rows: readonly CpuRow[],
  resource: Resource,
  scope: InstrumentationScope,
): ReadableLogRecord[] {
  const records: ReadableLogRecord[] = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i]!;
    const hr = msToHrTime(r.tsMs);
    records[i] = {
      hrTime: hr,
      hrTimeObserved: hr,
      severityNumber: SeverityNumber.INFO,
      severityText: 'INFO',
      body: 'benchmark',
      resource,
      instrumentationScope: scope,
      attributes: {
        host: r.host,
        region: r.region,
        datacenter: r.datacenter,
        service: r.service,
        cpu: r.cpu,
        memory: r.memory,
        disk_util: r.diskUtil,
        net_in: r.netIn,
        net_out: r.netOut,
      },
      droppedAttributesCount: 0,
    };
  }
  return records;
}

function exportBatch(exporter: OTLPLogExporter, records: ReadableLogRecord[]): Promise<void> {
  return new Promise((resolve, reject) => {
    exporter.export(records, (result) => {
      if (result.code === ExportResultCode.SUCCESS) resolve();
      else reject(result.error ?? new Error('OTLP export failed'));
    });
  });
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

  const headers: Record<string, string> = {
    'X-Greptime-DB-Name': database,
    'X-Greptime-Log-Table-Name': TABLE_NAME,
    // Use GreptimeDB's built-in identity pipeline so that OTLP log attributes
    // are promoted into real columns instead of being collapsed into a single
    // `log_attributes` JSON blob. Without this, the resulting table is a
    // generic log table (PK = scope_name + ts, attrs = JSON) and the comparison
    // against cpu-bulk-api / cpu-influxdb is not apples-to-apples.
    'X-Greptime-Pipeline-Name': 'greptime_identity',
  };
  if (user !== '' || password !== '') {
    headers.Authorization = `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`;
  }

  const exporter = new OTLPLogExporter({
    url: `${httpEndpoint.replace(/\/$/, '')}/v1/otlp/v1/logs`,
    headers,
  });

  const resource = emptyResource();
  const scope: InstrumentationScope = { name: 'greptimedb-ingester-ts-bench', version: '0.0.0' };
  const hist = createLatencyHistogram();

  try {
    const numBatches = Math.floor(totalRows / batchSize);
    const allBatches: ReadableLogRecord[][] = new Array(numBatches);
    for (let i = 0; i < numBatches; i++) {
      allBatches[i] = buildRecords(
        generateCpuRows(batchSize, numHosts, i * batchSize),
        resource,
        scope,
      );
    }

    let nextIdx = 0;
    let writtenRows = 0;
    const start = process.hrtime.bigint();

    await Promise.all(
      Array.from({ length: parallelism }, async () => {
        // nextIdx++ is safe: JS is single-threaded, each worker's check-and-
        // increment runs atomically before the next `await` yields.
        while (nextIdx < numBatches) {
          const i = nextIdx++;
          const recs = allBatches[i]!;
          const t0 = process.hrtime.bigint();
          await exportBatch(exporter, recs);
          hist.recordValue(Number(process.hrtime.bigint() - t0) / 1e6);
          writtenRows += recs.length;
        }
      }),
    );

    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    printReport('cpu-otel', {
      rows: writtenRows,
      elapsedMs,
      p50Ms: hist.getValueAtPercentile(50),
      p95Ms: hist.getValueAtPercentile(95),
      p99Ms: hist.getValueAtPercentile(99),
    });
  } finally {
    await exporter.shutdown();
  }
}

main().catch((err: unknown) => {
  console.error('bench failed:', err);
  process.exit(1);
});
