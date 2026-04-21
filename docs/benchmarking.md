# Benchmarking

Numbers are measured against a local `greptime/greptimedb:v1.0.0` container on Apple M4 Max (16 cores) / 48 GB, Node.js 22.17.

## Single-client numbers

Schema: 22-column `bench_logs` with ~3072-series cardinality ([details below](#schema)).

| Bench     | Setup               | Concurrency       | Throughput      | p50  | p95   | p99   |
| --------- | ------------------- | ----------------- | --------------- | ---- | ----- | ----- |
| unary     | 1M rows, batch=1000 | 1 (request/resp)  | 26k rows/s      | 33ms | 40ms  | 207ms |
| streaming | 1M rows, batch=1000 | 1 (single stream) | 31k rows/s      | 27ms | 38ms  | 207ms |
| bulk      | 2M rows, batch=5000 | 8                 | **137k rows/s** | 48ms | 353ms | 395ms |

Batch sizes mirror the Rust SDK's published log benchmark.

### Methodology

- Rows are pre-generated before the timer starts.
- Timer is end-to-end — it includes every ack / `stream.finish()` / `bulk.finish()`.
- Unary and streaming are single-concurrency; real deployments run multiple clients for more throughput.
- Bulk uses fire-and-forget (`writeRowsAsync`); p50/p95/p99 measure submit-to-ack under the saturated pipeline.

## Apples-to-apples: CPU schema vs Go SDK

A separate `cpu-bulk-api` bench mirrors the schema and shape used in the [Greptime ingestion protocol benchmark blog](https://greptime.com/blogs/2026-03-24-ingestion-protocol-benchmark): 4 string tags + 5 Float64 fields + 1 ms timestamp, `numHosts × 5 × 10 × 20` series, round-robin distribution.

| Config                          | Go SDK (blog)            | TS SDK (this repo)      |
| ------------------------------- | ------------------------ | ----------------------- |
| 1M series, 10M rows, batch=1000 | 2.01M rows/s (p50=1.7ms) | ~700k rows/s (p50=11ms) |

Today's gap is Arrow JS single-thread encoding (`rowsToArrowTable` ~= 99% of client CPU, measured via `bench/encode-only.ts`). Encoder work is ongoing.

## Apples-to-apples: vs InfluxDB and OpenTelemetry JS SDKs

Three benches share the CPU schema above and feed identically-shaped pre-generated data (same series layout and ms-stepped timestamps; Float64 values are re-rolled per run via `Math.random()`) through their respective clients. GreptimeDB serves gRPC on `4001`, InfluxDB v2 and OTLP over HTTP on `4000`.

- `cpu-bulk-api` — `@greptime/ingester`, Arrow Flight bulk path. Writes a proper time-series table (4-tag composite PK + 5 fields + ms ts).
- `cpu-influxdb` — `@influxdata/influxdb-client` v1.35, InfluxDB line protocol.
- `cpu-otel` — `@opentelemetry/exporter-logs-otlp-proto` v0.215, OTLP log records with `X-Greptime-Pipeline-Name: greptime_identity` so attributes map to real columns (without it they collapse into a JSON blob and the comparison is meaningless).

`parallelism=8`, `numHosts=100` (100k series). Median of 3 runs; each run starts with a fresh (just-dropped) table.

| Bench          | 1M rows, batch=1000 | p50  | p95  | p99  | 1M rows, batch=5000 | p50  | p95   | p99   |
| -------------- | ------------------- | ---- | ---- | ---- | ------------------- | ---- | ----- | ----- |
| `cpu-bulk-api` | **789k rows/s**     | 9ms  | 11ms | 22ms | **758k rows/s**     | 47ms | 72ms  | 99ms  |
| `cpu-influxdb` | 494k rows/s         | 14ms | 24ms | 26ms | 520k rows/s         | 69ms | 119ms | 127ms |
| `cpu-otel`     | 679k rows/s         | 10ms | 14ms | 28ms | 638k rows/s         | 53ms | 110ms | 156ms |

Arrow Flight bulk leads OTLP by ~1.2× and InfluxDB LP by ~1.6×. The advantage is server-side: rows arrive as a ready-made Arrow columnar batch, skipping text/proto parsing and per-attribute column mapping. The OTel table still carries log-model columns (`ScopeName`, `TraceId`, …) and has no `TAG`-marked primary key, so these numbers measure ingestion throughput only, not query-path parity.

Row counts were spot-checked out-of-band with `SELECT COUNT(*)`; the bench scripts themselves don't run the verification.

### SDK config

Both comparison SDKs run near-default. We override only what's needed for `--batch-size` and `--parallelism` to actually take effect:

- **InfluxDB**: one `WriteApi` per worker with `{ batchSize: --batch-size }`. The SDK's default of 1000 would silently chunk a 5000-point batch into 5 POSTs. Each worker calls `writePoints(batch)` then `flush(true)` — `true` drains the retry buffer so a transient 429/5xx is only counted as written once its retry succeeds. `maxRetries`, `flushInterval`, etc. stay at defaults.
- **OTLP**: single `OTLPLogExporter` with `concurrencyLimit: max(30, --parallelism)`. The default 30 would reject `--parallelism > 30` outright. We call `exporter.export(records, cb)` directly instead of going through `LoggerProvider` + `BatchLogRecordProcessor`, which serialises exports internally and would cap in-flight requests at 1.

## Running the benches

```bash
./scripts/run-greptimedb.sh   # or point at your own deployment

# gRPC (default endpoint localhost:4001)
pnpm bench bulk-api --rows=2000000 --batch-size=5000
pnpm bench cpu-bulk-api --rows=1000000 --batch-size=1000 --parallelism=8

# HTTP (default endpoint http://localhost:4000)
pnpm bench cpu-influxdb --rows=1000000 --batch-size=1000 --parallelism=8
pnpm bench cpu-otel     --rows=1000000 --batch-size=1000 --parallelism=8
```

Available benches: `regular-api`, `stream-api`, `bulk-api`, `cpu-bulk-api`, `cpu-influxdb`, `cpu-otel`. Drop the target table (`benchmark_grpc_bulk`, `benchmark_influxdb`, `benchmark_otel`, or `bench_logs`) between runs for fresh-insert measurements. CLI flags take precedence over env vars: `GREPTIMEDB_ENDPOINT`, `GREPTIMEDB_HTTP_ENDPOINT`, `GREPTIMEDB_DATABASE`, `GREPTIMEDB_USER`, `GREPTIMEDB_PASSWORD`.

Shared flags:

- `--rows=N` — rounded down to a multiple of `--batch-size`
- `--batch-size=N`
- `--parallelism=N` — bulk / cpu-\* only (default 8)
- `--num-hosts=N` — cpu-\* only; cardinality = `N × 5 × 10 × 20` (default 100 → 100k series; 1000 matches the blog's 1M-series config)
- `--endpoint=host:port` — gRPC benches
- `--http-endpoint=URL`, `--database=NAME`, `--user=NAME`, `--password=VALUE` — HTTP benches

## Tuning tips

- Raise `parallelism` to 12–16 if your producer is CPU-light and request latency dominates (e.g. remote server).
- Enable `BulkCompression.Lz4` or `BulkCompression.Zstd` for bandwidth-constrained links.
- Prefer bulk over unary for sustained ingest — Arrow columnar amortizes encoding cost across the batch.
- One `Client` per process; channels pool internally.

## Schema

22-column `bench_logs` table, source [`bench/log-data-provider.ts`](../bench/log-data-provider.ts).

| Column              | Kind      | Type                    | Notes                                                  |
| ------------------- | --------- | ----------------------- | ------------------------------------------------------ |
| `service`           | tag       | String                  | 6 values                                               |
| `host`              | tag       | String                  | 32 values                                              |
| `region`            | tag       | String                  | 4 values                                               |
| `env`               | tag       | String                  | 4 values                                               |
| `pod`               | field     | String                  | 64 values                                              |
| `log_level`         | field     | String                  | 4 values                                               |
| `message`           | field     | String                  | synthesized per row                                    |
| `trace_id`          | field     | String                  | 32-hex, high cardinality                               |
| `span_id`           | field     | String                  | 16-hex, high cardinality                               |
| `http_method`       | field     | String                  | 5 values                                               |
| `http_path`         | field     | String                  | 5 values                                               |
| `http_status_class` | field     | String                  | 4 values                                               |
| `user_agent`        | field     | String                  | 4 values                                               |
| `client_ip`         | field     | String                  | synthesized per row                                    |
| `caller`            | field     | String                  | synthesized per row                                    |
| `latency_ms`        | field     | Float64                 |                                                        |
| `bytes_in`          | field     | Int64                   |                                                        |
| `bytes_out`         | field     | Int64                   |                                                        |
| `error_flag`        | field     | Bool                    |                                                        |
| `retry_flag`        | field     | Bool                    |                                                        |
| `log_ts`            | timestamp | Timestamp (millisecond) | time index                                             |
| `ingest_ts`         | field     | Int64                   | client-side ingest time (kept as field for comparison) |

The 4 tag columns yield up to `6 × 32 × 4 × 4 = 3072` distinct series. High-cardinality identifiers (`trace_id`, `span_id`) are fields, not tags, so they don't inflate the series count.
