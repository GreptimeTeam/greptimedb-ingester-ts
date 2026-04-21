# Benchmarking

Reference numbers measured against a local `greptime/greptimedb:v1.0.0` container on an Apple M-series laptop. The schema is 22 columns (mix of tags, fields, timestamp); see [Schema](#schema) below.

| Bench     | Setup               | Concurrency       | Throughput      | p50  | p95   | p99   |
| --------- | ------------------- | ----------------- | --------------- | ---- | ----- | ----- |
| unary     | 1M rows, batch=1000 | 1 (request/resp)  | 26k rows/s      | 33ms | 40ms  | 207ms |
| streaming | 1M rows, batch=1000 | 1 (single stream) | 31k rows/s      | 27ms | 38ms  | 207ms |
| bulk      | 2M rows, batch=5000 | 8 (default)       | **137k rows/s** | 48ms | 353ms | 395ms |

All three share the same schema and ~3072-series cardinality. Batch sizes mirror the Rust SDK's published log benchmark.

### Methodology

- **Rows pre-generated** before the timer starts (matches the Rust SDK benchmark layout).
- **Timer is end-to-end**: for unary/streaming it includes each ack / `stream.finish()`; for bulk it includes every batch's ack drained by `bulk.finish()`. Numbers reflect "time to commit N rows to the server", not "time to push N rows onto a buffer".
- **Unary and streaming are single-concurrency**: a single `Client` writing serially on one connection. Real applications needing more throughput run multiple clients.
- **Bulk uses fire-and-forget**: `writeRowsAsync()` submits without blocking on per-batch acks, so the SDK's `parallelism: 8` semaphore keeps up to 8 batches in flight. p50/p95/p99 measure submit-to-ack under this saturated pipeline — the tails reflect semaphore queue time at the cap, not individual RPC latency.

## Schema

The 22-column `bench_logs` table mixes three tag tiers plus numeric / boolean / timestamp fields (source: [`bench/log-data-provider.ts`](../bench/log-data-provider.ts)).

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

**Series cardinality**: the 4 tag columns yield up to `service(6) × host(32) × region(4) × env(4) = 3072` distinct time series. At 1–2M random rows all 3072 are populated (verified against the running table). High-cardinality identifiers like `trace_id` / `span_id` are kept as fields, not tags, so they don't multiply the series count.

## Apples-to-apples: CPU schema (vs Go SDK)

A separate `cpu-bulk-api` bench mirrors the schema and data shape used in the [Greptime ingestion protocol benchmark blog](https://greptime.com/blogs/2026-03-24-ingestion-protocol-benchmark): 4 string tags + 5 Float64 fields + 1 ms timestamp, `numHosts × 5 × 10 × 20` series, round-robin distribution.

Same M4 Max / 48GB / GreptimeDB standalone as the blog:

| Config                          | Go SDK (blog)            | TS SDK (this repo)      |
| ------------------------------- | ------------------------ | ----------------------- |
| 1M series, 10M rows, batch=1000 | 2.01M rows/s (p50=1.7ms) | ~700k rows/s (p50=11ms) |

Today's gap is Arrow JS single-thread encoding (`rowsToArrowTable` = 99% of client CPU, measured via `bench/encode-only.ts`). We'll keep optimizing the encoder.

## Apples-to-apples: vs InfluxDB JS SDK & OpenTelemetry JS SDK

Three benches share the CPU schema above and pre-generate datasets with the same shape and cardinality (series layout + ms-stepped timestamps; Float64 values are re-rolled per run via `Math.random()`) through three JS clients, letting us isolate protocol/client overhead from schema effects. Ports are the GreptimeDB defaults: gRPC Bulk on `4001`, InfluxDB v2 and OTLP over HTTP on `4000`.

- `cpu-bulk-api` — our own `@greptime/ingester`, Arrow Flight bulk path. Writes a proper time-series table: 4-tag composite PK + 5 Float64 fields + ms timestamp.
- `cpu-influxdb` — `@influxdata/influxdb-client` v1.35, line protocol to `/v1/influxdb/api/v2/write`. GreptimeDB serves the InfluxDB v2 API natively; token is `"<user>:<password>"`. Writes the same tag/field shape; server parses LP and maps to the columnar path.
- `cpu-otel` — `@opentelemetry/exporter-logs-otlp-proto` v0.215, OTLP log records to `/v1/otlp/v1/logs`, with the `greptime_identity` pipeline (see below) so that log attributes are promoted into real columns. Without this header the resulting table is a generic log table whose 9 fields are collapsed into a `log_attributes` JSON blob — comparing that against the other two is not meaningful.

Measured on Apple M4 Max (16 cores) / 48GB / Node.js 22.17 / GreptimeDB `v1.0.0` in Docker, parallelism=8, numHosts=100 (100k series). Each cell is the median of 3 runs; tables are dropped between runs to measure fresh-insert throughput.

| Bench          | 1M rows, batch=1000 | p50  | p95  | p99  | 1M rows, batch=5000 | p50  | p95   | p99   |
| -------------- | ------------------- | ---- | ---- | ---- | ------------------- | ---- | ----- | ----- |
| `cpu-bulk-api` | **807k rows/s**     | 9ms  | 10ms | 17ms | **811k rows/s**     | 45ms | 62ms  | 82ms  |
| `cpu-influxdb` | 496k rows/s         | 13ms | 24ms | 26ms | 500k rows/s         | 71ms | 120ms | 127ms |
| `cpu-otel`     | 622k rows/s         | 11ms | 15ms | 53ms | 621k rows/s         | 55ms | 117ms | 154ms |

Run-to-run spread stayed ≤9% on throughput and the ranking (`cpu-bulk-api` > `cpu-otel` > `cpu-influxdb`) was identical across all three runs at both batch sizes.

Takeaways:

- Arrow Flight bulk wins by a comfortable margin: ~1.3× over OTLP and ~1.6× over InfluxDB LP. The advantage is on the server side: rows arrive as a ready-made Arrow columnar batch, no parsing or per-attribute promotion required.
- OTLP with `greptime_identity` pays for OTLP proto decode + per-attribute column mapping on the server, plus HTTP/1.1 framing. Still beats InfluxDB LP, which pays for text parsing on top of the same column mapping.
- Row counts were spot-checked out-of-band with `SELECT COUNT(*)` on each per-protocol table; the bench scripts themselves do not run the verification query.
- Even with `greptime_identity`, the OTel and bulk tables aren't strictly identical — the OTel table still carries log-model columns (`ScopeName`, `TraceId`, etc.) and has no `TAG`-marked primary key, so per-series semantics differ. The numbers here measure ingestion throughput only, not query-path parity.

### SDK usage notes

Both comparison SDKs are driven with their default config — no tuning of batch sizes, flush intervals, retry counts, or concurrency limits. Only the shape of the loop differs:

- **InfluxDB**: one `WriteApi` instance per worker (default `batchSize=1000`, `flushInterval=60000ms`, `maxRetries=3`). Each worker calls `writePoints(batch)` then awaits `flush()`. A shared `WriteApi` races on its internal buffer under concurrent flushes, and the JS SDK has no blocking-write equivalent of Go's `WriteAPIBlocking` — N instances is the minimal adaptation to drive N concurrent in-flight HTTP POSTs.
- **OTLP**: single `OTLPLogExporter` shared across workers (defaults). Each worker builds `ReadableLogRecord[]` and calls `exporter.export(records, cb)` directly. This mirrors Go's `exporter.Export(ctx, records)`. We don't go through `LoggerProvider` + `BatchLogRecordProcessor` because that path serialises exports internally and caps in-flight requests at 1, which would skew the comparison. The request carries three GreptimeDB headers: `X-Greptime-DB-Name`, `X-Greptime-Log-Table-Name`, and `X-Greptime-Pipeline-Name: greptime_identity` — the last one tells the server to expand each attribute into its own column instead of storing them as a `log_attributes` JSON blob.

## Running the comparison

Both comparison benches default to `http://localhost:4000` and can be pointed elsewhere via env or flags. GreptimeDB serves gRPC on `4001` and HTTP (InfluxDB + OTLP) on `4000`.

```bash
# GreptimeDB gRPC bulk — baseline
pnpm bench cpu-bulk-api --rows=1000000 --batch-size=1000 --parallelism=8

# InfluxDB line protocol
pnpm bench cpu-influxdb --rows=1000000 --batch-size=1000 --parallelism=8 \
  --http-endpoint=http://localhost:4000 --database=public

# OTLP logs
pnpm bench cpu-otel --rows=1000000 --batch-size=1000 --parallelism=8 \
  --http-endpoint=http://localhost:4000 --database=public
```

Env vars override flags: `GREPTIMEDB_HTTP_ENDPOINT`, `GREPTIMEDB_DATABASE`, `GREPTIMEDB_USER`, `GREPTIMEDB_PASSWORD`. Before rerunning, truncate the target table to keep measurements clean — the three benches write to `benchmark_grpc_bulk`, `benchmark_influxdb`, and `benchmark_otel` respectively.

## Running against your own deployment

```bash
./scripts/run-greptimedb.sh          # spin up local docker, or point to your own
pnpm bench bulk-api --rows=2000000 --batch-size=5000 --endpoint=localhost:4001
```

Available benchmark names: `regular-api`, `stream-api`, `bulk-api`, `cpu-bulk-api`, `cpu-influxdb`, `cpu-otel`. Shared flags:

- `--rows=N` — target row count; rounded down to a multiple of `--batch-size` (benches send whole batches only)
- `--batch-size=N` — per-`write()` batch
- `--parallelism=N` — concurrent in-flight RPCs (bulk / cpu-\* benches; default 8)
- `--num-hosts=N` — `cpu-*` benches only; cardinality = `N × 5 × 10 × 20` series (default 100 → 100k series; use 1000 for the blog's 1M-series config)
- `--endpoint=host:port` — gRPC endpoint (default `localhost:4001`); applies to `regular-api`, `stream-api`, `bulk-api`, `cpu-bulk-api`
- `--http-endpoint=URL` — HTTP endpoint (default `http://localhost:4000`); applies to `cpu-influxdb`, `cpu-otel`
- `--database=NAME`, `--user=NAME`, `--password=VALUE` — auth / db selection for the HTTP-based benches

## Tuning tips

- Raise `parallelism` above the default (8) to 12–16 if your producer is CPU-light and the server is far enough away that request latency dominates.
- Enable `BulkCompression.Lz4` or `BulkCompression.Zstd` for bandwidth-constrained links; LZ4 costs very little CPU, ZSTD compresses harder.
- For sustained ingest, prefer the bulk path over unary — the Arrow columnar layout amortizes encoding cost over the whole batch.
- Keep one `Client` per process. Channels are pooled internally.
