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

## Running against your own deployment

```bash
./scripts/run-greptimedb.sh          # spin up local docker, or point to your own
pnpm bench bulk-api --rows=2000000 --batch-size=5000 --endpoint=localhost:4001
```

Available benchmark names: `regular-api`, `stream-api`, `bulk-api`, `cpu-bulk-api`. Shared flags:

- `--rows=N` — total rows to push
- `--batch-size=N` — per-`write()` batch
- `--parallelism=N` — concurrent in-flight RPCs (bulk only; default 8)
- `--num-hosts=N` — `cpu-bulk-api` only; cardinality = `N × 5 × 10 × 20` series (default 100 → 100k series; use 1000 for the blog's 1M-series config)
- `--endpoint=host:port`

## Tuning tips

- Raise `parallelism` above the default (8) to 12–16 if your producer is CPU-light and the server is far enough away that request latency dominates.
- Enable `BulkCompression.Lz4` or `BulkCompression.Zstd` for bandwidth-constrained links; LZ4 costs very little CPU, ZSTD compresses harder.
- For sustained ingest, prefer the bulk path over unary — the Arrow columnar layout amortizes encoding cost over the whole batch.
- Keep one `Client` per process. Channels are pooled internally.
