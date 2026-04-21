# Benchmarking

Reference numbers measured against a local `greptime/greptimedb:v1.0.0` container on an Apple M-series laptop. The schema is 22 columns (mix of tags, fields, timestamp) and mirrors the Rust SDK's log benchmark so numbers compare directly.

| Bench     | Setup               | Throughput      | p50  | p95  | p99  |
| --------- | ------------------- | --------------- | ---- | ---- | ---- |
| unary     | 1M rows, batch=1000 | 29k rows/s      | 32ms | 36ms | 39ms |
| streaming | 1M rows, batch=1000 | 34k rows/s      | 26ms | 37ms | 48ms |
| bulk      | 2M rows, batch=5000 | **141k rows/s** | 26ms | 29ms | 37ms |

All three benchmarks use the same schema and the same ~3072-series cardinality (see [Schema](#schema)). Bulk uses the default `parallelism=8`. Batch sizes mirror the Rust SDK's published log benchmark.

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

## Running against your own deployment

```bash
./scripts/run-greptimedb.sh          # spin up local docker, or point to your own
pnpm bench bulk-api --rows=2000000 --batch-size=5000 --endpoint=localhost:4001
```

Available benchmark names: `regular-api`, `stream-api`, `bulk-api`. Shared flags:

- `--rows=N` — total rows to push
- `--batch-size=N` — per-`write()` batch
- `--parallelism=N` — concurrent in-flight RPCs (bulk only; default 8)
- `--endpoint=host:port`

## Tuning tips

- Raise `parallelism` above the default (8) to 12–16 if your producer is CPU-light and the server is far enough away that request latency dominates.
- Enable `BulkCompression.Lz4` or `BulkCompression.Zstd` for bandwidth-constrained links; LZ4 costs very little CPU, ZSTD compresses harder.
- For sustained ingest, prefer the bulk path over unary — the Arrow columnar layout amortizes encoding cost over the whole batch.
- Keep one `Client` per process. Channels are pooled internally.
