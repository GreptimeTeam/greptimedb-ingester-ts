# Benchmarking

Reference numbers measured against a local `greptime/greptimedb:v1.0.0` container on an Apple M-series laptop. The schema is 22 columns (mix of tags, fields, timestamp) and mirrors the Rust SDK's log benchmark so numbers compare directly.

| Bench     | Setup                                | Throughput      | p50  | p95  | p99  |
| --------- | ------------------------------------ | --------------- | ---- | ---- | ---- |
| unary     | 20k rows, batch=1000                 | 26k rows/s      | 34ms | 41ms | 65ms |
| streaming | 100k rows, batch=1000                | 33k rows/s      | 26ms | 37ms | 46ms |
| bulk      | 200k rows, batch=5000, parallelism=8 | **141k rows/s** | 26ms | 31ms | 42ms |

## Running against your own deployment

```bash
./scripts/run-greptimedb.sh          # spin up local docker, or point to your own
pnpm bench bulk-api --rows=2000000 --batch-size=5000 --parallelism=8 --endpoint=localhost:4001
```

Available benchmark names: `unary`, `stream`, `bulk-api`. Shared flags:

- `--rows=N` — total rows to push
- `--batch-size=N` — per-`write()` batch
- `--parallelism=N` — concurrent in-flight RPCs (bulk only)
- `--endpoint=host:port`

## Tuning tips

- Raise `parallelism` from the default (8) to 12–16 if your producer is CPU-light and the server is far enough away that request latency dominates.
- Enable `BulkCompression.Lz4` or `BulkCompression.Zstd` for bandwidth-constrained links; LZ4 costs very little CPU, ZSTD compresses harder.
- For sustained ingest, prefer the bulk path over unary — the Arrow columnar layout amortizes encoding cost over the whole batch.
- Keep one `Client` per process. Channels are pooled internally.
