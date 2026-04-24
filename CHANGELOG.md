# Changelog

## [0.1.0] — 2026-04-24

Initial public release.

### Features

- Three write modes on one `Client`: unary (`write` / `writeObject`), streaming (`HandleRequests`), bulk (Arrow Flight `DoPut`).
- `Table` builder plus Stage-3 decorators (`@tableName`, `@tag`, `@field`, `@timestamp`) — no `reflect-metadata`.
- All scalar column types supported end-to-end (Int/Uint 8–64, Float32/64, Bool, String, Binary, Date, Datetime, Timestamp{Second|Millisecond|Microsecond|Nanosecond}, Time{Second|Millisecond|Microsecond|Nanosecond}, JSON). Range validation is identical across unary and bulk paths; Arrow Time32[s|ms] columns carry `number`, Time64[us|ns] carry `bigint`, matching apache-arrow's typed-array widths.
- `BulkStreamWriter` with per-batch parallelism, configurable settled-response cache (`maxUnclaimedResponses`, default `10_000`, oldest-first eviction), and a `finish()` that awaits in-flight writes and surfaces both fire-and-forget rejections and unclaimed group failures as `BulkError`. No silent partial ingestion.
- Bulk body compression via LZ4_FRAME (`lz4-napi`) and ZSTD (`@mongodb-js/zstd`); optional native deps, loaded on demand, enabled through `BulkCompression.Lz4` / `BulkCompression.Zstd`.
- DoPut server ack validation: malformed / fractional / out-of-safe-range `request_id` or `affected_rows` throws `BulkError` instead of miscorrelating acks.
- Configurable retry (`aggressive` / `conservative`) with full-jitter exponential backoff. `AbortedError` is non-retriable; `AbortSignal` is honored throughout.
- Random-peer load balancing across endpoints, TLS (system / PEM / file), basic auth, gzip transport compression.
- `StateError` on closed-client / finished-stream misuse; `Client.close()` is terminal (idempotent).
- Logger hook (`ConfigBuilder.withLogger`): `debug` per retry attempt, `warn` on cache eviction, `error` on bulk handshake / drain failures, `info` on finish with unclaimed acks.
- Hints wire format matches the Rust SDK: single `x-greptime-hints: k1=v1,k2=v2` header; keys or values containing `,` or `=` are rejected with `ValueError`.
- Strict TypeScript (`strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`), ES2022 `Error(cause)`, dual ESM + CJS via `tshy`, Node.js ≥ 20; smoke-tested against Bun (latest) and Deno 2.x in CI.
- 180+ unit and integration tests (testcontainers + `greptime/greptimedb:v1.0.0`), 8 runnable examples, 3 end-to-end benchmarks mirroring the Rust SDK's 22-column log schema.

### Known limitations

- Node.js only; no gRPC-web / browser transport in this release.
- Token authentication is not exposed: the GreptimeDB gRPC frontend rejects `AuthScheme::Token`. Only basic auth works today.
