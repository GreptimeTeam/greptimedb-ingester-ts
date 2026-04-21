# Changelog

## [0.1.0] — 2026-04-21

Initial public release.

### Added

- Three write modes on one `Client`: unary (`write` / `writeObject`), streaming (`HandleRequests`), bulk (Arrow Flight `DoPut`).
- `Table` builder plus Stage-3 decorators (`@tableName`, `@tag`, `@field`, `@timestamp`) — no `reflect-metadata`.
- Bulk body compression via LZ4_FRAME (`lz4-napi`) and ZSTD (`@mongodb-js/zstd`); both declared as `optionalDependencies`, loaded on demand, enabled through `BulkCompression.Lz4` / `BulkCompression.Zstd`.
- Configurable retry (`aggressive` / `conservative`) with full-jitter exponential backoff; `AbortSignal` honored throughout.
- Random-peer load balancing across endpoints, TLS (system / PEM / file), basic auth, gzip transport compression.
- `StateError` for closed-client / finished-stream misuse; `Client.close()` is terminal (idempotent).
- Logger (`ConfigBuilder.withLogger`) receives `debug` on each retry attempt, `error` on bulk schema-handshake / drain-loop failure, and `info` at `finish()` if any `writeRowsAsync()` acks were never claimed via `waitForResponse(id)`.
- Hints wire format matches the Rust SDK: single `x-greptime-hints: k1=v1,k2=v2` header. Keys or values containing `,` or `=` are rejected with `ValueError`.
- Strict TypeScript (`strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`), dual ESM + CJS via `tshy`, Node.js ≥ 20.
- 70+ unit and integration tests (testcontainers + `greptime/greptimedb:v1.0.0`), 8 runnable examples, 3 end-to-end benchmarks mirroring the Rust SDK's 22-column log schema.

### Known limitations

- Node.js only; no gRPC-web / browser transport in this release.
- Token authentication is not exposed: the GreptimeDB gRPC frontend rejects `AuthScheme::Token`. Only basic auth works today.
