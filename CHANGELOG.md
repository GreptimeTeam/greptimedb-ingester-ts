# Changelog

## 0.1.0-alpha.0 — 2026-04-19

Initial release.

- `Client` with three write modes: unary (`write`/`writeObject`), streaming (`HandleRequests`), bulk (Arrow Flight `DoPut`).
- `Table` builder + Stage-3 decorators (`@tableName`, `@tag`, `@field`, `@timestamp`).
- Full value conversion with strict `bigint` / `number` handling and `Date` timestamp scaling across all four precisions.
- Configurable retry (aggressive / conservative) + `AbortSignal` throughout.
- Random-peer load balancing across multiple endpoints, TLS three modes (system / PEM / file), basic auth, gzip transport compression.
- 70 unit + 5 integration tests, 8 examples, 3 end-to-end benchmarks mirroring Rust's 22-col log schema.
- Dual ESM + CJS via `tshy`.

### Known limitations

- LZ4 / ZSTD bulk compression raises `ConfigError` — apache-arrow JS 18.x has no IPC body compression support yet. Planned for 0.2.x.
- No gRPC-web / browser transport. Node.js-only for 0.1.
- Token authentication is not exposed: GreptimeDB's gRPC frontend currently rejects `AuthScheme::Token` — only basic auth works on both unary/streaming and bulk paths.
