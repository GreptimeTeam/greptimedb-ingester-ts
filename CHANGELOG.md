# Changelog

## Unreleased

Audit-driven correctness + performance pass. **Includes breaking public-API removals** — review before upgrading.

### Breaking

- **Removed `ConfigBuilder.withTokenAuth(token)` and the `token` variant of `AuthConfig`.** The GreptimeDB gRPC frontend explicitly rejects `AuthScheme::Token` (see `src/servers/src/grpc/context_auth.rs`). Exposing it was a footgun. Use basic auth or wait for server-side support.
- **Removed `AuthError` class.** It was exported but never constructed. gRPC UNAUTHENTICATED / PERMISSION_DENIED already surface as `TransportError` with the appropriate `grpcCode`.
- **Removed `Table.semantic` static helper.** It was unused and unexported at the index level.
- **`Client.close()` is now terminal**: any subsequent `write` / `writeObject` / `createStreamWriter` / `createBulkStreamWriter` throws `StateError`. Previously, the channel pool silently rebuilt a channel and the call would succeed against a pool the caller asked to stop. `close()` itself remains idempotent (a second call is a no-op).

### Added

- **`StateError` class** (exported from root): raised for "wrong state" calls — using a closed `Client`, writing to a finished `StreamWriter`, etc. Classified as non-retriable alongside `ConfigError` / `SchemaError` / `ValueError`.
- **Logger is wired up for real**: `cfg.logger` (set via `ConfigBuilder.withLogger`) now receives events at — (a) `debug` per retry attempt with `{attempt, backoffMs, errorKind}`; (b) `error` on bulk schema handshake failure and drain-loop errors; (c) `warn` once on bulk `completed` map LRU-eviction. Previously the logger was stored in config but never invoked.

### Fixed / Aligned

- **Wire format aligned with Rust SDK**: `hints` are now sent as a single `x-greptime-hints: k1=v1,k2=v2` header (matches Rust `database.rs:198-211`) instead of per-key `x-greptime-hint-<k>` headers. The server's `hint_headers.rs:19-37` accepts both forms (per-key as a whitelisted fallback for `auto_create_table`/`ttl`/`append_mode`/`merge_mode`/`physical_table`/`read_preference`), but the single-header form is unrestricted (any key passes through) and matches the canonical client.
- **Hints validation**: keys/values containing `,` or `=` now throw `ValueError` (the wire format has no escaping; silently mangling them is worse than rejecting).
- **Default change**: `BulkWriteOptions.parallelism` default 8 → 4, aligned with Rust SDK `bulk.rs:129`.
- **Perf**: module-level `TextEncoder`/`TextDecoder` singletons in flight-codec, arrow-encoder, value converter (was: `new` per row/frame).
- **Perf**: shared frozen `EMPTY_METADATA` reused for hint-less calls.
- **Perf**: `BulkStreamWriter` caches `Schema` / `Field[]` / Arrow type instances across batches; `rowsToArrowTable` accepts pre-computed schema.
- **Correctness**: `BulkStreamWriter` keeps completed async bulk acks retrievable until `waitForResponse(id)` consumes them; no silent dropping of old request ids.
- **Correctness**: bulk path now validates all scalar types (Int8/16/32, Uint8/16/32, Float32/64, Bool, String, Time*) before handing values to Arrow builders. Previously bulk silently coerced wrong-typed inputs (e.g. passing a string into an Int8 column produced NaN in the typed array), while unary correctly threw `ValueError`. Validators are shared between the two paths via new `src/table/validators.ts`.
- **Correctness**: JSON columns now throw `ValueError` (with the `TypeError` as cause) when the value contains a `bigint` field or a circular reference, instead of leaking the raw `TypeError` from `JSON.stringify`.
- **Minor perf**: `Table.addRowObject()` no longer rebuilds the column-name `Set` on every call; it's cached once at `freezeSchema()` time.

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
