# Intentional divergences from the Rust / Go SDKs

The TS SDK aims for wire-compatibility with the Rust and Go ingesters. Where it diverges, the choice is deliberate and listed here.

| Area                          | Rust / Go             | TS                                           | Why                                                                                                                                 |
| ----------------------------- | --------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| gRPC transport compression    | ZSTD                  | gzip                                         | `@grpc/grpc-js` does not ship a ZSTD wire codec                                                                                     |
| DoPut `app_metadata` encoding | `serde_json` with i64 | `JSON.stringify` with number                 | JavaScript has no native BigInt JSON. `request_id < 2^53` is always enough for a single stream                                      |
| Retry policy knob             | one default mode      | `aggressive` default + `conservative` option | TS ecosystem convention; `aggressive` still matches the Rust SDK's `is_retriable` behavior                                          |
| `maxMessageSize` default      | 512 MB                | 128 MB                                       | 512 MB risks OOM in typical Node.js containers. Raise it via `withMaxMessageSize()` if you send very wide batches                   |
| Bulk default `parallelism`    | 4                     | 8                                            | The Node event loop comfortably handles 8 in-flight RPCs; it lands directly on the measured sweet spot for the published benchmarks |

## Not a divergence

- **Bulk body compression**: LZ4 and ZSTD are implemented and wire-compatible with the Rust / Go ingesters. Enable via `BulkCompression.Lz4` or `BulkCompression.Zstd`. The compressors are optional native dependencies (`lz4-napi`, `@mongodb-js/zstd`) — installed by default, but a `--no-optional` install will throw a `ConfigError` with an install hint when compression is first requested.
- **Hints header**: the TS SDK emits the canonical single-header form (`x-greptime-hints: k1=v1,k2=v2`), same as the Rust SDK. The server still accepts the older per-key fallback headers for a whitelist of keys, but we do not use that path.
