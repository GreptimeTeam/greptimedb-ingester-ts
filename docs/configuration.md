# Configuration reference

The client is built through `ConfigBuilder`. Every method returns the same builder, so calls chain.

```ts
import { Client, consoleLogger } from '@greptime/ingester';

const cfg = Client.create('localhost:4001')
  .withEndpoints('host2:4001', 'host3:4001') // random-peer load balancing
  .withDatabase('public')
  .withBasicAuth('admin', 'pw')
  .withTls({ kind: 'system' })
  .withTimeout(60_000)
  .withKeepAlive(30_000, 10_000)
  .withMaxMessageSize(128 * 1024 * 1024) // receive == send by default
  .withGrpcCompression('gzip')
  .withRetry({ mode: 'aggressive', maxAttempts: 3 })
  .withLogger(consoleLogger('info'))
  .withUserAgent('my-service/1.0')
  .build();

const client = new Client(cfg);
```

## Endpoints and load balancing

`withEndpoints(...endpoints)` accepts one or more `host:port` strings. Multiple endpoints are used as a random-peer pool (matches the Rust SDK) — each RPC independently picks a peer. No sticky routing, no health checking beyond gRPC's own connection state.

## Authentication

Only `withBasicAuth(username, password)` is supported. Token authentication is not exposed because the GreptimeDB gRPC frontend rejects `AuthScheme::Token`.

Auth flows through two transports, which the SDK wires automatically:

- Unary and streaming writes — proto field `RequestHeader.authorization`.
- Bulk (Arrow Flight) — gRPC metadata `x-greptime-auth` plus `x-greptime-db-name`.

## TLS

```ts
.withTls({ kind: 'system' })                            // OS trust store
.withTls({ kind: 'pem', ca, cert, key })                // in-memory PEM
.withTls({ kind: 'file', caPath, certPath, keyPath })   // disk paths
```

Each variant accepts an optional `serverNameOverride` for SNI.

## Timeouts and keepalive

- `withTimeout(ms)` — per-RPC deadline. Default `60_000`.
- `withKeepAlive(timeMs, timeoutMs)` — HTTP/2 PING interval and ACK timeout. Default `30_000` / `10_000`.

## Message size limits

`withMaxMessageSize(receiveBytes, sendBytes?)` — cap gRPC frame size for both directions. Default **128 MB** on both. Raise it if you send very wide batches; lower it to fail fast in constrained containers.

## gRPC transport compression

`withGrpcCompression('none' | 'gzip' | 'deflate')` — default `'none'`. This is wire-level gRPC compression, orthogonal to the bulk path's Arrow IPC body compression (`BulkCompression.Lz4` / `.Zstd`).

## Retry

```ts
.withRetry({
  mode: 'aggressive',        // 'aggressive' | 'conservative'
  maxAttempts: 3,
  initialBackoffMs: 100,
  maxBackoffMs: 5_000,
  backoffMultiplier: 2,
  jitter: 'full',            // 'full' | 'none'
})
```

- `aggressive` (default) mirrors the Rust SDK's `is_retriable`: everything retriable except local `ConfigError` / `SchemaError` / `ValueError` / `StateError`.
- `conservative` narrows to transient gRPC codes: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `UNKNOWN`.

Classify an error directly with `isRetriable(err, 'aggressive' | 'conservative')`.

## Logger

`withLogger(logger)` is optional; defaults to no-op. The SDK emits:

| Level   | Event                                                                                          |
| ------- | ---------------------------------------------------------------------------------------------- |
| `debug` | each retry attempt, with `{ attempt, backoffMs, errorKind }`                                   |
| `error` | bulk schema-handshake failure or drain-loop failure                                            |
| `info`  | at bulk `finish()` if any `writeRowsAsync()` acks were never claimed via `waitForResponse(id)` |

Pass `consoleLogger('info' \| 'debug' \| ...)` for a simple stdout logger, or implement the `Logger` interface for anything custom.
