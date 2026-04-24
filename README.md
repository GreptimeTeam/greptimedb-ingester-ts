# @greptime/ingester

[![npm](https://img.shields.io/npm/v/@greptime/ingester.svg)](https://www.npmjs.com/package/@greptime/ingester)
[![CI](https://github.com/GreptimeTeam/greptimedb-ingester-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-ts/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A520-brightgreen.svg)](./package.json)

Official TypeScript ingester SDK for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb). gRPC row inserts, streaming inserts, and Arrow Flight bulk writes in one package.

## Features

- Three write modes on one `Client`: unary, streaming, and Arrow Flight bulk with LZ4 / ZSTD body compression
- Stage-3 decorators (TS 5) for object mapping — no `reflect-metadata`
- TLS (system / PEM / file), basic auth, gzip transport compression, random-peer load balancing
- Configurable retry (`aggressive` / `conservative`) with full-jitter exponential backoff + `AbortSignal`
- Dual ESM + CJS, strict TypeScript, Node.js ≥ 20

## Install

```bash
pnpm add @greptime/ingester
# or: npm install @greptime/ingester
# or: yarn add @greptime/ingester
```

## Quickstart

```ts
import { Client, DataType, Precision, Table } from '@greptime/ingester';

const client = new Client(Client.create('localhost:4001').withDatabase('public').build());

const table = Table.new('cpu_usage')
  .addTagColumn('host', DataType.String)
  .addFieldColumn('usage', DataType.Float64)
  .addTimestampColumn('ts', Precision.Millisecond)
  .addRow(['server-01', 75.3, Date.now()]);

const result = await client.write(table);
console.log(`inserted ${result.value} rows`);
await client.close();
```

## Three write modes

| Mode      | API                                     | When                                                     |
| --------- | --------------------------------------- | -------------------------------------------------------- |
| Unary     | `client.write(tables)`                  | &lt;1k rows/s, mixed schemas, supports auto-create table |
| Streaming | `client.createStreamWriter()`           | Sustained writes on a single connection                  |
| Bulk      | `client.createBulkStreamWriter(schema)` | &gt;10k rows/s, Arrow Flight DoPut with parallelism      |

### Streaming

```ts
const stream = client.createStreamWriter();
for (const batch of batches) await stream.write(batch);
const { value } = await stream.finish();
```

The stream is not auto-retried — rebuild it on error.

### Bulk

```ts
// Prerequisite: the table exists with this schema. One unary write auto-creates it.
await client.write(buildTable().addRow([...sampleRow]));
const schema = buildTable().schema();

const bulk = await client.createBulkStreamWriter(schema);
for (const batch of batches) {
  await bulk.writeRows({ kind: 'rows', rows: batch });
}
const { totalAffectedRows } = await bulk.finish();
```

For LZ4 / ZSTD body compression, pass `{ compression: BulkCompression.Lz4 }`. See [`examples/05-bulk-compression-lz4.ts`](./examples/05-bulk-compression-lz4.ts).

For fire-and-forget, use `writeRowsAsync(batch)` — returns the request id and lets you submit the next batch immediately. `finish()` waits for every in-flight call and throws `BulkError` if any rejected, or if any group settled as a failure and was never claimed via `waitForResponse(id)` — silent partial ingestion isn't possible. Unclaimed acks are capped at `maxUnclaimedResponses` (default 10_000, oldest-first eviction). Claim ids with `waitForResponse(id)` when you need per-batch `affectedRows`; otherwise `writeRows(batch)` does the claim for you.

## Decorator API

Stage-3 decorators. Keep `experimentalDecorators` **off** in your tsconfig.

```ts
import { Client, DataType, Precision, field, tableName, tag, timestamp } from '@greptime/ingester';

@tableName('cpu_usage')
class CpuMetric {
  @tag(DataType.String) host!: string;
  @field(DataType.Float64) usage!: number;
  @timestamp({ precision: Precision.Millisecond }) ts!: number | Date;
}

await client.writeObject([
  Object.assign(new CpuMetric(), { host: 'a', usage: 1.5, ts: Date.now() }),
]);
```

## Configuration

```ts
Client.create('host:port')
  .withDatabase('public')
  .withBasicAuth('user', 'pw')
  .withTls({ kind: 'system' })
  .withRetry({ mode: 'aggressive', maxAttempts: 3 })
  .build();
```

Full reference: [docs/configuration.md](./docs/configuration.md).

## Errors

All errors extend `IngesterError`. Non-retriable: `ConfigError`, `SchemaError`, `ValueError`, `StateError`, `AbortedError`. Retriable or case-by-case: `TransportError` (`.grpcCode`), `ServerError` (`.statusCode`), `TimeoutError`, `BulkError`.

Classify with `isRetriable(err, 'aggressive' | 'conservative')`. Default is `aggressive` and mirrors the Rust SDK; `conservative` narrows to transient gRPC codes only.

## Examples

| File                                                                                   | What                                                            |
| -------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| [`examples/01-simple-insert.ts`](./examples/01-simple-insert.ts)                       | Table builder → `client.write`                                  |
| [`examples/02-insert-object-decorators.ts`](./examples/02-insert-object-decorators.ts) | `@tableName` / `@tag` / `@field` / `@timestamp` + `writeObject` |
| [`examples/03-stream-insert.ts`](./examples/03-stream-insert.ts)                       | `StreamWriter` with 10k rows                                    |
| [`examples/04-bulk-insert.ts`](./examples/04-bulk-insert.ts)                           | Unary bootstrap → bulk 100k rows                                |
| [`examples/05-bulk-compression-lz4.ts`](./examples/05-bulk-compression-lz4.ts)         | LZ4 frame compression on the bulk path                          |
| [`examples/06-auth-and-tls.ts`](./examples/06-auth-and-tls.ts)                         | Basic auth + TLS config                                         |
| [`examples/07-multi-endpoint-lb.ts`](./examples/07-multi-endpoint-lb.ts)               | Multiple endpoints, random LB                                   |
| [`examples/08-abort-and-retry.ts`](./examples/08-abort-and-retry.ts)                   | `AbortSignal` + conservative retry                              |

Run any of them with `pnpm example <name>` after `./scripts/run-greptimedb.sh` starts a local server.

## Performance

Writing to GreptimeDB from Node.js? The bulk path is the fastest option by a wide margin. Median of 3 runs against a local GreptimeDB on an Apple M4 Max, 1M rows with the 4-tag / 5-field CPU schema, default SDK config, `parallelism=8`:

| JS client                                 | batch=1000   | batch=5000   | Relative     |
| ----------------------------------------- | ------------ | ------------ | ------------ |
| `@greptime/ingester` (bulk)               | **789k r/s** | **758k r/s** | **baseline** |
| `@opentelemetry/exporter-logs-otlp-proto` | 679k r/s     | 638k r/s     | 0.85×        |
| `@influxdata/influxdb-client`             | 494k r/s     | 520k r/s     | 0.66×        |

Same schema, same data generator, same server, same Node.js runtime; each client is driven with its own default configuration. Arrow Flight ships the batch already-columnar so the server skips text/proto parsing and per-attribute column mapping.

On the 22-column log schema the bulk path reaches **~137k rows/s** (2M rows, batch=5000). Unary and streaming numbers, the exact SDK-usage decisions behind each bench, and reproduction commands: [docs/benchmarking.md](./docs/benchmarking.md).

## Compatibility

- CI-tested: Node.js 20.x and 22.x, full suite; integration tests against `greptime/greptimedb:v1.0.0`.
- Node.js 20.x is the supported minimum.
- Bun (latest) and Deno (2.x): CI-gated via a smoke-level integration test against a live GreptimeDB. Full unit suite runs on Node only.

See [docs/divergences.md](./docs/divergences.md) for where the TS SDK intentionally differs from the Rust / Go SDKs.

## Roadmap

- Multi-endpoint failover
  - Pluggable `EndpointSelector` (random / round-robin / health-aware with outlier detection), replacing the current fixed random pick
  - Retry-time exclusion of already-failed peers so a single dead endpoint cannot burn the entire retry budget
  - Auto-reconnect wrapper for streaming and bulk writers (re-pick endpoint, rebuild the stream, surface a resumable handle)
- Off-main-thread Arrow encoding (worker_threads pool) to close the TS↔Go throughput gap on wide-schema bulk — today `rowsToArrowTable` is ~99% of client CPU (see [docs/benchmarking.md](./docs/benchmarking.md))
- JSON v2 column type (binary JSON encoding)
- OpenTelemetry instrumentation of the SDK itself (write latency, retries, bulk/stream state as metrics + spans)
- Browser build via gRPC-Web in a separate `@greptime/ingester-web` package (unary + streaming only; no bulk)

## Links

- [Documentation](./docs/) — configuration, benchmarking, divergences
- [Contributing](./CONTRIBUTING.md)
- [Security policy](./SECURITY.md)
- [Changelog](./CHANGELOG.md)
- [Issues](https://github.com/GreptimeTeam/greptimedb-ingester-ts/issues)

## License

Apache-2.0 — see [LICENSE](./LICENSE).
