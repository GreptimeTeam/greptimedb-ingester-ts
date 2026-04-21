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

For fire-and-forget, use `writeRowsAsync(batch)` — it returns the request id and lets you submit the next batch immediately. **Every id must be claimed with `waitForResponse(id)` before `finish()`**; unclaimed acks accumulate in an internal map and are not auto-evicted, so skipping the claim in a long-running stream leaks memory. `writeRows(batch)` does the claim for you.

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

All errors extend `IngesterError`. Non-retriable: `ConfigError`, `SchemaError`, `ValueError`, `StateError`. Retriable or case-by-case: `TransportError` (`.grpcCode`), `ServerError` (`.statusCode`), `TimeoutError`, `AbortedError`, `BulkError`.

Classify with `isRetriable(err, 'aggressive' | 'conservative')`. Default is `aggressive` and mirrors the Rust SDK; `conservative` narrows to transient gRPC codes only.

## Examples

| File                                      | What                                                            |
| ----------------------------------------- | --------------------------------------------------------------- |
| `examples/01-simple-insert.ts`            | Table builder → `client.write`                                  |
| `examples/02-insert-object-decorators.ts` | `@tableName` / `@tag` / `@field` / `@timestamp` + `writeObject` |
| `examples/03-stream-insert.ts`            | `StreamWriter` with 10k rows                                    |
| `examples/04-bulk-insert.ts`              | Unary bootstrap → bulk 100k rows                                |
| `examples/05-bulk-compression-lz4.ts`     | LZ4 frame compression on the bulk path                          |
| `examples/06-auth-and-tls.ts`             | Basic auth + TLS config                                         |
| `examples/07-multi-endpoint-lb.ts`        | Multiple endpoints, random LB                                   |
| `examples/08-abort-and-retry.ts`          | `AbortSignal` + conservative retry                              |

Run any of them with `pnpm example <name>` after `./scripts/run-greptimedb.sh` starts a local server.

## Performance

Bulk path reaches **~141k rows/s** on local docker with a 22-column log schema at `parallelism=8`. Full numbers and how to reproduce: [docs/benchmarking.md](./docs/benchmarking.md).

## Compatibility

- Tested: Node.js 22.x + GreptimeDB 1.0.0 / latest.
- Node.js 20.x: supported (minimum).
- Bun / Deno (node-compat): best-effort, not CI-gated.

See [docs/divergences.md](./docs/divergences.md) for where the TS SDK intentionally differs from the Rust / Go SDKs.

## Roadmap

- VECTOR / LIST / STRUCT column types
- OpenTelemetry metrics + tracing integration
- Browser transport via gRPC-Web (separate package)

## Links

- [Documentation](./docs/) — configuration, benchmarking, divergences
- [Contributing](./CONTRIBUTING.md)
- [Security policy](./SECURITY.md)
- [Changelog](./CHANGELOG.md)
- [Issues](https://github.com/GreptimeTeam/greptimedb-ingester-ts/issues)

## License

Apache-2.0 — see [LICENSE](./LICENSE).
