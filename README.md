# @greptime/ingester

Official TypeScript ingester SDK for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb). gRPC row inserts, streaming inserts, and Arrow Flight bulk writes in one package.

- Node.js **≥ 20**, dual ESM + CJS
- Strict TypeScript (`strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`)
- TS 5 Stage-3 decorators (no `reflect-metadata`)
- `@bufbuild/protobuf` 2.x + `@grpc/grpc-js` 1.x + `apache-arrow` 18.x

## Install

```bash
pnpm add @greptime/ingester
# or npm install @greptime/ingester
```

## Quickstart

```ts
import { Client, DataType, Precision, Table } from '@greptime/ingester';

const client = new Client(
  Client.create('localhost:4001').withDatabase('public').build(),
);

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

| Mode | API | When |
|---|---|---|
| Unary | `client.write(tables)` | &lt;1k rows/s, mixed schemas, supports auto-create table |
| Streaming | `client.createStreamWriter()` | Sustained writes on a single connection |
| Bulk | `client.createBulkStreamWriter(schema)` | &gt;10k rows/s, Arrow Flight DoPut with parallelism |

### Unary

Retriable (policy configurable via `ConfigBuilder.withRetry`). Supports `AbortSignal`, `timeoutMs`, and custom hints.

### Streaming (`HandleRequests`)

```ts
const stream = client.createStreamWriter();
for (const batch of batches) await stream.write(batch);
const { value } = await stream.finish();
```

Internal state machine rejects writes after `finish()` / `cancel()`. Not auto-retried — the caller rebuilds the stream on error.

### Bulk (Arrow Flight `DoPut`)

```ts
// Prerequisite: the table must already exist with the exact schema.
// Idiomatic bootstrap — one unary write (which auto-creates), then bulk:
await client.write(buildTable().addRow([...sampleRow]));
const schema = buildTable().schema();

const bulk = await client.createBulkStreamWriter(schema, { parallelism: 8 });
for (const batch of batches) {
  await bulk.writeRows({ kind: 'rows', rows: batch });
}
const { totalAffectedRows } = await bulk.finish();
```

Bulk runs a bidirectional Flight stream. The first frame carries the Arrow schema + `FlightDescriptor.PATH = [table]`; subsequent frames carry RecordBatches with `app_metadata = {"request_id": N}` (JSON). Server responses are demultiplexed by `request_id` and can arrive out of order.

## Decorator API (object mapper)

Stage-3 decorators (TS 5). `experimentalDecorators` must stay **off** in your tsconfig.

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

## Configuration reference

```ts
Client.create('host:port')
  .withEndpoints('host2:port', 'host3:port')       // random-peer LB (matches Rust)
  .withDatabase('public')
  .withBasicAuth('admin', 'pw')                    // or .withTokenAuth('...')
  .withTls({ kind: 'system' })                     // system | pem | file
  .withTimeout(60_000)
  .withKeepAlive(30_000, 10_000)
  .withGrpcCompression('gzip')                     // none | gzip | deflate
  .withRetry({ mode: 'aggressive', maxAttempts: 3 })
  .withLogger(consoleLogger('info'))
  .build();
```

Auth flows through two transports (by protocol design):
- Unary / streaming: proto `RequestHeader.authorization`
- Bulk: gRPC metadata `x-greptime-auth` + `x-greptime-db-name`

The SDK wires both for you — you configure auth once.

## Errors

Class hierarchy (all extend `IngesterError`):

- `ConfigError`, `SchemaError`, `ValueError` — never retriable
- `TransportError` (with `.grpcCode`), `ServerError` (with `.statusCode`), `TimeoutError`, `AbortedError`, `AuthError`, `BulkError`

Classify with `isRetriable(err, 'aggressive' | 'conservative')`. Default is `aggressive` and mirrors Rust's `is_retriable`: every runtime error except local config/schema/value errors is retriable. `conservative` narrows to transient gRPC codes (`UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` / `ABORTED` / `UNKNOWN`).

## Performance

22-column synthetic log schema against local GreptimeDB docker (`greptime/greptimedb:v1.0.0`, Apple M-series):

| Bench | Setup | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|
| unary | 20k rows, batch=1000 | 26k rows/s | 34ms | 41ms | 65ms |
| streaming | 100k rows, batch=1000 | 33k rows/s | 26ms | 37ms | 46ms |
| bulk | 200k rows, batch=5000, parallelism=8 | **141k rows/s** | 26ms | 31ms | 42ms |

Run against your own deployment:
```bash
pnpm bench bulk-api --rows=2000000 --batch-size=5000 --parallelism=8 --endpoint=greptime:4001
```

## Intentional divergences from Rust/Go

| | Rust/Go | TS | Why |
|---|---|---|---|
| gRPC transport compression | ZSTD | gzip | `@grpc/grpc-js` does not ship a ZSTD codec |
| Bulk body compression | LZ4 / ZSTD | None (v0.1) | apache-arrow JS 18.x does not emit IPC body-compression; planned v0.2 |
| DoPut metadata encoding | `serde_json` (i64) | `JSON.stringify` (number) | JS has no native BigInt JSON; request_id &lt; 2^53 is always enough |
| Retry policy | single default | `aggressive` (default, matches Rust) + `conservative` knob | TS ecosystem convention |

## Examples

| File | What |
|---|---|
| `examples/01-simple-insert.ts` | Table builder → `client.write` |
| `examples/02-insert-object-decorators.ts` | `@tableName`/`@tag`/`@field`/`@timestamp` + `writeObject` |
| `examples/03-stream-insert.ts` | `StreamWriter` with 10k rows |
| `examples/04-bulk-insert.ts` | Unary bootstrap → bulk 100k rows |
| `examples/05-bulk-compression-lz4.ts` | Shows the v0.1 `ConfigError` for LZ4 |
| `examples/06-auth-and-tls.ts` | Basic auth + TLS config |
| `examples/07-multi-endpoint-lb.ts` | Multiple endpoints, random LB |
| `examples/08-abort-and-retry.ts` | `AbortSignal` + conservative retry |

Each is runnable as `pnpm example <name>` after `./scripts/run-greptimedb.sh` starts a local server.

## Compatibility

- Tested: Node.js 22.x + GreptimeDB 1.0.0 / latest.
- Node.js 20.x: supported (minimum).
- Bun / Deno (node-compat): best-effort. `@grpc/grpc-js` and `apache-arrow` both ship — not CI-gated.

## Roadmap

- LZ4 / ZSTD IPC body compression (pending Arrow JS support or flatbuffer-level fallback via `lz4-napi` / `@mongodb-js/zstd`)
- VECTOR / LIST / STRUCT column types
- OpenTelemetry metrics + tracing integration
- Browser transport via gRPC-Web (separate package)

## Contributing

```bash
pnpm install
./scripts/vendor-proto.sh     # refresh proto from ../rust/greptime-proto
pnpm codegen                   # regenerate src/generated/
pnpm typecheck && pnpm lint && pnpm test
./scripts/run-greptimedb.sh    # local docker
pnpm example 01-simple-insert  # smoke-test
```

Integration tests: `INTEGRATION=1 pnpm test:integration` (requires docker / testcontainers).

## License

Apache-2.0 — see `LICENSE`.
