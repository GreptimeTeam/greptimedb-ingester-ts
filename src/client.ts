import { ConfigBuilder, type ClientConfig } from './config.js';
import { StateError } from './errors.js';
import { NOOP_LOGGER, type Logger } from './internal/logger.js';
import { ChannelPool, pickRandom } from './transport/channel.js';
import { withRetry } from './transport/retry.js';
import type { Table } from './table/table.js';
import { objectsToTables } from './decorators/object-mapper.js';
import { StreamWriter, type StreamOptions } from './write/stream-writer.js';
import { performUnaryWrite, type WriteOptions } from './write/unary.js';
import type { AffectedRows } from './write/affected-rows.js';
import { BulkStreamWriter, type BulkWriteOptions } from './bulk/bulk-stream-writer.js';
import type { TableSchema } from './table/schema.js';

/**
 * GreptimeDB ingest client. Thread-safe, reusable across async operations — internally
 * maintains a `ChannelPool` keyed by endpoint.
 */
export class Client {
  private readonly cfg: ClientConfig;
  private readonly pool: ChannelPool;
  private readonly logger: Logger;
  private _closed = false;

  public constructor(cfg: ClientConfig) {
    this.cfg = cfg;
    this.pool = new ChannelPool(cfg);
    this.logger = cfg.logger ?? NOOP_LOGGER;
  }

  /**
   * Guard used by every public write/stream/bulk method. Calls made after `close()`
   * must fail deterministically — otherwise the channel pool would silently recreate
   * a channel and the call would "succeed" against a pool the caller asked to stop.
   */
  private ensureOpen(): void {
    if (this._closed) throw new StateError('client is closed');
  }

  /** Shortcut that returns a `ConfigBuilder` seeded with a single endpoint. */
  public static create(endpoint: string): ConfigBuilder {
    return ConfigBuilder.create(endpoint);
  }

  /**
   * Send one or more `Table`s in a single unary insert. Retries are applied per the
   * configured `RetryPolicy`. Rejects with an `IngesterError` subclass on failure.
   */
  public async write(tables: Table | readonly Table[], opts?: WriteOptions): Promise<AffectedRows> {
    this.ensureOpen();
    const list = Array.isArray(tables) ? tables : [tables as Table];
    return withRetry(
      () => {
        this.ensureOpen();
        const peer = pickRandom(this.cfg.endpoints);
        const channel = this.pool.get(peer);
        return performUnaryWrite(channel, this.cfg, list, opts);
      },
      this.cfg.retry,
      opts?.signal,
      this.logger,
    );
  }

  /**
   * Send decorator-annotated object instances. Instances are grouped by class into one
   * `Table` per distinct class, then sent in a single unary insert.
   */
  public async writeObject(
    instances: readonly object[],
    opts?: WriteOptions,
  ): Promise<AffectedRows> {
    this.ensureOpen();
    const tables = objectsToTables(instances);
    return this.write(tables, opts);
  }

  /**
   * Open a client-streaming writer (`HandleRequests`). Call `write(tables)` repeatedly,
   * then `finish()` to half-close and receive the aggregate `AffectedRows`. Streaming is
   * not auto-retried; callers must handle transport errors and rebuild the stream.
   */
  public createStreamWriter(opts?: StreamOptions): StreamWriter {
    this.ensureOpen();
    const peer = pickRandom(this.cfg.endpoints);
    const channel = this.pool.get(peer);
    return new StreamWriter(channel, this.cfg, opts);
  }

  /**
   * Open a bulk Arrow Flight DoPut writer.
   *
   * Prerequisites (not enforced client-side, server will reject otherwise):
   *   (a) The table must already exist — this path does not auto-create.
   *   (b) The passed `schema` must exactly match the existing table's schema.
   *
   * Typical pattern: call `client.write(sampleTable)` once with a single representative
   * row (unary path does auto-create), then open bulk with the matching schema.
   */
  public async createBulkStreamWriter(
    schema: TableSchema,
    opts?: BulkWriteOptions,
  ): Promise<BulkStreamWriter> {
    this.ensureOpen();
    const peer = pickRandom(this.cfg.endpoints);
    const channel = this.pool.get(peer);
    return BulkStreamWriter.open(channel, this.cfg, schema, opts);
  }

  /**
   * Release all channels and mark the client as terminal. Any subsequent write,
   * streaming, or bulk call throws `StateError`. Idempotent: a second `close()`
   * call is a no-op.
   */
  public close(): Promise<void> {
    if (this._closed) return Promise.resolve();
    this._closed = true;
    this.pool.close();
    return Promise.resolve();
  }

  public isClosed(): boolean {
    return this._closed;
  }
}
