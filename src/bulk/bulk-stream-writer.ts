// Bulk path via Arrow Flight DoPut (bidi). Protocol, in order:
//   1. Client sends one FlightData carrying the Arrow Schema + FlightDescriptor.PATH=[table]
//      and app_metadata = JSON.stringify({request_id:0}).
//   2. Client waits for one PutResult (the server's schema ACK).
//   3. Client sends N data FlightData messages, each with app_metadata
//      = JSON.stringify({request_id:N}) where N is monotonically increasing.
//   4. Server sends PutResult messages with app_metadata
//      = JSON.stringify({request_id:N, affected_rows:M}), possibly out of order.
//   5. Client calls end(); server completes the stream after draining responses.
//
// Auth + dbname travel in gRPC metadata (x-greptime-auth / x-greptime-db-name) — there is
// no proto RequestHeader on this path.

import type { ClientConfig } from '../config.js';
import { BulkError, ConfigError } from '../errors.js';
import type { FlightData, PutResult } from '../generated/arrow/flight/Flight_pb.js';
import { DoPutMethod } from '../transport/grpc-services.js';
import type { Channel } from '../transport/channel.js';
import { bidiStreamingCall, type BidiStreamingCall } from '../transport/promise-adapter.js';
import { buildFlightMetadata } from '../auth.js';
import { validateTableSchema, type TableSchema } from '../table/schema.js';
import { rowsToArrowTable } from './arrow-encoder.js';
import {
  buildBatchFlightData,
  buildSchemaFlightData,
  encodeTableForFlight,
  parseDoPutResponse,
  schemaToIpcMessage,
} from './flight-codec.js';
import { Semaphore } from './parallelism.js';
import { RequestTracker, type BulkWriteResponse } from './request-tracker.js';
import { BulkCompression } from './compression.js';

export interface BulkWriteOptions {
  readonly compression?: BulkCompression;
  readonly timeoutMs?: number;
  readonly parallelism?: number;
  readonly signal?: AbortSignal;
}

export type RowBatch =
  | { readonly kind: 'rows'; readonly rows: readonly (readonly unknown[])[] };

export interface BulkFinishSummary {
  readonly totalRequests: number;
  readonly totalAffectedRows: number;
}

type State = 'fresh' | 'ready' | 'halfClosed' | 'closed' | 'errored';

export class BulkStreamWriter {
  private readonly call: BidiStreamingCall<FlightData, PutResult>;
  private readonly tracker = new RequestTracker();
  private readonly semaphore: Semaphore;
  private readonly schema: TableSchema;
  private readonly schemaArrow: ReturnType<typeof schemaToIpcMessage>;
  private readonly timeoutMs?: number;
  private state: State = 'fresh';
  private totalAffected = 0;
  private totalRequests = 0;
  private handshakePromise: Promise<void> | undefined;
  private drainPromise: Promise<void> | undefined;

  private constructor(
    channel: Channel,
    cfg: ClientConfig,
    schema: TableSchema,
    opts: BulkWriteOptions | undefined,
  ) {
    validateTableSchema(schema);
    const compression = opts?.compression ?? BulkCompression.None;
    if (compression !== BulkCompression.None) {
      // v0.1 limitation: apache-arrow JS 18.x does not emit LZ4_FRAME / ZSTD body-compression
      // in IPC records. The server expects Arrow-level body compression (not gRPC transport
      // compression). A v0.2 follow-up will either upgrade to an Arrow JS release that
      // supports body compression or implement flatbuffer-level BodyCompression injection
      // using the `@mongodb-js/zstd` / `lz4-napi` optional deps.
      throw new ConfigError(
        `BulkCompression.${compression} is not yet supported in the TS ingester; ` +
          `use BulkCompression.None for v0.1 (see roadmap)`,
      );
    }
    this.schema = schema;
    const parallelism = opts?.parallelism ?? 8;
    this.semaphore = new Semaphore(parallelism);
    if (opts?.timeoutMs !== undefined) this.timeoutMs = opts.timeoutMs;

    // Pre-encode the Arrow schema once — it's reused for every batch's handshake.
    const arrowSchema = rowsToArrowTable(schema, []).schema;
    this.schemaArrow = schemaToIpcMessage(arrowSchema);

    this.call = bidiStreamingCall(channel.unwrap(), DoPutMethod, {
      metadata: buildFlightMetadata(cfg),
      deadlineMs: opts?.timeoutMs ?? cfg.timeoutMs,
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    });
    this.drainPromise = this.drainResponses();
  }

  public static async open(
    channel: Channel,
    cfg: ClientConfig,
    schema: TableSchema,
    opts: BulkWriteOptions | undefined,
  ): Promise<BulkStreamWriter> {
    const w = new BulkStreamWriter(channel, cfg, schema, opts);
    await w.handshake();
    return w;
  }

  private async handshake(): Promise<void> {
    if (this.handshakePromise !== undefined) return this.handshakePromise;
    this.handshakePromise = (async () => {
      const schemaFlight = buildSchemaFlightData(this.schema.tableName, this.schemaArrow);
      // request_id=0 is the schema ACK correlation id (server mirrors it back).
      const ackPromise = this.tracker.track(0, this.timeoutMs);
      this.tracker.resetIdCounter(1);
      try {
        await this.call.write(schemaFlight);
      } catch (err) {
        this.state = 'errored';
        this.tracker.rejectAll(err);
        throw err;
      }
      try {
        await ackPromise;
        this.state = 'ready';
      } catch (err) {
        this.state = 'errored';
        throw new BulkError(
          `schema handshake failed for table "${this.schema.tableName}" ` +
            `(ensure the table exists and schema matches)`,
          undefined,
          err,
        );
      }
    })();
    return this.handshakePromise;
  }

  /**
   * Send one batch of rows; awaits schema-ack and per-batch server ack. Honors parallelism:
   * at most `parallelism` in-flight requests wait concurrently.
   */
  public async writeRows(batch: RowBatch): Promise<BulkWriteResponse> {
    const id = await this.writeRowsAsync(batch);
    return this.waitForResponse(id);
  }

  /**
   * Send one batch and return its request id immediately. Use `waitForResponse(id)` later
   * to collect the ack. Still honors parallelism via the semaphore.
   */
  public async writeRowsAsync(batch: RowBatch): Promise<number> {
    if (this.state === 'fresh') await this.handshake();
    if (this.state !== 'ready') {
      throw new BulkError(`cannot write in state "${this.state}"`);
    }
    await this.semaphore.acquire();
    const id = this.tracker.alloc();
    const track = this.tracker.track(id, this.timeoutMs);
    // Release the semaphore on settle. Attaching both branches here keeps the
    // background release path from surfacing an "unhandled rejection" — the real
    // rejection is still observable via waitForResponse/writeRows.
    track.then(
      () => {
        this.semaphore.release();
      },
      () => {
        this.semaphore.release();
      },
    );

    const arrowTable = rowsToArrowTable(this.schema, batch.rows);
    const { batchMessages } = encodeTableForFlight(arrowTable);
    if (batchMessages.length === 0) {
      // Zero-row batch — synthesize an ack so callers aren't stranded.
      this.tracker.resolve(id, { requestId: id, affectedRows: 0 });
      return id;
    }
    if (batchMessages.length > 1) {
      // Arrow may split very large batches internally. Each sub-batch rides a distinct
      // request id; for simplicity v0.1 sends them all under the same id. If this becomes
      // observable (it shouldn't at typical batch sizes), we'll allocate one id per frame.
    }
    try {
      for (const msg of batchMessages) {
        const flight = buildBatchFlightData(msg, id);
        await this.call.write(flight);
      }
    } catch (err) {
      this.state = 'errored';
      this.tracker.reject(id, err);
      throw err;
    }
    this.totalRequests++;
    return id;
  }

  public waitForResponse(id: number, opts?: { timeoutMs?: number }): Promise<BulkWriteResponse> {
    const existing = this.tracker.getPromise(id);
    if (existing === undefined) {
      return Promise.reject(new BulkError(`no pending response for request_id ${id}`, id));
    }
    if (opts?.timeoutMs !== undefined) {
      const timeoutMs = opts.timeoutMs;
      return Promise.race([
        existing,
        new Promise<BulkWriteResponse>((_, rej) => {
          setTimeout(() => {
            rej(new BulkError(`waitForResponse ${id} timed out`, id));
          }, timeoutMs);
        }),
      ]);
    }
    return existing;
  }

  /** Half-close the stream, drain outstanding responses, return summary. */
  public async finish(): Promise<BulkFinishSummary> {
    if (this.state === 'closed') {
      return { totalRequests: this.totalRequests, totalAffectedRows: this.totalAffected };
    }
    if (this.state !== 'ready' && this.state !== 'fresh') {
      throw new BulkError(`cannot finish in state "${this.state}"`);
    }
    this.state = 'halfClosed';
    this.call.end();
    try {
      if (this.drainPromise !== undefined) await this.drainPromise;
    } finally {
      this.state = 'closed';
    }
    if (this.tracker.hasPending()) {
      const ids = this.tracker.pendingIds();
      const err = new BulkError(
        `stream ended with ${ids.length} pending requests: [${ids.join(', ')}]`,
      );
      this.tracker.rejectAll(err);
      throw err;
    }
    return { totalRequests: this.totalRequests, totalAffectedRows: this.totalAffected };
  }

  public cancel(reason?: unknown): void {
    if (this.state === 'closed' || this.state === 'errored') return;
    this.state = 'errored';
    this.call.cancel(reason);
    this.tracker.rejectAll(reason ?? new BulkError('bulk stream cancelled'));
  }

  private async drainResponses(): Promise<void> {
    try {
      for await (const pr of this.call.responses()) {
        try {
          const parsed = parseDoPutResponse(pr.appMetadata);
          const resp: BulkWriteResponse = {
            requestId: parsed.request_id,
            affectedRows: parsed.affected_rows,
          };
          this.totalAffected += resp.affectedRows;
          this.tracker.resolve(parsed.request_id, resp);
        } catch (err) {
          // Surface malformed responses as BulkError but keep draining — one bad frame
          // shouldn't doom the whole stream.
          this.tracker.rejectAll(
            new BulkError('malformed DoPut response app_metadata', undefined, err),
          );
          break;
        }
      }
    } catch (err) {
      this.state = 'errored';
      const wrapped =
        err instanceof BulkError
          ? err
          : new BulkError(
              `bulk stream errored: ${err instanceof Error ? err.message : String(err)}`,
              undefined,
              err,
            );
      this.tracker.rejectAll(wrapped);
    }
  }
}
