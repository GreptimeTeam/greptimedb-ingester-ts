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
import { BulkError } from '../errors.js';
import type { FlightData, PutResult } from '../generated/arrow/flight/Flight_pb.js';
import { DoPutMethod } from '../transport/grpc-services.js';
import type { Channel } from '../transport/channel.js';
import { bidiStreamingCall, type BidiStreamingCall } from '../transport/promise-adapter.js';
import { buildFlightMetadata } from '../auth.js';
import { validateTableSchema, type TableSchema } from '../table/schema.js';
import {
  precomputeArrowSchema,
  rowsToArrowTable,
  type PrecomputedArrowSchema,
} from './arrow-encoder.js';
import {
  buildBatchFlightData,
  buildSchemaFlightData,
  encodeTableForFlight,
  parseDoPutResponse,
  schemaToIpcMessage,
  type EncodeCompressionContext,
} from './flight-codec.js';
import { Semaphore } from './parallelism.js';
import { RequestTracker, type BulkWriteResponse } from './request-tracker.js';
import { BulkCompression } from './compression.js';
import { resolveCompressor } from './ipc-compression.js';
import { createDeferred, type Deferred } from '../internal/deferred.js';
import { NOOP_LOGGER, type Logger } from '../internal/logger.js';

export interface BulkWriteOptions {
  readonly compression?: BulkCompression;
  readonly timeoutMs?: number;
  readonly parallelism?: number;
  readonly signal?: AbortSignal;
  /**
   * Cap on the number of settled-but-unclaimed responses held in memory for
   * `writeRowsAsync` callers that never invoke `waitForResponse(id)`. When the
   * map reaches this size, the oldest entry is evicted (insertion order) and a
   * single warn is logged per writer. Callers hitting the cap have a leaky
   * drain pattern — missed acks do not imply a server-side write failure, but
   * the id becomes unclaimable. Default: 10_000.
   */
  readonly maxUnclaimedResponses?: number;
}

const DEFAULT_MAX_UNCLAIMED_RESPONSES = 10_000;

export interface RowBatch {
  readonly kind: 'rows';
  readonly rows: readonly (readonly unknown[])[];
}

export interface BulkFinishSummary {
  readonly totalRequests: number;
  readonly totalAffectedRows: number;
}

type State = 'fresh' | 'ready' | 'halfClosed' | 'closed' | 'errored';

// One write batch may serialize into multiple Arrow IPC frames (Arrow can split very large
// RecordBatches internally). Each frame gets its own request_id so the server's per-frame
// PutResult can be correlated. The user-facing API still returns one id and one promise per
// `writeRowsAsync`; we aggregate sub-frame acks here.
interface FrameGroup {
  readonly userId: number;
  readonly subIds: readonly number[];
  remaining: number;
  totalAffected: number;
  readonly deferred: Deferred<BulkWriteResponse>;
}

type SettledGroup =
  | { readonly ok: true; readonly value: BulkWriteResponse }
  | { readonly ok: false; readonly error: unknown };

export class BulkStreamWriter {
  private readonly call: BidiStreamingCall<FlightData, PutResult>;
  private readonly tracker = new RequestTracker();
  private readonly semaphore: Semaphore;
  private readonly schema: TableSchema;
  private readonly schemaArrow: ReturnType<typeof schemaToIpcMessage>;
  // Hoisted Arrow schema artifacts: built once at construction, reused for every batch.
  // Without this each `writeRowsAsync` re-allocates the same Schema/Field/type instances.
  private readonly precomputedArrow: PrecomputedArrowSchema;
  private readonly timeoutMs?: number;
  private readonly logger: Logger;
  private readonly compressionCodec: BulkCompression;
  private compressionCtx: EncodeCompressionContext | undefined;
  private readonly groups = new Map<number, FrameGroup>();
  private readonly completed = new Map<number, SettledGroup>();
  // Every live writeRowsAsync invocation's top-level promise. Registered
  // synchronously at call time (before any await), so `finish()` can always
  // see in-flight work even when the caller fire-and-forgets: `void
  // bulk.writeRowsAsync(...)` then immediately `await bulk.finish()`.
  private readonly inFlight = new Set<Promise<number>>();
  private readonly maxUnclaimedResponses: number;
  private completedEvictionWarned = false;
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
    this.compressionCodec = opts?.compression ?? BulkCompression.None;
    this.schema = schema;
    this.logger = cfg.logger ?? NOOP_LOGGER;
    const parallelism = opts?.parallelism ?? 8;
    this.semaphore = new Semaphore(parallelism);
    const maxUnclaimed = opts?.maxUnclaimedResponses ?? DEFAULT_MAX_UNCLAIMED_RESPONSES;
    // Reject 0 / negative / NaN at construction: a cap of 0 turns recordCompleted
    // into "evict-then-insert on every call", and NaN breaks the size comparison
    // entirely. Integer >= 1 is the only meaningful range.
    if (!Number.isInteger(maxUnclaimed) || maxUnclaimed < 1) {
      throw new BulkError(
        `maxUnclaimedResponses must be a positive integer, got ${String(maxUnclaimed)}`,
      );
    }
    this.maxUnclaimedResponses = maxUnclaimed;
    if (opts?.timeoutMs !== undefined) this.timeoutMs = opts.timeoutMs;

    // Pre-compute Arrow schema artifacts (Field[], Type[], Schema) once. Reused both
    // for the schema handshake frame and for every subsequent `writeRowsAsync` batch.
    this.precomputedArrow = precomputeArrowSchema(schema);
    this.schemaArrow = schemaToIpcMessage(this.precomputedArrow.arrowSchema);

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
    // Resolve the native compressor BEFORE opening the bidi gRPC stream. Loading
    // `lz4-napi`/`@mongodb-js/zstd` takes ~1s on first use; if we opened the stream
    // first, the server would see an idle DoPut bidi that hasn't sent a schema yet
    // and — on GreptimeDB — can stall the first schema-ack. Resolving up front also
    // surfaces a missing optional dep as ConfigError before any network work happens.
    const codec = opts?.compression ?? BulkCompression.None;
    const resolved = await resolveCompressor(codec);
    const w = new BulkStreamWriter(channel, cfg, schema, opts);
    if (resolved !== null) {
      w.compressionCtx = {
        codec: w.compressionCodec,
        fbCodec: resolved.fbCodec,
        compressor: resolved.compressor,
      };
    }
    try {
      await w.handshake();
    } catch (err) {
      // handshake() marks state='errored' and rejects tracked promises, but the
      // underlying bidi gRPC stream is still live. The caller never receives `w`
      // (we rethrow), so nobody can call `cancel()` on it — tear it down here to
      // avoid leaking the call and its drain loop.
      w.call.cancel(err);
      throw err;
    }
    return w;
  }

  private async handshake(): Promise<void> {
    if (this.handshakePromise !== undefined) return this.handshakePromise;
    this.handshakePromise = (async () => {
      const schemaFlight = buildSchemaFlightData(this.schema.tableName, this.schemaArrow);
      // request_id=0 is the schema ACK correlation id (server mirrors it back). The
      // tracker's `alloc()` counter already starts at 1 so user batches get distinct ids.
      const ackPromise = this.tracker.track(0, this.timeoutMs);
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
        this.logger.log('error', 'bulk schema handshake failed', {
          table: this.schema.tableName,
          error: err instanceof Error ? err.message : String(err),
        });
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
   *
   * Contract: every id returned here SHOULD eventually be consumed by `waitForResponse(id)`;
   * settled-but-unclaimed acks are capped per `maxUnclaimedResponses` (default 10_000) and
   * then evicted oldest-first with a single warn log. For write-and-wait semantics, use
   * `writeRows()`.
   */
  public writeRowsAsync(batch: RowBatch): Promise<number> {
    // Synchronous wrapper: register the invocation's promise in `inFlight` BEFORE
    // returning so a caller that fire-and-forgets (`void bulk.writeRowsAsync(...)`)
    // followed by `await bulk.finish()` still has its work awaited. If we registered
    // inside the async body, the registration would happen AFTER the first internal
    // await — by then finish() may have already called call.end() and the frame
    // write would fail with "write after end".
    const p = this._writeRowsAsync(batch);
    this.inFlight.add(p);
    // Swallow rejection locally on the tracker-cleanup branch so the set is drained
    // even on failure; callers still observe the original rejection via `p`.
    p.then(
      () => this.inFlight.delete(p),
      () => this.inFlight.delete(p),
    );
    return p;
  }

  private async _writeRowsAsync(batch: RowBatch): Promise<number> {
    if (this.state === 'fresh') await this.handshake();
    if (this.state !== 'ready') {
      throw new BulkError(`cannot write in state "${this.state}"`);
    }
    // Acquire one semaphore slot per user-batch (regardless of sub-frame count) — limiting
    // by user-batches matches the documented `parallelism` semantics.
    await this.semaphore.acquire();

    // Encode first so we know the sub-frame count before allocating ids.
    let batchMessages: Awaited<ReturnType<typeof encodeTableForFlight>>['batchMessages'];
    try {
      const arrowTable = rowsToArrowTable(this.schema, batch.rows, this.precomputedArrow);
      ({ batchMessages } = await encodeTableForFlight(arrowTable, this.compressionCtx));
    } catch (err) {
      this.semaphore.release();
      throw err;
    }

    if (batchMessages.length === 0) {
      // Zero-row batch — record a completed synthetic result so waitForResponse(id)
      // resolves cleanly without pinning a live group forever.
      const id = this.tracker.alloc();
      this.recordCompleted(id, { ok: true, value: { requestId: id, affectedRows: 0 } });
      this.semaphore.release();
      return id;
    }

    // batchMessages.length >= 1 here (zero-row short-circuit handled above), so the
    // first allocation is the user-visible id and always defined.
    const userId = this.tracker.alloc();
    const subIds: number[] = [userId];
    for (let i = 1; i < batchMessages.length; i++) subIds.push(this.tracker.alloc());
    const subPromises = subIds.map((id) => this.tracker.track(id, this.timeoutMs));
    const group: FrameGroup = {
      userId,
      subIds,
      remaining: subIds.length,
      totalAffected: 0,
      deferred: createDeferred<BulkWriteResponse>(),
    };
    this.groups.set(userId, group);

    // Wire sub-acks → group aggregate. Each sub-promise rejection fails the group fast
    // (subsequent sub-promises still settle but are ignored).
    for (const p of subPromises) {
      p.then(
        (r) => {
          group.totalAffected += r.affectedRows;
          group.remaining--;
          if (group.remaining === 0) {
            group.deferred.resolve({ requestId: userId, affectedRows: group.totalAffected });
          }
        },
        (err: unknown) => {
          group.remaining = -1; // mark failed; later sub-acks no-op via deferred idempotency
          group.deferred.reject(err);
        },
      );
    }

    // Single semaphore release once the whole group settles. Move settled results out of
    // the live-group map so a long-running writer does not accumulate completed groups.
    group.deferred.promise.then(
      (value) => {
        this.groups.delete(userId);
        this.recordCompleted(userId, { ok: true, value });
        this.semaphore.release();
      },
      (err: unknown) => {
        this.groups.delete(userId);
        this.recordCompleted(userId, { ok: false, error: err });
        this.semaphore.release();
      },
    );

    try {
      for (let i = 0; i < batchMessages.length; i++) {
        const msg = batchMessages[i];
        const id = subIds[i];
        if (msg === undefined || id === undefined) {
          throw new BulkError('internal error: frame/request_id count mismatch', userId);
        }
        await this.call.write(buildBatchFlightData(msg, id));
      }
    } catch (err) {
      this.state = 'errored';
      // Reject any still-pending sub-ids; the aggregate forwards the rejection.
      for (const id of subIds) this.tracker.reject(id, err);
      throw err;
    }
    this.totalRequests++;
    return userId;
  }

  private recordCompleted(id: number, settled: SettledGroup): void {
    // Cap the map so a caller that never invokes waitForResponse() can't grow
    // it unboundedly. Map preserves insertion order; the first key is the
    // oldest. Evict it before inserting so size stays <= maxUnclaimedResponses.
    if (this.completed.size >= this.maxUnclaimedResponses) {
      const oldest = this.completed.keys().next();
      if (!oldest.done) this.completed.delete(oldest.value);
      if (!this.completedEvictionWarned) {
        this.completedEvictionWarned = true;
        this.logger.log(
          'warn',
          `bulk completed-response cache full (${this.maxUnclaimedResponses}); ` +
            'evicting oldest entries — unclaimed ids will report "no pending response"',
          { table: this.schema.tableName, cap: this.maxUnclaimedResponses },
        );
      }
    }
    this.completed.set(id, settled);
  }

  public waitForResponse(id: number, opts?: { timeoutMs?: number }): Promise<BulkWriteResponse> {
    const settled = this.completed.get(id);
    let existing: Promise<BulkWriteResponse> | undefined;
    if (settled !== undefined) {
      this.completed.delete(id);
      if (settled.ok) {
        existing = Promise.resolve(settled.value);
      } else {
        const errVal = settled.error;
        existing = Promise.reject(
          errVal instanceof Error
            ? errVal
            : new BulkError(
                typeof errVal === 'string' ? errVal : 'bulk request failed',
                id,
                errVal,
              ),
        );
      }
    } else {
      const group = this.groups.get(id);
      existing = group?.deferred.promise ?? this.tracker.getPromise(id);
    }
    if (existing === undefined) {
      return Promise.reject(new BulkError(`no pending response for request_id ${id}`, id));
    }
    if (opts?.timeoutMs === undefined) return existing;
    const timeoutMs = opts.timeoutMs;
    let timer: NodeJS.Timeout | undefined;
    const timeoutPromise = new Promise<BulkWriteResponse>((_, rej) => {
      timer = setTimeout(() => {
        rej(new BulkError(`waitForResponse ${id} timed out`, id));
      }, timeoutMs);
    });
    return Promise.race([existing, timeoutPromise]).finally(() => {
      if (timer !== undefined) clearTimeout(timer);
    });
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
    // Wait for every in-flight writeRowsAsync to finish writing its frames
    // before half-closing the wire. Callers who fire-and-forget
    // `void bulk.writeRowsAsync(...)` and then hit finish() would otherwise
    // race: call.end() closes the write side mid-frame and the still-running
    // _writeRowsAsync trips ERR_STREAM_WRITE_AFTER_END. allSettled — not
    // Promise.all — because one in-flight failure must not short-circuit
    // draining the rest, but we do surface rejections below.
    let fireAndForgetErrors: unknown[] = [];
    if (this.inFlight.size > 0) {
      const results = await Promise.allSettled([...this.inFlight]);
      // Fire-and-forget reject surfacing: a caller who `void`-ed the promise
      // has no other channel to learn about the failure. Without this step
      // finish() would return a success summary that silently dropped one or
      // more batches (encode error, mid-stream call.write failure, etc.) —
      // turning a hard failure into silent partial ingestion.
      // Promises the caller already awaited and observed have already been
      // removed from `inFlight` via the `.then(delete, delete)` cleanup, so
      // they don't double-report here.
      // `PromiseRejectedResult.reason` is typed `any` in lib.d.ts — narrow to
      // unknown explicitly so downstream `cause` plumbing stays type-safe.
      fireAndForgetErrors = results
        .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
        .map((r): unknown => r.reason);
    }
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
    if (fireAndForgetErrors.length > 0) {
      // Preserve the first underlying error as `cause` so it survives util.inspect;
      // the message enumerates the count so the caller knows whether to look deeper.
      const first = fireAndForgetErrors[0];
      const msg =
        fireAndForgetErrors.length === 1
          ? `1 fire-and-forget writeRowsAsync call rejected and was never awaited by the caller`
          : `${fireAndForgetErrors.length} fire-and-forget writeRowsAsync calls rejected and were never awaited by the caller`;
      throw new BulkError(msg, undefined, first);
    }
    // Fire-and-forget detector: acks that completed but were never claimed via
    // `waitForResponse(id)`. Not an error — the server-side write succeeded — but
    // signals the caller's drain pattern is leaky and the `completed` map grew
    // longer than necessary. Logging at `info` keeps this diagnosable without
    // false-alarming users of the common `writeRows()` (write-and-wait) API.
    if (this.completed.size > 0) {
      this.logger.log(
        'info',
        `bulk writer finished with ${this.completed.size} unclaimed ack(s); ` +
          'call waitForResponse(id) on every writeRowsAsync() to drain',
        { table: this.schema.tableName, unclaimed: this.completed.size },
      );
    }
    return { totalRequests: this.totalRequests, totalAffectedRows: this.totalAffected };
  }

  public cancel(reason?: unknown): void {
    if (this.state === 'closed' || this.state === 'errored') return;
    this.state = 'errored';
    this.call.cancel(reason);
    // Reject all tracked sub-ids. Each group's `.then()` aggregate handler will
    // observe the rejection asynchronously (next microtask) and move the group
    // into `completed` via `recordCompleted`. We intentionally do NOT clear
    // `groups` or `completed` here — doing so would create a race window between
    // `cancel()` returning and those microtasks settling, during which a caller's
    // `waitForResponse(userId)` would see neither map and fail with "no pending
    // response" instead of the real cancellation error.
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
          // totalAffected counts every sub-frame ack: server sends one PutResult per
          // FlightData frame, and we sum them across frames + groups.
          this.totalAffected += resp.affectedRows;
          this.tracker.resolve(parsed.request_id, resp);
        } catch (err) {
          // Surface malformed responses as BulkError but keep draining — one bad frame
          // shouldn't doom the whole stream.
          this.logger.log('error', 'bulk drain received malformed response', {
            table: this.schema.tableName,
            error: err instanceof Error ? err.message : String(err),
          });
          this.tracker.rejectAll(
            new BulkError('malformed DoPut response app_metadata', undefined, err),
          );
          break;
        }
      }
    } catch (err) {
      this.state = 'errored';
      this.logger.log('error', 'bulk drain loop errored', {
        table: this.schema.tableName,
        error: err instanceof Error ? err.message : String(err),
      });
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
