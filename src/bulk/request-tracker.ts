import { createDeferred, type Deferred } from '../internal/deferred.js';
import { BulkError, TimeoutError } from '../errors.js';

export interface BulkWriteResponse {
  readonly requestId: number;
  readonly affectedRows: number;
}

interface Entry {
  readonly deferred: Deferred<BulkWriteResponse>;
  timeoutHandle?: NodeJS.Timeout;
}

/**
 * Correlates bulk DoPut requests by their monotonically-increasing `request_id` with the
 * server's out-of-order PutResult responses.
 */
export class RequestTracker {
  private readonly pending = new Map<number, Entry>();
  private nextId = 1;

  public alloc(): number {
    // 2^53 ids is astronomical for a single stream (~2850 years at 100k rps), but
    // guard the ceiling anyway — wrapping to 1 would risk collision with still-
    // pending ids and force a lookup on every alloc. Fail loudly instead so the
    // caller can recreate the writer.
    if (this.nextId > Number.MAX_SAFE_INTEGER) {
      throw new BulkError('bulk request id space exhausted; recreate the writer');
    }
    const id = this.nextId++;
    return id;
  }

  /** Look up an already-tracked request's promise, or `undefined` if unknown. */
  public getPromise(id: number): Promise<BulkWriteResponse> | undefined {
    return this.pending.get(id)?.deferred.promise;
  }

  public track(id: number, timeoutMs?: number): Promise<BulkWriteResponse> {
    const deferred = createDeferred<BulkWriteResponse>();
    const entry: Entry = { deferred };
    if (timeoutMs !== undefined) {
      entry.timeoutHandle = setTimeout(() => {
        this.reject(id, new TimeoutError(`bulk request ${id} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    }
    this.pending.set(id, entry);
    return deferred.promise;
  }

  public resolve(id: number, resp: BulkWriteResponse): void {
    const entry = this.pending.get(id);
    if (entry === undefined) return;
    if (entry.timeoutHandle !== undefined) clearTimeout(entry.timeoutHandle);
    this.pending.delete(id);
    entry.deferred.resolve(resp);
  }

  public reject(id: number, err: unknown): void {
    const entry = this.pending.get(id);
    if (entry === undefined) return;
    if (entry.timeoutHandle !== undefined) clearTimeout(entry.timeoutHandle);
    this.pending.delete(id);
    entry.deferred.reject(err);
  }

  /** Fail every pending request. Used when the underlying stream errors out. */
  public rejectAll(err: unknown): void {
    for (const [id] of this.pending) {
      const entry = this.pending.get(id);
      if (entry?.timeoutHandle !== undefined) clearTimeout(entry.timeoutHandle);
      entry?.deferred.reject(err);
    }
    this.pending.clear();
  }

  public size(): number {
    return this.pending.size;
  }

  public hasPending(): boolean {
    return this.pending.size > 0;
  }

  public pendingIds(): number[] {
    return [...this.pending.keys()];
  }
}
