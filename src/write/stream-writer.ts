// StreamWriter: client-streaming `HandleRequests`. Each `write(tables)` flushes one
// GreptimeRequest message containing RowInsertRequests. `finish()` half-closes and awaits
// the server's single aggregate `AffectedRows` response.

import { create } from '@bufbuild/protobuf';
import { status } from '@grpc/grpc-js';

import { buildHintsMetadata, buildRequestHeader } from '../auth.js';
import type { ClientConfig } from '../config.js';
import { ServerError, StateError, TransportError } from '../errors.js';
import {
  GreptimeRequestSchema,
  type GreptimeRequest,
  type GreptimeResponse,
} from '../generated/greptime/v1/database_pb.js';
import { HandleRequestsMethod } from '../transport/grpc-services.js';
import type { Channel } from '../transport/channel.js';
import type { EndpointOutcomeHook } from '../transport/endpoint-selector.js';
import { clientStreamingCall, type ClientStreamingCall } from '../transport/promise-adapter.js';
import type { Table } from '../table/table.js';
import { encodeTables } from './encode.js';
import type { AffectedRows } from './affected-rows.js';

export interface StreamOptions {
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
  readonly hints?: Record<string, string>;
}

type State = 'open' | 'halfClosed' | 'closed' | 'errored';

export class StreamWriter {
  public readonly endpoint: string;
  private readonly call: ClientStreamingCall<GreptimeRequest, GreptimeResponse>;
  private readonly cfg: ClientConfig;
  private readonly onSettle: EndpointOutcomeHook | undefined;
  private settled = false;
  private state: State = 'open';

  public constructor(
    channel: Channel,
    cfg: ClientConfig,
    opts: StreamOptions | undefined,
    onSettle?: EndpointOutcomeHook,
  ) {
    this.cfg = cfg;
    this.endpoint = channel.endpoint;
    this.onSettle = onSettle;
    this.call = clientStreamingCall(channel.unwrap(), HandleRequestsMethod, {
      metadata: buildHintsMetadata(opts?.hints),
      deadlineMs: opts?.timeoutMs ?? cfg.timeoutMs,
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    });
  }

  /**
   * Report this session's terminal outcome to the endpoint selector exactly once. A transport
   * error ejects the endpoint from a health-aware selector; success or a server business error
   * does not. Caller-initiated `cancel()` does not settle — it is not an endpoint verdict.
   */
  private settle(error?: unknown): void {
    if (this.settled) return;
    this.settled = true;
    this.onSettle?.(error);
  }

  /** Enqueue one or more Tables. Rejects if the stream is already half-closed or errored. */
  public async write(tables: Table | readonly Table[]): Promise<void> {
    if (this.state !== 'open') {
      throw new StateError(`cannot write to stream in state "${this.state}"`);
    }
    const list: readonly Table[] = Array.isArray(tables) ? tables : [tables as Table];
    const header = buildRequestHeader(this.cfg);
    const rowInserts = encodeTables(list);
    const req: GreptimeRequest = create(GreptimeRequestSchema, {
      header,
      request: { case: 'rowInserts', value: rowInserts },
    });
    try {
      await this.call.write(req);
    } catch (err) {
      this.state = 'errored';
      this.settle(err);
      throw err;
    }
  }

  /** Half-close the stream and await the server's final `AffectedRows`. */
  public async finish(): Promise<AffectedRows> {
    if (this.state === 'closed') {
      throw new StateError('stream already closed');
    }
    if (this.state !== 'open' && this.state !== 'halfClosed') {
      throw new StateError(`cannot finish stream in state "${this.state}"`);
    }
    this.state = 'halfClosed';
    let res: GreptimeResponse;
    try {
      res = await this.call.finish();
    } catch (err) {
      this.state = 'errored';
      this.settle(err);
      throw err;
    }
    this.state = 'closed';
    const statusCode = res.header?.status?.statusCode;
    if (statusCode !== undefined && statusCode !== 0) {
      const msg = res.header?.status?.errMsg ?? `server returned status ${statusCode}`;
      // Server business error: the endpoint answered, so it is healthy — settle as success.
      const err = new ServerError(msg, statusCode);
      this.settle(err);
      throw err;
    }
    if (res.response.case !== 'affectedRows') {
      const err = new TransportError('HandleRequests returned empty response', status.UNKNOWN);
      this.settle(err);
      throw err;
    }
    this.settle();
    return { value: res.response.value.value };
  }

  /** Cancel the underlying call. Safe to call multiple times. */
  public cancel(reason?: unknown): void {
    if (this.state === 'closed' || this.state === 'errored') return;
    this.state = 'errored';
    this.call.cancel(reason);
  }
}
