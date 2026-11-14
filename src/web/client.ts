import { create } from '@bufbuild/protobuf';
import { Code, ConnectError, createClient } from '@connectrpc/connect';
import { createGrpcWebTransport } from '@connectrpc/connect-web';
import { DEFAULT_RETRY_POLICY, type AuthConfig, type RetryPolicy } from '../config-shared.js';
import { objectsToTables } from '../decorators/object-mapper.js';
import { AbortedError, ConfigError, StateError, TimeoutError, TransportError } from '../errors.js';
import { GreptimeDatabase, GreptimeRequestSchema } from '../generated/greptime/v1/database_pb.js';
import type { Logger } from '../internal/logger.js';
import type { Table } from '../table/table.js';
import { withRetry } from '../transport/retry.js';
import type { AffectedRows } from '../write/affected-rows.js';
import { encodeTables } from '../write/encode.js';
import { buildRequestHeader, encodeHints, type WriteOptions } from '../write/request.js';
import { extractAffected } from '../write/response.js';

export interface ClientConfig {
  /** HTTP(S) URL of the gRPC listener, including an optional reverse-proxy path prefix. */
  readonly endpoint: string;
  readonly database?: string;
  readonly auth?: AuthConfig;
  readonly timeoutMs?: number;
  readonly retry?: Partial<RetryPolicy>;
  readonly logger?: Logger;
}

function validateTimeout(ms: number): void {
  if (!Number.isFinite(ms) || ms <= 0) throw new ConfigError('timeoutMs must be finite and > 0');
}

/** Unary ingestion over gRPC-Web in browsers with Fetch and AbortSignal.any support. */
export class Client {
  private readonly rpc;
  private readonly database: string;
  private readonly timeoutMs: number;
  private readonly retry: RetryPolicy;
  private readonly shutdown = new AbortController();

  public constructor(private readonly cfg: ClientConfig) {
    let url: URL;
    try {
      url = new URL(cfg.endpoint);
    } catch (err) {
      throw new ConfigError('endpoint must be an absolute HTTP(S) URL', err);
    }
    if (
      !['http:', 'https:'].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    ) {
      throw new ConfigError(
        'endpoint must be an HTTP(S) URL without credentials, query, or fragment',
      );
    }
    this.database = cfg.database ?? 'public';
    if (!this.database) throw new ConfigError('database name must not be empty');
    this.timeoutMs = cfg.timeoutMs ?? 60_000;
    validateTimeout(this.timeoutMs);
    this.retry = { ...DEFAULT_RETRY_POLICY, ...cfg.retry };
    if (!Number.isInteger(this.retry.maxAttempts) || this.retry.maxAttempts < 1) {
      throw new ConfigError('maxAttempts must be a positive integer');
    }
    this.rpc = createClient(GreptimeDatabase, createGrpcWebTransport({ baseUrl: url.href }));
  }

  private ensureOpen(): void {
    if (this.isClosed()) throw new StateError('client is closed');
  }

  public async write(tables: Table | readonly Table[], opts?: WriteOptions): Promise<AffectedRows> {
    this.ensureOpen();
    const timeoutMs = opts?.timeoutMs ?? this.timeoutMs;
    validateTimeout(timeoutMs);
    const signal =
      opts?.signal === undefined
        ? this.shutdown.signal
        : AbortSignal.any([this.shutdown.signal, opts.signal]);
    if (signal.aborted) throw new AbortedError('aborted before write');
    const hints = encodeHints(opts?.hints);
    const headers = hints === undefined ? undefined : { 'x-greptime-hints': hints };
    const request = create(GreptimeRequestSchema, {
      header: buildRequestHeader({
        database: this.database,
        ...(this.cfg.auth !== undefined && { auth: this.cfg.auth }),
      }),
      request: {
        case: 'rowInserts',
        value: encodeTables(Array.isArray(tables) ? tables : [tables as Table]),
      },
    });
    return withRetry(
      async () => {
        this.ensureOpen();
        let response;
        try {
          response = await this.rpc.handle(request, {
            signal,
            timeoutMs,
            ...(headers !== undefined && { headers }),
          });
        } catch (err) {
          if (err instanceof ConnectError) {
            if (err.code === Code.Canceled) throw new AbortedError(err.rawMessage, err);
            if (err.code === Code.DeadlineExceeded) throw new TimeoutError(err.rawMessage, err);
            throw new TransportError(err.rawMessage, err.code, err);
          }
          throw err;
        }
        return extractAffected(response);
      },
      this.retry,
      signal,
      this.cfg.logger,
    );
  }

  public async writeObject(
    instances: readonly object[],
    opts?: WriteOptions,
  ): Promise<AffectedRows> {
    this.ensureOpen();
    return this.write(objectsToTables(instances), opts);
  }

  /** Abort active writes and retry waits. Subsequent writes fail with StateError. */
  public close(): Promise<void> {
    this.shutdown.abort();
    return Promise.resolve();
  }

  public isClosed(): boolean {
    return this.shutdown.signal.aborted;
  }
}
