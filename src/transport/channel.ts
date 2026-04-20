// Thin wrapper around @grpc/grpc-js Client with keepalive, TLS, compression, and max-size
// options pre-configured from ClientConfig.

import fs from 'node:fs';
import {
  Client as GrpcClient,
  ChannelCredentials,
  compressionAlgorithms,
  type ChannelOptions,
} from '@grpc/grpc-js';

import type { ClientConfig, GrpcCompression, TlsConfig } from '../config.js';
import { ConfigError, StateError } from '../errors.js';

function compressionToGrpc(c: GrpcCompression): number {
  switch (c) {
    case 'none':
      return compressionAlgorithms.identity;
    case 'gzip':
      return compressionAlgorithms.gzip;
    case 'deflate':
      return compressionAlgorithms.deflate;
  }
}

function readIfPath(v: string | Buffer | undefined): Buffer | undefined {
  if (v === undefined) return undefined;
  return typeof v === 'string' ? Buffer.from(v, 'utf8') : v;
}

function readFileOrUndefined(p: string | undefined): Buffer | undefined {
  if (p === undefined) return undefined;
  try {
    return fs.readFileSync(p);
  } catch (err) {
    throw new ConfigError(`failed to read TLS file ${p}`, err);
  }
}

function buildCredentials(tls: TlsConfig | undefined): ChannelCredentials {
  if (tls === undefined) return ChannelCredentials.createInsecure();
  switch (tls.kind) {
    case 'system':
      return ChannelCredentials.createSsl();
    case 'pem':
      return ChannelCredentials.createSsl(
        readIfPath(tls.ca) ?? null,
        readIfPath(tls.key) ?? null,
        readIfPath(tls.cert) ?? null,
      );
    case 'file':
      return ChannelCredentials.createSsl(
        readFileOrUndefined(tls.caPath) ?? null,
        readFileOrUndefined(tls.keyPath) ?? null,
        readFileOrUndefined(tls.certPath) ?? null,
      );
  }
}

function buildChannelOptions(cfg: ClientConfig): ChannelOptions {
  const opts: ChannelOptions = {
    'grpc.keepalive_time_ms': cfg.keepAlive.timeMs,
    'grpc.keepalive_timeout_ms': cfg.keepAlive.timeoutMs,
    'grpc.keepalive_permit_without_calls': 1,
    'grpc.max_receive_message_length': cfg.maxReceiveMessageSize,
    'grpc.max_send_message_length': cfg.maxSendMessageSize,
    'grpc.default_compression_algorithm': compressionToGrpc(cfg.grpcCompression),
    'grpc.default_compression_level': 2,
    'grpc.primary_user_agent': cfg.userAgent,
  };
  const serverNameOverride =
    cfg.tls && cfg.tls.kind !== 'system' ? cfg.tls.serverNameOverride : undefined;
  if (serverNameOverride !== undefined) {
    opts['grpc.ssl_target_name_override'] = serverNameOverride;
  }
  return opts;
}

/** A connected @grpc/grpc-js Client keyed to a single endpoint. */
export class Channel {
  public readonly endpoint: string;
  private readonly client: GrpcClient;

  public constructor(endpoint: string, cfg: ClientConfig) {
    this.endpoint = endpoint;
    this.client = new GrpcClient(endpoint, buildCredentials(cfg.tls), buildChannelOptions(cfg));
  }

  public unwrap(): GrpcClient {
    return this.client;
  }

  public close(): void {
    this.client.close();
  }
}

/** Connection pool keyed by "host:port". Terminal after `close()`. */
export class ChannelPool {
  private readonly channels = new Map<string, Channel>();
  private _closed = false;

  public constructor(private readonly cfg: ClientConfig) {}

  public get(endpoint: string): Channel {
    if (this._closed) {
      throw new StateError('channel pool is closed');
    }
    let ch = this.channels.get(endpoint);
    if (!ch) {
      ch = new Channel(endpoint, this.cfg);
      this.channels.set(endpoint, ch);
    }
    return ch;
  }

  /** Idempotent. After the first call, `get()` throws StateError. */
  public close(): void {
    if (this._closed) return;
    this._closed = true;
    for (const ch of this.channels.values()) ch.close();
    this.channels.clear();
  }

  public isClosed(): boolean {
    return this._closed;
  }
}

/**
 * Random-peer selector. Matches Rust `src/load_balance.rs` — no health probing, gRPC's
 * own state machine handles reconnection and failover.
 */
export function pickRandom<T>(items: readonly T[]): T {
  if (items.length === 0) {
    throw new ConfigError('no endpoints available');
  }
  const idx = Math.floor(Math.random() * items.length);
  return items[idx] as T;
}
