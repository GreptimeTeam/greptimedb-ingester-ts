import { ConfigError } from './errors.js';
import type { Logger } from './internal/logger.js';
import { VERSION } from './version.js';

/**
 * Authentication for the ingest client. Only `basic` is supported right now.
 */
export interface AuthConfig {
  readonly kind: 'basic';
  readonly username: string;
  readonly password: string;
}

export type TlsConfig =
  | { readonly kind: 'system' }
  | {
      readonly kind: 'pem';
      readonly ca?: string | Buffer;
      readonly cert?: string | Buffer;
      readonly key?: string | Buffer;
      readonly serverNameOverride?: string;
    }
  | {
      readonly kind: 'file';
      readonly caPath?: string;
      readonly certPath?: string;
      readonly keyPath?: string;
      readonly serverNameOverride?: string;
    };

/**
 * gRPC keepalive configuration
 */
export interface KeepAliveConfig {
  readonly timeMs: number;
  readonly timeoutMs: number;
}

/**
 * Retry policy for writing.
 */
export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly backoffMultiplier: number;
  readonly jitter: 'full' | 'none';
  readonly mode: 'aggressive' | 'conservative';
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 100,
  maxBackoffMs: 5_000,
  backoffMultiplier: 2,
  jitter: 'full',
  mode: 'aggressive',
};

export type GrpcCompression = 'none' | 'gzip' | 'deflate';

export interface ClientConfig {
  readonly endpoints: readonly string[];
  readonly database: string;
  readonly auth?: AuthConfig;
  readonly tls?: TlsConfig;
  readonly timeoutMs: number;
  readonly keepAlive: KeepAliveConfig;
  readonly maxReceiveMessageSize: number;
  readonly maxSendMessageSize: number;
  readonly grpcCompression: GrpcCompression;
  readonly userAgent: string;
  readonly logger?: Logger;
  readonly retry: RetryPolicy;
}

const DEFAULT_MAX_MESSAGE_SIZE = 128 * 1024 * 1024;

export class ConfigBuilder {
  private _endpoints: string[] = [];
  private _database = 'public';
  private _auth?: AuthConfig;
  private _tls?: TlsConfig;
  private _timeoutMs = 60_000;
  private _keepAlive: KeepAliveConfig = { timeMs: 30_000, timeoutMs: 10_000 };
  private _maxReceiveMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
  private _maxSendMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
  private _grpcCompression: GrpcCompression = 'none';
  private _userAgent = `greptime-ingester-ts/${VERSION}`;
  private _logger?: Logger;
  private _retry: RetryPolicy = DEFAULT_RETRY_POLICY;

  /** Create a configuration builder with a single endpoint. */
  public static create(endpoint: string): ConfigBuilder {
    return new ConfigBuilder().withEndpoints(endpoint);
  }

  /** Create a configuration builder with one or more endpoints. */
  public static createWithEndpoints(...endpoints: string[]): ConfigBuilder {
    return new ConfigBuilder().withEndpoints(...endpoints);
  }

  public withEndpoints(...endpoints: string[]): this {
    this._endpoints = [...this._endpoints, ...endpoints];
    return this;
  }

  public withDatabase(db: string): this {
    this._database = db;
    return this;
  }

  public withBasicAuth(username: string, password: string): this {
    this._auth = { kind: 'basic', username, password };
    return this;
  }

  public withTls(tls: TlsConfig): this {
    this._tls = tls;
    return this;
  }

  public withTimeout(ms: number): this {
    this._timeoutMs = ms;
    return this;
  }

  public withKeepAlive(timeMs: number, timeoutMs: number): this {
    this._keepAlive = { timeMs, timeoutMs };
    return this;
  }

  public withMaxMessageSize(receiveBytes: number, sendBytes: number = receiveBytes): this {
    this._maxReceiveMessageSize = receiveBytes;
    this._maxSendMessageSize = sendBytes;
    return this;
  }

  public withGrpcCompression(c: GrpcCompression): this {
    this._grpcCompression = c;
    return this;
  }

  public withUserAgent(ua: string): this {
    this._userAgent = ua;
    return this;
  }

  public withLogger(l: Logger): this {
    this._logger = l;
    return this;
  }

  public withRetry(retry: Partial<RetryPolicy>): this {
    this._retry = { ...this._retry, ...retry };
    return this;
  }

  public build(): ClientConfig {
    if (this._endpoints.length === 0) {
      throw new ConfigError('at least one endpoint is required');
    }
    for (const ep of this._endpoints) {
      if (!ep.includes(':')) {
        throw new ConfigError(`endpoint "${ep}" must be in host:port form`);
      }
    }
    if (this._database.length === 0) {
      throw new ConfigError('database name must not be empty');
    }
    if (this._timeoutMs <= 0) {
      throw new ConfigError('timeoutMs must be > 0');
    }
    const built: ClientConfig = {
      endpoints: Object.freeze([...this._endpoints]),
      database: this._database,
      timeoutMs: this._timeoutMs,
      keepAlive: this._keepAlive,
      maxReceiveMessageSize: this._maxReceiveMessageSize,
      maxSendMessageSize: this._maxSendMessageSize,
      grpcCompression: this._grpcCompression,
      userAgent: this._userAgent,
      retry: this._retry,
      ...(this._auth !== undefined && { auth: this._auth }),
      ...(this._tls !== undefined && { tls: this._tls }),
      ...(this._logger !== undefined && { logger: this._logger }),
    };
    return built;
  }
}
