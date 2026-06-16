// Error class hierarchy. Classes (not a discriminated union) so users can `instanceof`
// and also read a `.kind` string tag for switch/case. Every error carries an optional
// `cause` that preserves the underlying error for debugging.

export abstract class IngesterError extends Error {
  public abstract readonly kind: string;

  protected constructor(message: string, cause?: unknown) {
    // ES2022 Error(cause) — lets util.inspect print the cause chain automatically.
    super(message, cause !== undefined ? { cause } : undefined);
    this.name = new.target.name;
  }
}

/** Invalid user-supplied configuration (TLS paths, endpoints, etc.). NOT retriable. */
export class ConfigError extends IngesterError {
  public readonly kind = 'config' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/** Schema validation failure (duplicate columns, wrong row length, etc.). NOT retriable. */
export class SchemaError extends IngesterError {
  public readonly kind = 'schema' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/** Value conversion failure (bigint overflow, wrong JS type for column, etc.). NOT retriable. */
export class ValueError extends IngesterError {
  public readonly kind = 'value' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/**
 * Invalid call in the current object state — e.g., using a closed `Client`,
 * writing to a finished `StreamWriter`, or sending frames after cancel.
 * NOT retriable.
 */
export class StateError extends IngesterError {
  public readonly kind = 'state' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/** gRPC transport-level error. Carries the numeric grpc status code. */
export class TransportError extends IngesterError {
  public readonly kind = 'transport' as const;
  public readonly grpcCode: number;
  public constructor(message: string, grpcCode: number, cause?: unknown) {
    super(message, cause);
    this.grpcCode = grpcCode;
  }
}

/** Business-level error from the GreptimeDB server. */
export class ServerError extends IngesterError {
  public readonly kind = 'server' as const;
  public readonly statusCode: number;
  public constructor(message: string, statusCode: number, cause?: unknown) {
    super(message, cause);
    this.statusCode = statusCode;
  }
}

/** Timeout waiting for a response (client-side deadline). */
export class TimeoutError extends IngesterError {
  public readonly kind = 'timeout' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/** Operation was aborted via AbortSignal. */
export class AbortedError extends IngesterError {
  public readonly kind = 'aborted' as const;
  public constructor(message: string, cause?: unknown) {
    super(message, cause);
  }
}

/** Bulk Arrow Flight path error. Optionally tagged with the DoPut request id. */
export class BulkError extends IngesterError {
  public readonly kind = 'bulk' as const;
  public readonly requestId?: number;
  public constructor(message: string, requestId?: number, cause?: unknown) {
    super(message, cause);
    if (requestId !== undefined) this.requestId = requestId;
  }
}

/**
 * GreptimeDB business status codes, mirrored from
 * `src/common/error/src/status_code.rs`. They arrive in the gRPC response header
 * (`header.status.status_code`) and surface as {@link ServerError.statusCode}. Exposed so
 * callers can match on `serverError.statusCode` without hard-coding magic numbers.
 */
export const GreptimeStatusCode = {
  Success: 0,
  Unknown: 1000,
  Unsupported: 1001,
  Unexpected: 1002,
  Internal: 1003,
  InvalidArguments: 1004,
  Cancelled: 1005,
  IllegalState: 1006,
  External: 1007,
  DeadlineExceeded: 1008,
  Suspended: 1009,
  InvalidSyntax: 2000,
  PlanQuery: 3000,
  EngineExecuteQuery: 3001,
  TableAlreadyExists: 4000,
  TableNotFound: 4001,
  TableColumnNotFound: 4002,
  TableColumnExists: 4003,
  DatabaseNotFound: 4004,
  RegionNotFound: 4005,
  RegionAlreadyExists: 4006,
  RegionReadonly: 4007,
  RegionNotReady: 4008,
  RegionBusy: 4009,
  TableUnavailable: 4010,
  DatabaseAlreadyExists: 4011,
  StorageUnavailable: 5000,
  RequestOutdated: 5001,
  RuntimeResourcesExhausted: 6000,
  RateLimited: 6001,
  UserNotFound: 7000,
  UnsupportedPasswordType: 7001,
  UserPasswordMismatch: 7002,
  AuthHeaderNotFound: 7003,
  InvalidAuthHeader: 7004,
  AccessDenied: 7005,
  PermissionDenied: 7006,
  FlowAlreadyExists: 8000,
  FlowNotFound: 8001,
  TriggerAlreadyExists: 9000,
  TriggerNotFound: 9001,
} as const;

export type GreptimeStatusCode = (typeof GreptimeStatusCode)[keyof typeof GreptimeStatusCode];

/**
 * Server status codes whose errors are transient and worth retrying. Mirrors
 * `StatusCode::is_retryable` in GreptimeDB's `status_code.rs`, minus `Internal` (1003):
 * a generic internal error is too often a real bug to retry blindly.
 */
const RETRYABLE_GREPTIME_STATUS_CODES = new Set<number>([
  GreptimeStatusCode.RegionNotReady,
  GreptimeStatusCode.RegionBusy,
  GreptimeStatusCode.TableUnavailable,
  GreptimeStatusCode.StorageUnavailable,
  GreptimeStatusCode.RuntimeResourcesExhausted,
]);

/** True if a server-side status code denotes a transient, retriable condition. */
export function isRetryableStatusCode(code: number): boolean {
  return RETRYABLE_GREPTIME_STATUS_CODES.has(code);
}

/**
 * Classify whether an error is worth retrying.
 *
 * - `ServerError` is authoritative in both modes: only the transient GreptimeDB status
 *   codes ({@link isRetryableStatusCode}) retry; business errors (InvalidArguments,
 *   TableNotFound, auth failures, ...) never do.
 * - `TimeoutError` (a client-side `DEADLINE_EXCEEDED`) is NOT retriable in either mode:
 *   `timeoutMs` is the caller's hard latency budget for the write, and each attempt resets
 *   that deadline (see `unaryCall`), so retrying would silently multiply the budget by
 *   `maxAttempts` — and usually time out again. A timeout reflects the caller's clock, not
 *   a transient server/network condition.
 * - `aggressive` (default): retries any of the SDK's own errors (`IngesterError` subclasses,
 *   chiefly `TransportError`) except the local config/schema/value/state errors and client
 *   timeouts listed above. Foreign throws (plain `Error`, non-`Error` values) are never retried.
 * - `conservative`: retry only transient transport conditions
 *   (UNAVAILABLE / RESOURCE_EXHAUSTED / ABORTED / UNKNOWN).
 */
export type RetryMode = 'aggressive' | 'conservative';

// gRPC status codes, from @grpc/grpc-js status.ts. DEADLINE_EXCEEDED (4) is intentionally
// absent: promise-adapter maps it to TimeoutError, and a client-side timeout is never retried
// (see the early return in isRetriable).
const CONSERVATIVE_RETRIABLE_GRPC_CODES = new Set<number>([
  2, // UNKNOWN
  8, // RESOURCE_EXHAUSTED
  10, // ABORTED
  14, // UNAVAILABLE
]);

export function isRetriable(err: unknown, mode: RetryMode = 'aggressive'): boolean {
  // AbortedError and TimeoutError are explicitly non-retriable in every mode. AbortedError:
  // the caller signaled stop. TimeoutError: the caller's latency budget (`timeoutMs`) elapsed,
  // and since each attempt resets the deadline, retrying would blow past that budget and
  // usually time out again. Without these early returns, aggressive mode's broad
  // `instanceof IngesterError` branch would wrongly classify both as retriable.
  if (
    err instanceof ConfigError ||
    err instanceof SchemaError ||
    err instanceof ValueError ||
    err instanceof StateError ||
    err instanceof AbortedError ||
    err instanceof TimeoutError
  ) {
    return false;
  }
  // Server business status is authoritative regardless of mode: a non-retriable code
  // would only burn the retry budget, and a retriable one (RegionBusy, ...) should retry
  // even under `conservative`.
  if (err instanceof ServerError) {
    return isRetryableStatusCode(err.statusCode);
  }
  if (mode === 'aggressive') {
    return err instanceof IngesterError;
  }
  if (err instanceof TransportError) {
    return CONSERVATIVE_RETRIABLE_GRPC_CODES.has(err.grpcCode);
  }
  return false;
}

// gRPC status codes that indicate the *endpoint* (not the request) is unhealthy: transient
// connectivity / capacity conditions. DEADLINE_EXCEEDED is intentionally absent: it surfaces
// as TimeoutError and reflects the caller's clock, not endpoint health (see below).
const ENDPOINT_FAILURE_GRPC_CODES = new Set<number>([
  8, // RESOURCE_EXHAUSTED
  14, // UNAVAILABLE
]);

/**
 * True if `err` indicates the endpoint itself is unhealthy and should be temporarily
 * avoided by a health-aware {@link EndpointSelector}. Only transport-level connectivity /
 * capacity errors qualify. A client-side TimeoutError does NOT: it reflects the caller's
 * own deadline (`timeoutMs`), not the endpoint's health, so a tight caller deadline must
 * never eject an otherwise-healthy endpoint. A server business error — even a retriable one
 * like `RegionBusy` — likewise means the endpoint is alive and routing correctly.
 */
export function isEndpointFailure(err: unknown): boolean {
  if (err instanceof TransportError) {
    return ENDPOINT_FAILURE_GRPC_CODES.has(err.grpcCode);
  }
  return false;
}
