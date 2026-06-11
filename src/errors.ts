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
 * - `aggressive` (default): every other runtime error except local config/schema/value
 *   errors is retriable.
 * - `conservative`: retry only transient transport conditions
 *   (UNAVAILABLE / DEADLINE_EXCEEDED / RESOURCE_EXHAUSTED / ABORTED / UNKNOWN).
 */
export type RetryMode = 'aggressive' | 'conservative';

// gRPC status codes, from @grpc/grpc-js status.ts
const CONSERVATIVE_RETRIABLE_GRPC_CODES = new Set<number>([
  2, // UNKNOWN
  4, // DEADLINE_EXCEEDED
  8, // RESOURCE_EXHAUSTED
  10, // ABORTED
  14, // UNAVAILABLE
]);

export function isRetriable(err: unknown, mode: RetryMode = 'aggressive'): boolean {
  // AbortedError is explicitly non-retriable in every mode: the caller has signaled
  // they want the operation to stop. Without this, aggressive mode's broad
  // `instanceof IngesterError` branch would classify an abort as retriable — in
  // practice `withRetry` still stops because AbortSignal is latching, but the
  // semantics are wrong and confusing in logs.
  if (
    err instanceof ConfigError ||
    err instanceof SchemaError ||
    err instanceof ValueError ||
    err instanceof StateError ||
    err instanceof AbortedError
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
  return err instanceof TimeoutError;
}

// gRPC status codes that indicate the *endpoint* (not the request) is unhealthy: transient
// connectivity / capacity conditions. DEADLINE_EXCEEDED surfaces as TimeoutError (see
// promise-adapter), handled separately below.
const ENDPOINT_FAILURE_GRPC_CODES = new Set<number>([
  4, // DEADLINE_EXCEEDED (defensive; normally mapped to TimeoutError before reaching here)
  8, // RESOURCE_EXHAUSTED
  14, // UNAVAILABLE
]);

/**
 * True if `err` indicates the endpoint itself is unhealthy and should be temporarily
 * avoided by a health-aware {@link EndpointSelector}. Only transport-level connectivity /
 * capacity errors and client-side timeouts qualify. A server business error — even a
 * retriable one like `RegionBusy` — means the endpoint is alive and routing correctly, so
 * it must NOT eject the endpoint (that would punish a healthy frontend for a datanode-side
 * condition).
 */
export function isEndpointFailure(err: unknown): boolean {
  if (err instanceof TransportError) {
    return ENDPOINT_FAILURE_GRPC_CODES.has(err.grpcCode);
  }
  return err instanceof TimeoutError;
}
