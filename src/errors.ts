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
 * Classify whether an error is worth retrying.
 *
 * - `aggressive` (default): mirror Rust `src/error.rs:211` — every runtime error except
 *   local config/schema/value errors is retriable.
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
  if (mode === 'aggressive') {
    return err instanceof IngesterError;
  }
  if (err instanceof TransportError) {
    return CONSERVATIVE_RETRIABLE_GRPC_CODES.has(err.grpcCode);
  }
  return err instanceof TimeoutError;
}
