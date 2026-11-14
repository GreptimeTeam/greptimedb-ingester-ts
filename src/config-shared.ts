/**
 * Authentication for the ingest client. Only `basic` is supported right now.
 */
export interface AuthConfig {
  readonly kind: 'basic';
  readonly username: string;
  readonly password: string;
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
