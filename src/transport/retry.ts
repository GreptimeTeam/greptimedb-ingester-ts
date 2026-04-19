import type { RetryPolicy } from '../config.js';
import { AbortedError, isRetriable } from '../errors.js';

function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // Use a const holder so eslint `prefer-const` is satisfied while the timer can
    // still be both observed by the abort listener and cleared from cleanup.
    const state: { timer?: NodeJS.Timeout } = {};
    const cleanup = (): void => {
      signal?.removeEventListener('abort', onAbort);
      if (state.timer !== undefined) clearTimeout(state.timer);
    };
    const onAbort = (): void => {
      cleanup();
      reject(new AbortedError('aborted during retry sleep'));
    };
    if (signal?.aborted === true) {
      reject(new AbortedError('aborted before retry sleep'));
      return;
    }
    signal?.addEventListener('abort', onAbort, { once: true });
    state.timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
  });
}

function computeBackoffMs(policy: RetryPolicy, attempt: number): number {
  const base = Math.min(
    policy.maxBackoffMs,
    policy.initialBackoffMs * policy.backoffMultiplier ** attempt,
  );
  if (policy.jitter === 'full') return Math.random() * base;
  return base;
}

/**
 * Run `fn` with retry. Retries iff `isRetriable(err, policy.mode)` returns true and attempts
 * remain. Each retry waits `computeBackoffMs(policy, attempt)`. Respects AbortSignal.
 */
export async function withRetry<T>(
  fn: (attempt: number) => Promise<T>,
  policy: RetryPolicy,
  signal?: AbortSignal,
): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < policy.maxAttempts; attempt++) {
    if (signal?.aborted === true) throw new AbortedError('aborted before attempt');
    try {
      return await fn(attempt);
    } catch (err) {
      lastErr = err;
      if (attempt === policy.maxAttempts - 1) break;
      if (!isRetriable(err, policy.mode)) break;
      const backoff = computeBackoffMs(policy, attempt);
      await sleep(backoff, signal);
    }
  }
  throw lastErr;
}
