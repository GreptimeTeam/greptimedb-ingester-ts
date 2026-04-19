import { describe, expect, it, vi } from 'vitest';
import { AbortedError, TransportError } from '../../src/errors.js';
import { DEFAULT_RETRY_POLICY } from '../../src/config.js';
import { withRetry } from '../../src/transport/retry.js';

describe('withRetry', () => {
  it('returns the first successful result without retrying', async () => {
    const fn = vi.fn(() => Promise.resolve('ok'));
    const got = await withRetry(fn, { ...DEFAULT_RETRY_POLICY, maxAttempts: 3 });
    expect(got).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('retries retriable errors up to maxAttempts times', async () => {
    let attempts = 0;
    const fn = vi.fn(() => {
      attempts++;
      if (attempts < 3) return Promise.reject(new TransportError('unavailable', 14));
      return Promise.resolve('ok');
    });
    const got = await withRetry(fn, {
      ...DEFAULT_RETRY_POLICY,
      maxAttempts: 5,
      initialBackoffMs: 1,
      maxBackoffMs: 2,
    });
    expect(got).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('does not retry non-retriable errors', async () => {
    const err = new Error('not ours');
    const fn = vi.fn(() => Promise.reject(err));
    await expect(
      withRetry(fn, { ...DEFAULT_RETRY_POLICY, maxAttempts: 3, initialBackoffMs: 1 }),
    ).rejects.toBe(err);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('respects AbortSignal before the first attempt', async () => {
    const fn = vi.fn(() => Promise.resolve('never'));
    const ac = new AbortController();
    ac.abort();
    await expect(
      withRetry(fn, { ...DEFAULT_RETRY_POLICY, initialBackoffMs: 1 }, ac.signal),
    ).rejects.toBeInstanceOf(AbortedError);
    expect(fn).not.toHaveBeenCalled();
  });

  it('respects AbortSignal during backoff sleep', async () => {
    const ac = new AbortController();
    const fn = vi.fn(() => Promise.reject(new TransportError('try again', 14)));
    const p = withRetry(
      fn,
      { ...DEFAULT_RETRY_POLICY, maxAttempts: 5, initialBackoffMs: 50, maxBackoffMs: 100 },
      ac.signal,
    );
    setTimeout(() => {
      ac.abort();
    }, 10);
    await expect(p).rejects.toBeInstanceOf(AbortedError);
  });

  it('stops after maxAttempts and surfaces the last error', async () => {
    const fn = vi.fn(() => Promise.reject(new TransportError('still down', 14)));
    await expect(
      withRetry(fn, {
        ...DEFAULT_RETRY_POLICY,
        maxAttempts: 3,
        initialBackoffMs: 1,
        maxBackoffMs: 2,
      }),
    ).rejects.toBeInstanceOf(TransportError);
    expect(fn).toHaveBeenCalledTimes(3);
  });
});
