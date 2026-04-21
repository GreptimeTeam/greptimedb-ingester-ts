// Logger plumbing: before this round, `ConfigBuilder.withLogger()` was dead —
// users could configure a Logger but nothing in src/ ever called it. These tests
// lock down the minimum set of events we currently promise to log on the retry path.

import { describe, expect, it } from 'vitest';
import { DEFAULT_RETRY_POLICY } from '../../src/config.js';
import { TransportError, ValueError } from '../../src/errors.js';
import type { LogLevel, Logger } from '../../src/index.js';
import { withRetry } from '../../src/transport/retry.js';

interface LogEntry {
  level: LogLevel;
  msg: string;
  meta?: Record<string, unknown>;
}

function recordingLogger(): { logger: Logger; entries: LogEntry[] } {
  const entries: LogEntry[] = [];
  const logger: Logger = {
    log(level, msg, meta) {
      const entry: LogEntry = { level, msg };
      if (meta !== undefined) entry.meta = meta;
      entries.push(entry);
    },
  };
  return { logger, entries };
}

describe('withRetry logger wiring', () => {
  it('emits a debug entry per retry attempt, with attempt/backoff/errorKind', async () => {
    const { logger, entries } = recordingLogger();
    let calls = 0;
    const fn = (): Promise<string> => {
      calls++;
      if (calls < 3) return Promise.reject(new TransportError('transient', 14));
      return Promise.resolve('ok');
    };
    const got = await withRetry(
      fn,
      { ...DEFAULT_RETRY_POLICY, maxAttempts: 5, initialBackoffMs: 1, maxBackoffMs: 2 },
      undefined,
      logger,
    );
    expect(got).toBe('ok');
    expect(calls).toBe(3);
    // Two retries scheduled → two debug entries.
    const debugEntries = entries.filter((e) => e.level === 'debug');
    expect(debugEntries).toHaveLength(2);
    for (const e of debugEntries) {
      expect(e.msg).toContain('withRetry scheduling retry');
      expect(e.meta).toBeDefined();
      expect(e.meta?.attempt).toBeTypeOf('number');
      expect(e.meta?.backoffMs).toBeTypeOf('number');
      expect(e.meta?.errorKind).toBe('transport');
    }
  });

  it('does not log when there is no retry (success on first attempt)', async () => {
    const { logger, entries } = recordingLogger();
    await withRetry(
      () => Promise.resolve('ok'),
      { ...DEFAULT_RETRY_POLICY, maxAttempts: 3 },
      undefined,
      logger,
    );
    expect(entries).toHaveLength(0);
  });

  it('logs stop reason, but no retry scheduling, for non-retriable errors', async () => {
    const { logger, entries } = recordingLogger();
    await expect(
      withRetry(
        () => Promise.reject(new ValueError('bad input')),
        { ...DEFAULT_RETRY_POLICY, maxAttempts: 3 },
        undefined,
        logger,
      ),
    ).rejects.toBeInstanceOf(ValueError);
    const debugEntries = entries.filter((e) => e.level === 'debug');
    expect(debugEntries).toHaveLength(1);
    expect(debugEntries[0]?.msg).toBe('withRetry stopping on non-retriable error');
    expect(debugEntries[0]?.meta).toEqual(
      expect.objectContaining({
        attempt: 1,
        maxAttempts: 3,
        errorKind: 'value',
        mode: 'aggressive',
      }),
    );
  });
});
