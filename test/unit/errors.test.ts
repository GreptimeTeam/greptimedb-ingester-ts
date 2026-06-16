import { describe, expect, it } from 'vitest';
import {
  AbortedError,
  ConfigError,
  GreptimeStatusCode,
  SchemaError,
  ServerError,
  StateError,
  TimeoutError,
  TransportError,
  ValueError,
  isEndpointFailure,
  isRetriable,
  isRetryableStatusCode,
} from '../../src/index.js';

describe('isRetriable', () => {
  it('never retries local config/schema/value errors in either mode', () => {
    for (const mode of ['aggressive', 'conservative'] as const) {
      expect(isRetriable(new ConfigError('bad'), mode)).toBe(false);
      expect(isRetriable(new SchemaError('dup'), mode)).toBe(false);
      expect(isRetriable(new ValueError('overflow'), mode)).toBe(false);
      expect(isRetriable(new StateError('closed'), mode)).toBe(false);
    }
  });

  // Regression: aggressive mode used to classify AbortedError as retriable because
  // the bare `instanceof IngesterError` branch caught it. `withRetry`'s signal
  // check saved the runtime outcome, but the semantics leaked into logs as a
  // scheduled retry. Must be non-retriable in every mode.
  it('never retries AbortedError in either mode', () => {
    for (const mode of ['aggressive', 'conservative'] as const) {
      expect(isRetriable(new AbortedError('cancelled'), mode)).toBe(false);
    }
  });

  it('aggressive mode retries transport errors but not client timeouts', () => {
    expect(isRetriable(new TransportError('x', 13), 'aggressive')).toBe(true);
    expect(isRetriable(new TimeoutError('x'), 'aggressive')).toBe(false);
  });

  // A client-side timeout (DEADLINE_EXCEEDED -> TimeoutError) is the caller's hard latency
  // budget; each attempt resets the deadline, so retrying would multiply it by maxAttempts
  // and usually time out again. Non-retriable in every mode.
  it('never retries client TimeoutError in either mode', () => {
    for (const mode of ['aggressive', 'conservative'] as const) {
      expect(isRetriable(new TimeoutError('slow'), mode)).toBe(false);
    }
  });

  it('classifies ServerError by GreptimeDB status code in both modes', () => {
    for (const mode of ['aggressive', 'conservative'] as const) {
      expect(isRetriable(new ServerError('busy', GreptimeStatusCode.RegionBusy), mode)).toBe(true);
      expect(
        isRetriable(new ServerError('unavailable', GreptimeStatusCode.TableUnavailable), mode),
      ).toBe(true);
      // Business errors never retry, even under aggressive.
      expect(isRetriable(new ServerError('bad', GreptimeStatusCode.InvalidArguments), mode)).toBe(
        false,
      );
      expect(isRetriable(new ServerError('gone', GreptimeStatusCode.TableNotFound), mode)).toBe(
        false,
      );
      // Internal is deliberately excluded from the retryable set.
      expect(isRetriable(new ServerError('boom', GreptimeStatusCode.Internal), mode)).toBe(false);
    }
  });

  it('conservative mode only retries transient gRPC codes', () => {
    // UNAVAILABLE, RESOURCE_EXHAUSTED, ABORTED, UNKNOWN
    for (const code of [14, 8, 10, 2]) {
      expect(isRetriable(new TransportError('x', code), 'conservative')).toBe(true);
    }
    // DEADLINE_EXCEEDED (4) is excluded — it surfaces as TimeoutError and is never retried.
    // NOT_FOUND, PERMISSION_DENIED etc. not retried either.
    for (const code of [4, 5, 7, 3, 13, 16]) {
      expect(isRetriable(new TransportError('x', code), 'conservative')).toBe(false);
    }
    expect(isRetriable(new TimeoutError('x'), 'conservative')).toBe(false);
  });

  it('does not treat foreign errors as retriable', () => {
    expect(isRetriable(new Error('random'))).toBe(false);
    expect(isRetriable('string error')).toBe(false);
    expect(isRetriable(null)).toBe(false);
  });
});

describe('isRetryableStatusCode', () => {
  it('matches the GreptimeDB is_retryable set (minus Internal)', () => {
    const retryable = [
      GreptimeStatusCode.RegionNotReady,
      GreptimeStatusCode.RegionBusy,
      GreptimeStatusCode.TableUnavailable,
      GreptimeStatusCode.StorageUnavailable,
      GreptimeStatusCode.RuntimeResourcesExhausted,
    ];
    for (const code of retryable) expect(isRetryableStatusCode(code)).toBe(true);

    const notRetryable = [
      GreptimeStatusCode.Success,
      GreptimeStatusCode.Internal,
      GreptimeStatusCode.InvalidArguments,
      GreptimeStatusCode.TableNotFound,
      GreptimeStatusCode.PermissionDenied,
      GreptimeStatusCode.RateLimited,
    ];
    for (const code of notRetryable) expect(isRetryableStatusCode(code)).toBe(false);
  });
});

describe('isEndpointFailure', () => {
  it('treats only transport connectivity / capacity errors as endpoint failures', () => {
    // UNAVAILABLE, RESOURCE_EXHAUSTED
    for (const code of [14, 8]) {
      expect(isEndpointFailure(new TransportError('down', code))).toBe(true);
    }
    // A client timeout reflects the caller's clock, not endpoint health — must not eject.
    expect(isEndpointFailure(new TimeoutError('slow'))).toBe(false);
    // Request-level / caller-clock codes: NOT_FOUND, INVALID_ARGUMENT, UNKNOWN, PERMISSION_DENIED,
    // DEADLINE_EXCEEDED. None signal endpoint health.
    for (const code of [5, 3, 2, 7, 4]) {
      expect(isEndpointFailure(new TransportError('x', code))).toBe(false);
    }
  });

  it('never ejects an endpoint for a server business error, even a retriable one', () => {
    expect(isEndpointFailure(new ServerError('busy', GreptimeStatusCode.RegionBusy))).toBe(false);
    expect(isEndpointFailure(new ServerError('gone', GreptimeStatusCode.TableNotFound))).toBe(
      false,
    );
  });

  it('ignores foreign and local errors', () => {
    expect(isEndpointFailure(new Error('random'))).toBe(false);
    expect(isEndpointFailure(new ConfigError('bad'))).toBe(false);
    expect(isEndpointFailure(null)).toBe(false);
  });
});
