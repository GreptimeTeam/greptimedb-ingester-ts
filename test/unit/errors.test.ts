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

  it('aggressive mode retries transport / timeout errors', () => {
    expect(isRetriable(new TransportError('x', 13), 'aggressive')).toBe(true);
    expect(isRetriable(new TimeoutError('x'), 'aggressive')).toBe(true);
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
    // UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, UNKNOWN
    for (const code of [14, 4, 8, 10, 2]) {
      expect(isRetriable(new TransportError('x', code), 'conservative')).toBe(true);
    }
    // NOT_FOUND, PERMISSION_DENIED etc. not retried
    for (const code of [5, 7, 3, 13, 16]) {
      expect(isRetriable(new TransportError('x', code), 'conservative')).toBe(false);
    }
    expect(isRetriable(new TimeoutError('x'), 'conservative')).toBe(true);
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
  it('treats only transport connectivity / capacity errors and timeouts as endpoint failures', () => {
    // UNAVAILABLE, RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED
    for (const code of [14, 8, 4]) {
      expect(isEndpointFailure(new TransportError('down', code))).toBe(true);
    }
    expect(isEndpointFailure(new TimeoutError('slow'))).toBe(true);
    // Other transport codes (NOT_FOUND, INVALID_ARGUMENT, UNKNOWN) are request-level.
    for (const code of [5, 3, 2, 7]) {
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
