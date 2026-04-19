import { describe, expect, it } from 'vitest';
import {
  ConfigError,
  SchemaError,
  ServerError,
  TimeoutError,
  TransportError,
  ValueError,
  isRetriable,
} from '../../src/index.js';

describe('isRetriable', () => {
  it('never retries local config/schema/value errors in either mode', () => {
    for (const mode of ['aggressive', 'conservative'] as const) {
      expect(isRetriable(new ConfigError('bad'), mode)).toBe(false);
      expect(isRetriable(new SchemaError('dup'), mode)).toBe(false);
      expect(isRetriable(new ValueError('overflow'), mode)).toBe(false);
    }
  });

  it('aggressive mode matches Rust is_retriable — everything else is retriable', () => {
    expect(isRetriable(new TransportError('x', 13), 'aggressive')).toBe(true);
    expect(isRetriable(new ServerError('x', 500), 'aggressive')).toBe(true);
    expect(isRetriable(new TimeoutError('x'), 'aggressive')).toBe(true);
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
    // ServerError is not retried under conservative
    expect(isRetriable(new ServerError('x', 500), 'conservative')).toBe(false);
    // Timeout is retried
    expect(isRetriable(new TimeoutError('x'), 'conservative')).toBe(true);
  });

  it('does not treat foreign errors as retriable', () => {
    expect(isRetriable(new Error('random'))).toBe(false);
    expect(isRetriable('string error')).toBe(false);
    expect(isRetriable(null)).toBe(false);
  });
});
