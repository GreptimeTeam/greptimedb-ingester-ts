import { describe, expect, it } from 'vitest';
import { ConfigBuilder, ConfigError } from '../../src/index.js';

describe('ConfigBuilder', () => {
  it('builds a minimal config with defaults', () => {
    const cfg = ConfigBuilder.create('localhost:4001').build();
    expect(cfg.endpoints).toEqual(['localhost:4001']);
    expect(cfg.database).toBe('public');
    expect(cfg.timeoutMs).toBe(60_000);
    expect(cfg.grpcCompression).toBe('none');
    expect(cfg.maxReceiveMessageSize).toBe(512 * 1024 * 1024);
    expect(cfg.auth).toBeUndefined();
    expect(cfg.tls).toBeUndefined();
  });

  it('threads chained options', () => {
    const cfg = ConfigBuilder.create('localhost:4001')
      .withEndpoints('localhost:4002', 'localhost:4003')
      .withDatabase('metrics')
      .withBasicAuth('admin', 's3cret')
      .withTls({ kind: 'system' })
      .withTimeout(10_000)
      .withKeepAlive(5_000, 2_000)
      .withGrpcCompression('gzip')
      .withRetry({ maxAttempts: 5, mode: 'conservative' })
      .build();
    expect(cfg.endpoints).toEqual(['localhost:4001', 'localhost:4002', 'localhost:4003']);
    expect(cfg.database).toBe('metrics');
    expect(cfg.auth).toEqual({ kind: 'basic', username: 'admin', password: 's3cret' });
    expect(cfg.tls).toEqual({ kind: 'system' });
    expect(cfg.timeoutMs).toBe(10_000);
    expect(cfg.keepAlive).toEqual({ timeMs: 5_000, timeoutMs: 2_000 });
    expect(cfg.grpcCompression).toBe('gzip');
    expect(cfg.retry.maxAttempts).toBe(5);
    expect(cfg.retry.mode).toBe('conservative');
  });

  it('rejects empty endpoint list', () => {
    expect(() => new ConfigBuilder().build()).toThrow(ConfigError);
  });

  it('rejects endpoints without port', () => {
    expect(() => ConfigBuilder.create('localhost').build()).toThrow(ConfigError);
  });

  it('rejects endpoints with empty host', () => {
    expect(() => ConfigBuilder.create(':4001').build()).toThrow(ConfigError);
  });

  it('rejects endpoints with empty port', () => {
    expect(() => ConfigBuilder.create('localhost:').build()).toThrow(ConfigError);
  });

  it('rejects endpoints with non-numeric port', () => {
    expect(() => ConfigBuilder.create('localhost:abc').build()).toThrow(ConfigError);
  });

  it('rejects endpoints with port out of range', () => {
    expect(() => ConfigBuilder.create('localhost:0').build()).toThrow(ConfigError);
    expect(() => ConfigBuilder.create('localhost:65536').build()).toThrow(ConfigError);
    expect(() => ConfigBuilder.create('localhost:99999').build()).toThrow(ConfigError);
  });

  it('accepts IPv4:port', () => {
    expect(() => ConfigBuilder.create('127.0.0.1:4001').build()).not.toThrow();
  });

  it('accepts IPv6 bracketed form', () => {
    expect(() => ConfigBuilder.create('[::1]:4001').build()).not.toThrow();
  });

  it('rejects malformed IPv6 (missing "]:port")', () => {
    expect(() => ConfigBuilder.create('[::1]').build()).toThrow(ConfigError);
  });

  it('rejects empty database', () => {
    expect(() => ConfigBuilder.create('localhost:4001').withDatabase('').build()).toThrow(
      ConfigError,
    );
  });

  it('rejects non-positive timeout', () => {
    expect(() => ConfigBuilder.create('localhost:4001').withTimeout(0).build()).toThrow(
      ConfigError,
    );
  });
});
