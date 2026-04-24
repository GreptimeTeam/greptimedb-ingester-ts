import { describe, expect, it } from 'vitest';
import { Metadata } from '@grpc/grpc-js';
import { buildFlightMetadata, buildHintsMetadata, buildRequestHeader } from '../../src/auth.js';
import { ConfigBuilder, ValueError } from '../../src/index.js';

describe('buildRequestHeader (unary/streaming path)', () => {
  it('embeds dbname and basic auth into proto RequestHeader', () => {
    const cfg = ConfigBuilder.create('localhost:4001')
      .withDatabase('metrics')
      .withBasicAuth('alice', 'pw')
      .build();
    const h = buildRequestHeader(cfg);
    expect(h.dbname).toBe('metrics');
    expect(h.authorization?.authScheme.case).toBe('basic');
    if (h.authorization?.authScheme.case === 'basic') {
      expect(h.authorization.authScheme.value.username).toBe('alice');
      expect(h.authorization.authScheme.value.password).toBe('pw');
    }
  });

  it('omits authorization when no auth configured', () => {
    const cfg = ConfigBuilder.create('localhost:4001').build();
    const h = buildRequestHeader(cfg);
    expect(h.authorization).toBeUndefined();
  });
});

describe('buildFlightMetadata (bulk path)', () => {
  it('writes x-greptime-db-name and base64 basic auth into gRPC metadata', () => {
    const cfg = ConfigBuilder.create('localhost:4001')
      .withDatabase('metrics')
      .withBasicAuth('alice', 'pw')
      .build();
    const md = buildFlightMetadata(cfg);
    expect(md.get('x-greptime-db-name')).toEqual(['metrics']);
    const auth = md.get('x-greptime-auth');
    expect(auth).toHaveLength(1);
    const expected = `Basic ${Buffer.from('alice:pw').toString('base64')}`;
    expect(auth[0]).toBe(expected);
  });

  it('omits auth header when unauthenticated', () => {
    const cfg = ConfigBuilder.create('localhost:4001').build();
    const md = buildFlightMetadata(cfg);
    expect(md.get('x-greptime-auth')).toEqual([]);
  });
});

describe('buildHintsMetadata (unary/streaming hints — Rust-aligned wire format)', () => {
  it('returns a fresh empty Metadata when hints is undefined', () => {
    const md = buildHintsMetadata(undefined);
    expect(md).toBeInstanceOf(Metadata);
    expect(md.get('x-greptime-hints')).toEqual([]);
    // Distinct instance every call — no shared singleton that grpc-js might mutate.
    expect(buildHintsMetadata(undefined)).not.toBe(md);
  });

  it('returns a fresh empty Metadata when hints is empty', () => {
    const md = buildHintsMetadata({});
    expect(md).toBeInstanceOf(Metadata);
    expect(md.get('x-greptime-hints')).toEqual([]);
  });

  it('serializes hints as a single x-greptime-hints comma-joined header', () => {
    const md = buildHintsMetadata({ ttl: '7d', mode: 'append' });
    // Single key with comma-joined value, matching Rust src/database.rs:198-211.
    expect(md.get('x-greptime-hints')).toEqual(['ttl=7d,mode=append']);
    // Per-key form (used by the previous broken implementation) must NOT appear.
    expect(md.get('x-greptime-hint-ttl')).toEqual([]);
  });

  it('rejects empty key', () => {
    expect(() => buildHintsMetadata({ '': 'v' })).toThrow(ValueError);
  });

  it('rejects key containing illegal "," or "="', () => {
    expect(() => buildHintsMetadata({ 'a,b': 'v' })).toThrow(ValueError);
    expect(() => buildHintsMetadata({ 'a=b': 'v' })).toThrow(ValueError);
  });

  it('rejects value containing illegal "," or "="', () => {
    expect(() => buildHintsMetadata({ k: 'a,b' })).toThrow(ValueError);
    expect(() => buildHintsMetadata({ k: 'a=b' })).toThrow(ValueError);
  });
});
