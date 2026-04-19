import { describe, expect, it } from 'vitest';
import { buildFlightMetadata, buildRequestHeader } from '../../src/auth.js';
import { ConfigBuilder } from '../../src/index.js';

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

  it('supports token auth', () => {
    const cfg = ConfigBuilder.create('localhost:4001').withTokenAuth('t0k').build();
    const h = buildRequestHeader(cfg);
    expect(h.authorization?.authScheme.case).toBe('token');
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

  it('uses Bearer for token auth', () => {
    const cfg = ConfigBuilder.create('localhost:4001').withTokenAuth('abc').build();
    const md = buildFlightMetadata(cfg);
    expect(md.get('x-greptime-auth')).toEqual(['Bearer abc']);
  });

  it('omits auth header when unauthenticated', () => {
    const cfg = ConfigBuilder.create('localhost:4001').build();
    const md = buildFlightMetadata(cfg);
    expect(md.get('x-greptime-auth')).toEqual([]);
  });
});
