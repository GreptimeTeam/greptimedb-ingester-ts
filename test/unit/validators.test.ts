import { describe, expect, it } from 'vitest';
import { ValueError } from '../../src/index.js';
import { safeStringifyJson } from '../../src/table/validators.js';

describe('safeStringifyJson', () => {
  it('passes strings through verbatim', () => {
    expect(safeStringifyJson('{"a":1}')).toBe('{"a":1}');
  });

  it('stringifies plain objects', () => {
    expect(safeStringifyJson({ a: 1, b: [2, 3] })).toBe('{"a":1,"b":[2,3]}');
  });

  it('wraps bigint TypeError as ValueError', () => {
    expect(() => safeStringifyJson({ big: 1n })).toThrow(ValueError);
  });

  it('wraps circular-reference TypeError as ValueError', () => {
    const o: { self?: unknown } = {};
    o.self = o;
    expect(() => safeStringifyJson(o)).toThrow(ValueError);
  });

  // These three used to silently return the primitive `undefined` (typed as string),
  // which downstream wrote the literal ASCII `"undefined"` into JSON columns.
  it('rejects top-level undefined', () => {
    expect(() => safeStringifyJson(undefined)).toThrow(ValueError);
  });

  it('rejects functions', () => {
    expect(() => safeStringifyJson(() => 1)).toThrow(ValueError);
  });

  it('rejects symbols', () => {
    expect(() => safeStringifyJson(Symbol('x'))).toThrow(ValueError);
  });

  it('still strips function / symbol / undefined properties inside an object (standard JSON)', () => {
    // This is the *standard* JSON.stringify behavior and NOT what we want to reject:
    // only the top-level value returning `undefined` is the problem.
    const out = safeStringifyJson({ a: 1, b: undefined, c: () => 1, d: Symbol('x') });
    expect(out).toBe('{"a":1}');
  });
});
