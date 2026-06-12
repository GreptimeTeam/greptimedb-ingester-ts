import { describe, expect, it } from 'vitest';
import { DataType, SchemaError, Table, ValueError } from '../../src/index.js';
import { toProtoValue } from '../../src/table/value.js';
import { decimal128Parts, decimalToUnscaled } from '../../src/table/validators.js';

const D = { precision: 38, scale: 12 } as const;

describe('decimalToUnscaled', () => {
  it('scales exact strings without rounding', () => {
    expect(decimalToUnscaled('123.456789012345', 38, 12)).toBe(123456789012345n);
    expect(decimalToUnscaled('0', 38, 12)).toBe(0n);
    expect(decimalToUnscaled('-5.5', 38, 12)).toBe(-5500000000000n);
  });

  it('pads fractional digits up to scale', () => {
    expect(decimalToUnscaled('1', 38, 12)).toBe(1000000000000n);
    expect(decimalToUnscaled('0.001', 38, 12)).toBe(1000000000n);
  });

  it('rounds excess fractional digits half-up on magnitude', () => {
    expect(decimalToUnscaled('1.0000000000005', 38, 12)).toBe(1000000000001n);
    expect(decimalToUnscaled('1.0000000000004', 38, 12)).toBe(1000000000000n);
    expect(decimalToUnscaled('-1.0000000000005', 38, 12)).toBe(-1000000000001n);
  });

  it('accepts number and bigint inputs', () => {
    expect(decimalToUnscaled(1.5, 38, 12)).toBe(1500000000000n);
    expect(decimalToUnscaled(0n, 38, 12)).toBe(0n);
    expect(decimalToUnscaled(42n, 38, 12)).toBe(42000000000000n);
  });

  it('expands exponential notation from strings and numbers', () => {
    expect(decimalToUnscaled('1e-7', 38, 12)).toBe(100000n);
    expect(decimalToUnscaled(1e-7, 38, 12)).toBe(100000n);
    expect(decimalToUnscaled('1.5e3', 38, 12)).toBe(1500000000000000n);
    // Leading-dot scientific notation: .5e1 == 5.
    expect(decimalToUnscaled('.5e1', 38, 12)).toBe(5000000000000n);
  });

  it('rejects a crafted huge exponent instead of allocating an enormous string', () => {
    expect(() => decimalToUnscaled('1e1000000000', 38, 12)).toThrow(ValueError);
    expect(() => decimalToUnscaled('1e-1000000000', 38, 12)).toThrow(ValueError);
  });

  it('rejects values exceeding the declared precision', () => {
    // 27 integer digits + 12 fractional = 39 > 38.
    expect(() => decimalToUnscaled('999999999999999999999999999.0', 38, 12)).toThrow(ValueError);
  });

  it('rejects malformed input and invalid precision/scale', () => {
    expect(() => decimalToUnscaled('abc', 38, 12)).toThrow(ValueError);
    expect(() => decimalToUnscaled(NaN, 38, 12)).toThrow(ValueError);
    expect(() => decimalToUnscaled('1', 0, 0)).toThrow(ValueError);
    expect(() => decimalToUnscaled('1', 38, 39)).toThrow(ValueError);
  });
});

describe('decimal128Parts', () => {
  it('splits non-negative values into hi/lo', () => {
    expect(decimal128Parts(0n)).toEqual({ hi: 0n, lo: 0n });
    expect(decimal128Parts(1n)).toEqual({ hi: 0n, lo: 1n });
    expect(decimal128Parts(1n << 64n)).toEqual({ hi: 1n, lo: 0n });
  });

  it("uses two's-complement for negatives (lo carries low 64 bits)", () => {
    expect(decimal128Parts(-1n)).toEqual({ hi: -1n, lo: -1n });
  });
});

describe('Table.addDecimalFieldColumn', () => {
  it('rejects invalid precision/scale early with SchemaError', () => {
    expect(() => Table.new('t').addDecimalFieldColumn('d', 0, 0)).toThrow(SchemaError);
    expect(() => Table.new('t').addDecimalFieldColumn('d', 39, 2)).toThrow(SchemaError);
    expect(() => Table.new('t').addDecimalFieldColumn('d', 10, 11)).toThrow(SchemaError);
    expect(() => Table.new('t').addDecimalFieldColumn('d', 10, -1)).toThrow(SchemaError);
  });

  it('accepts valid precision/scale', () => {
    expect(() => Table.new('t').addDecimalFieldColumn('d', 38, 12)).not.toThrow();
  });
});

describe('toProtoValue Decimal128', () => {
  it('encodes a decimal column value into the decimal128Value oneof', () => {
    const v = toProtoValue('123.456789012345', DataType.Decimal128, D);
    expect(v.valueData.case).toBe('decimal128Value');
    if (v.valueData.case === 'decimal128Value') {
      expect(v.valueData.value.hi).toBe(0n);
      expect(v.valueData.value.lo).toBe(123456789012345n);
    }
  });

  it('throws when decimal metadata is missing', () => {
    expect(() => toProtoValue('1.0', DataType.Decimal128)).toThrow(ValueError);
  });

  it('encodes null as the empty oneof', () => {
    expect(toProtoValue(null, DataType.Decimal128, D).valueData.case).toBeUndefined();
  });
});
