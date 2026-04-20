import { describe, expect, it } from 'vitest';
import { DataType, ValueError } from '../../src/index.js';
import { toProtoValue } from '../../src/table/value.js';

describe('toProtoValue', () => {
  describe('integers', () => {
    it('encodes Int8 bounds', () => {
      expect(toProtoValue(-128, DataType.Int8).valueData).toEqual({ case: 'i8Value', value: -128 });
      expect(toProtoValue(127, DataType.Int8).valueData).toEqual({ case: 'i8Value', value: 127 });
    });
    it('rejects Int8 overflow', () => {
      expect(() => toProtoValue(128, DataType.Int8)).toThrow(ValueError);
      expect(() => toProtoValue(-129, DataType.Int8)).toThrow(ValueError);
    });
    it('rejects non-integer number for Int32', () => {
      expect(() => toProtoValue(1.5, DataType.Int32)).toThrow(ValueError);
    });
    it('encodes Int64 from bigint', () => {
      const v = toProtoValue(9007199254740993n, DataType.Int64);
      expect(v.valueData).toEqual({ case: 'i64Value', value: 9007199254740993n });
    });
    it('accepts number for Int64 within safe range', () => {
      const v = toProtoValue(42, DataType.Int64);
      expect(v.valueData).toEqual({ case: 'i64Value', value: 42n });
    });
    it('rejects unsafe integer number for Int64', () => {
      expect(() => toProtoValue(2 ** 53 + 10, DataType.Int64)).toThrow(ValueError);
    });
    it('rejects out-of-range bigint for Int64', () => {
      expect(() => toProtoValue(1n << 70n, DataType.Int64)).toThrow(ValueError);
    });
    it('rejects negative for Uint*', () => {
      expect(() => toProtoValue(-1, DataType.Uint8)).toThrow(ValueError);
      expect(() => toProtoValue(-1n, DataType.Uint64)).toThrow(ValueError);
    });
  });

  describe('floats', () => {
    it('encodes Float64', () => {
      expect(toProtoValue(1.5, DataType.Float64).valueData).toEqual({
        case: 'f64Value',
        value: 1.5,
      });
    });
    it('encodes Float32', () => {
      expect(toProtoValue(1.5, DataType.Float32).valueData).toEqual({
        case: 'f32Value',
        value: 1.5,
      });
    });
    it('rejects non-number for Float64', () => {
      expect(() => toProtoValue('x', DataType.Float64)).toThrow(ValueError);
    });
  });

  describe('bool / string / binary', () => {
    it('encodes Bool', () => {
      expect(toProtoValue(true, DataType.Bool).valueData).toEqual({ case: 'boolValue', value: true });
    });
    it('rejects non-boolean for Bool', () => {
      expect(() => toProtoValue(1, DataType.Bool)).toThrow(ValueError);
    });
    it('encodes String', () => {
      expect(toProtoValue('hi', DataType.String).valueData).toEqual({
        case: 'stringValue',
        value: 'hi',
      });
    });
    it('encodes Binary from Uint8Array', () => {
      const buf = new Uint8Array([1, 2, 3]);
      const v = toProtoValue(buf, DataType.Binary);
      expect(v.valueData).toEqual({ case: 'binaryValue', value: buf });
    });
    it('encodes Binary from string', () => {
      const v = toProtoValue('ab', DataType.Binary);
      expect(v.valueData.case).toBe('binaryValue');
      if (v.valueData.case === 'binaryValue') {
        expect(Array.from(v.valueData.value)).toEqual([97, 98]);
      }
    });
  });

  describe('timestamps', () => {
    it('accepts number for TimestampMillisecond (interpreted at given precision)', () => {
      const v = toProtoValue(1_700_000_000_000, DataType.TimestampMillisecond);
      expect(v.valueData).toEqual({ case: 'timestampMillisecondValue', value: 1_700_000_000_000n });
    });
    it('scales Date to ns', () => {
      const d = new Date(1_700_000_000_000);
      const v = toProtoValue(d, DataType.TimestampNanosecond);
      expect(v.valueData).toEqual({
        case: 'timestampNanosecondValue',
        value: 1_700_000_000_000_000_000n,
      });
    });
    it('scales Date to seconds', () => {
      const d = new Date(1_700_000_000_000);
      const v = toProtoValue(d, DataType.TimestampSecond);
      expect(v.valueData).toEqual({ case: 'timestampSecondValue', value: 1_700_000_000n });
    });
  });

  describe('null / json', () => {
    it('null/undefined produces empty valueData', () => {
      expect(toProtoValue(null, DataType.Int64).valueData).toEqual({ case: undefined });
      expect(toProtoValue(undefined, DataType.String).valueData).toEqual({ case: undefined });
    });
    it('json stringifies non-string values', () => {
      const v = toProtoValue({ a: 1 }, DataType.Json);
      expect(v.valueData).toEqual({ case: 'stringValue', value: '{"a":1}' });
    });
    it('json passes strings through verbatim', () => {
      const v = toProtoValue('{"pre":true}', DataType.Json);
      expect(v.valueData).toEqual({ case: 'stringValue', value: '{"pre":true}' });
    });
    it('json with bigint field throws ValueError (not native TypeError)', () => {
      expect(() => toProtoValue({ id: 1n }, DataType.Json)).toThrow(ValueError);
    });
    it('json with circular reference throws ValueError (not native TypeError)', () => {
      const circ: Record<string, unknown> = { name: 'x' };
      circ.self = circ;
      expect(() => toProtoValue(circ, DataType.Json)).toThrow(ValueError);
    });
  });
});
