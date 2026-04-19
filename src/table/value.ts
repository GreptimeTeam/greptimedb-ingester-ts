// TS value → proto `Value` oneof conversion. Per-type strict validation; unsafe conversions
// throw `ValueError` so the user sees the problem at encode time, not via a cryptic server
// response.

import { create } from '@bufbuild/protobuf';
import { ValueSchema, type Value } from '../generated/greptime/v1/row_pb.js';
import { ValueError } from '../errors.js';
import { DataType } from './data-type.js';

const EMPTY_VALUE: Value = create(ValueSchema, { valueData: { case: undefined } });

const MAX_SAFE_INT = Number.MAX_SAFE_INTEGER;
const MIN_SAFE_INT = Number.MIN_SAFE_INTEGER;
const I8_MIN = -(2 ** 7);
const I8_MAX = 2 ** 7 - 1;
const I16_MIN = -(2 ** 15);
const I16_MAX = 2 ** 15 - 1;
const I32_MIN = -(2 ** 31);
const I32_MAX = 2 ** 31 - 1;
const U8_MAX = 2 ** 8 - 1;
const U16_MAX = 2 ** 16 - 1;
const U32_MAX = 2 ** 32 - 1;
const U64_MAX = (1n << 64n) - 1n;
const I64_MAX = (1n << 63n) - 1n;
const I64_MIN = -(1n << 63n);

function asBigInt(name: string, v: unknown, min: bigint, max: bigint): bigint {
  let b: bigint;
  if (typeof v === 'bigint') {
    b = v;
  } else if (typeof v === 'number') {
    if (!Number.isFinite(v) || !Number.isInteger(v)) {
      throw new ValueError(`${name} expected integer, got ${v}`);
    }
    if (v > MAX_SAFE_INT || v < MIN_SAFE_INT) {
      throw new ValueError(
        `${name} received number ${v} outside safe integer range; pass a bigint instead`,
      );
    }
    b = BigInt(v);
  } else {
    throw new ValueError(`${name} expected bigint|number, got ${typeof v}`);
  }
  if (b < min || b > max) {
    throw new ValueError(`${name} value ${b} out of range [${min}, ${max}]`);
  }
  return b;
}

function asIntInRange(name: string, v: unknown, min: number, max: number): number {
  let n: number;
  if (typeof v === 'number') {
    if (!Number.isInteger(v)) throw new ValueError(`${name} expected integer, got ${v}`);
    n = v;
  } else if (typeof v === 'bigint') {
    if (v > BigInt(max) || v < BigInt(min)) {
      throw new ValueError(`${name} bigint ${v} out of range [${min}, ${max}]`);
    }
    n = Number(v);
  } else {
    throw new ValueError(`${name} expected number|bigint, got ${typeof v}`);
  }
  if (n < min || n > max) throw new ValueError(`${name} value ${n} out of range [${min}, ${max}]`);
  return n;
}

function asNumber(name: string, v: unknown): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'bigint') return Number(v);
  throw new ValueError(`${name} expected number, got ${typeof v}`);
}

function asBinary(v: unknown): Uint8Array {
  if (v instanceof Uint8Array) return v; // Buffer is-a Uint8Array, passes through
  if (typeof v === 'string') return new TextEncoder().encode(v);
  throw new ValueError(`binary expected Uint8Array|Buffer|string, got ${typeof v}`);
}

function dateToUnixDays(d: Date): number {
  // Date column = days since Unix epoch (1970-01-01 UTC).
  const ms = d.getTime();
  if (!Number.isFinite(ms)) throw new ValueError('Date value is invalid (NaN getTime)');
  return Math.floor(ms / 86_400_000);
}

function scaleDateToTimestamp(d: Date, dataType: DataType): bigint {
  const ms = BigInt(d.getTime());
  switch (dataType) {
    case DataType.TimestampSecond:
      return ms / 1000n;
    case DataType.TimestampMillisecond:
      return ms;
    case DataType.TimestampMicrosecond:
      return ms * 1000n;
    case DataType.TimestampNanosecond:
      return ms * 1_000_000n;
    default:
      throw new ValueError(`unexpected timestamp dataType ${dataType}`);
  }
}

function asTimestamp(name: string, v: unknown, dataType: DataType): bigint {
  if (v instanceof Date) return scaleDateToTimestamp(v, dataType);
  return asBigInt(name, v, I64_MIN, I64_MAX);
}

/**
 * Convert a TS value to a proto `Value` message. `null`/`undefined` produces an empty oneof
 * (the protobuf encoding of "null" for row-insert) — callers must reject null-on-non-nullable
 * columns before reaching this layer if the server schema forbids nulls.
 */
export function toProtoValue(ts: unknown, dataType: DataType): Value {
  if (ts === null || ts === undefined) return EMPTY_VALUE;

  switch (dataType) {
    case DataType.Int8: {
      const n = asIntInRange('Int8', ts, I8_MIN, I8_MAX);
      return create(ValueSchema, { valueData: { case: 'i8Value', value: n } });
    }
    case DataType.Int16: {
      const n = asIntInRange('Int16', ts, I16_MIN, I16_MAX);
      return create(ValueSchema, { valueData: { case: 'i16Value', value: n } });
    }
    case DataType.Int32: {
      const n = asIntInRange('Int32', ts, I32_MIN, I32_MAX);
      return create(ValueSchema, { valueData: { case: 'i32Value', value: n } });
    }
    case DataType.Int64: {
      const b = asBigInt('Int64', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'i64Value', value: b } });
    }
    case DataType.Uint8: {
      const n = asIntInRange('Uint8', ts, 0, U8_MAX);
      return create(ValueSchema, { valueData: { case: 'u8Value', value: n } });
    }
    case DataType.Uint16: {
      const n = asIntInRange('Uint16', ts, 0, U16_MAX);
      return create(ValueSchema, { valueData: { case: 'u16Value', value: n } });
    }
    case DataType.Uint32: {
      const n = asIntInRange('Uint32', ts, 0, U32_MAX);
      return create(ValueSchema, { valueData: { case: 'u32Value', value: n } });
    }
    case DataType.Uint64: {
      const b = asBigInt('Uint64', ts, 0n, U64_MAX);
      return create(ValueSchema, { valueData: { case: 'u64Value', value: b } });
    }
    case DataType.Float32: {
      const n = asNumber('Float32', ts);
      return create(ValueSchema, { valueData: { case: 'f32Value', value: n } });
    }
    case DataType.Float64: {
      const n = asNumber('Float64', ts);
      return create(ValueSchema, { valueData: { case: 'f64Value', value: n } });
    }
    case DataType.Bool: {
      if (typeof ts !== 'boolean') throw new ValueError(`Bool expected boolean, got ${typeof ts}`);
      return create(ValueSchema, { valueData: { case: 'boolValue', value: ts } });
    }
    case DataType.String: {
      if (typeof ts !== 'string')
        throw new ValueError(`String expected string, got ${typeof ts}`);
      return create(ValueSchema, { valueData: { case: 'stringValue', value: ts } });
    }
    case DataType.Binary: {
      return create(ValueSchema, { valueData: { case: 'binaryValue', value: asBinary(ts) } });
    }
    case DataType.Date: {
      const days = ts instanceof Date ? dateToUnixDays(ts) : asIntInRange('Date', ts, I32_MIN, I32_MAX);
      return create(ValueSchema, { valueData: { case: 'dateValue', value: days } });
    }
    case DataType.Datetime: {
      const b = ts instanceof Date ? BigInt(ts.getTime()) : asBigInt('Datetime', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'datetimeValue', value: b } });
    }
    case DataType.TimestampSecond:
      return create(ValueSchema, {
        valueData: { case: 'timestampSecondValue', value: asTimestamp('TimestampSecond', ts, dataType) },
      });
    case DataType.TimestampMillisecond:
      return create(ValueSchema, {
        valueData: {
          case: 'timestampMillisecondValue',
          value: asTimestamp('TimestampMillisecond', ts, dataType),
        },
      });
    case DataType.TimestampMicrosecond:
      return create(ValueSchema, {
        valueData: {
          case: 'timestampMicrosecondValue',
          value: asTimestamp('TimestampMicrosecond', ts, dataType),
        },
      });
    case DataType.TimestampNanosecond:
      return create(ValueSchema, {
        valueData: {
          case: 'timestampNanosecondValue',
          value: asTimestamp('TimestampNanosecond', ts, dataType),
        },
      });
    case DataType.TimeSecond: {
      const b = asBigInt('TimeSecond', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'timeSecondValue', value: b } });
    }
    case DataType.TimeMillisecond: {
      const b = asBigInt('TimeMillisecond', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'timeMillisecondValue', value: b } });
    }
    case DataType.TimeMicrosecond: {
      const b = asBigInt('TimeMicrosecond', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'timeMicrosecondValue', value: b } });
    }
    case DataType.TimeNanosecond: {
      const b = asBigInt('TimeNanosecond', ts, I64_MIN, I64_MAX);
      return create(ValueSchema, { valueData: { case: 'timeNanosecondValue', value: b } });
    }
    case DataType.Json: {
      // Unary row-insert path ships JSON columns as `string_value` holding the JSON text.
      // (The bulk Arrow path, by contrast, ships JSON as a `Binary` column of UTF-8 bytes;
      // both representations are accepted by the server for a JSON-typed column.)
      // Strings are taken verbatim to allow passing pre-serialized JSON.
      const text = typeof ts === 'string' ? ts : JSON.stringify(ts);
      return create(ValueSchema, {
        valueData: { case: 'stringValue', value: text },
      });
    }
  }
}
