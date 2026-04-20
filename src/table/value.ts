// TS value → proto `Value` oneof conversion. Per-type strict validation; unsafe conversions
// throw `ValueError` so the user sees the problem at encode time, not via a cryptic server
// response.

import { create } from '@bufbuild/protobuf';
import { ValueSchema, type Value } from '../generated/greptime/v1/row_pb.js';
import { ValueError } from '../errors.js';
import { DataType } from './data-type.js';
import {
  I64_MAX,
  I64_MIN,
  U64_MAX,
  asBigInt,
  asBinary,
  asBoolean,
  asIntInRange,
  asNumber,
  asString,
  dateToMs,
  safeStringifyJson,
} from './validators.js';

const EMPTY_VALUE: Value = create(ValueSchema, { valueData: { case: undefined } });

const I8_MIN = -(2 ** 7);
const I8_MAX = 2 ** 7 - 1;
const I16_MIN = -(2 ** 15);
const I16_MAX = 2 ** 15 - 1;
const I32_MIN = -(2 ** 31);
const I32_MAX = 2 ** 31 - 1;
const U8_MAX = 2 ** 8 - 1;
const U16_MAX = 2 ** 16 - 1;
const U32_MAX = 2 ** 32 - 1;

function dateToUnixDays(d: Date): number {
  // Date column = days since Unix epoch (1970-01-01 UTC).
  return Math.floor(dateToMs(d, 'Date') / 86_400_000);
}

function scaleDateToTimestamp(d: Date, dataType: DataType): bigint {
  const ms = BigInt(dateToMs(d, 'Timestamp'));
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
      return create(ValueSchema, { valueData: { case: 'boolValue', value: asBoolean('Bool', ts) } });
    }
    case DataType.String: {
      return create(ValueSchema, {
        valueData: { case: 'stringValue', value: asString('String', ts) },
      });
    }
    case DataType.Binary: {
      return create(ValueSchema, { valueData: { case: 'binaryValue', value: asBinary(ts) } });
    }
    case DataType.Date: {
      const days = ts instanceof Date ? dateToUnixDays(ts) : asIntInRange('Date', ts, I32_MIN, I32_MAX);
      return create(ValueSchema, { valueData: { case: 'dateValue', value: days } });
    }
    case DataType.Datetime: {
      const b = ts instanceof Date
        ? BigInt(dateToMs(ts, 'Datetime'))
        : asBigInt('Datetime', ts, I64_MIN, I64_MAX);
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
      // `safeStringifyJson` wraps native `TypeError` (bigint / circular) as `ValueError`.
      return create(ValueSchema, {
        valueData: { case: 'stringValue', value: safeStringifyJson(ts) },
      });
    }
  }
}
