// Public DataType / Precision enums. Keep these ergonomic on the user-facing surface;
// the proto `ColumnDataType` values are an implementation detail.

import { ColumnDataType, SemanticType } from '../generated/greptime/v1/common_pb.js';

/** Supported column data types. Mirrors a curated subset of proto ColumnDataType. */
export const DataType = {
  Int8: 'Int8',
  Int16: 'Int16',
  Int32: 'Int32',
  Int64: 'Int64',
  Uint8: 'Uint8',
  Uint16: 'Uint16',
  Uint32: 'Uint32',
  Uint64: 'Uint64',
  Float32: 'Float32',
  Float64: 'Float64',
  Bool: 'Bool',
  String: 'String',
  Binary: 'Binary',
  Date: 'Date',
  /**
   * @deprecated Use `TimestampMicrosecond` instead. GreptimeDB v1.0+ rejects
   * `datetime` at the SQL layer; this type is kept only for interop with tables
   * created on older servers. Semantically an alias of `TimestampMicrosecond`.
   */
  Datetime: 'Datetime',
  TimestampSecond: 'TimestampSecond',
  TimestampMillisecond: 'TimestampMillisecond',
  TimestampMicrosecond: 'TimestampMicrosecond',
  TimestampNanosecond: 'TimestampNanosecond',
  TimeSecond: 'TimeSecond',
  TimeMillisecond: 'TimeMillisecond',
  TimeMicrosecond: 'TimeMicrosecond',
  TimeNanosecond: 'TimeNanosecond',
  Json: 'Json',
} as const;

export type DataType = (typeof DataType)[keyof typeof DataType];

/** Timestamp precision. Only relevant for `addTimestampColumn`. */
export const Precision = {
  Second: 'Second',
  Millisecond: 'Millisecond',
  Microsecond: 'Microsecond',
  Nanosecond: 'Nanosecond',
} as const;

export type Precision = (typeof Precision)[keyof typeof Precision];

export function precisionToTimestampDataType(p: Precision): DataType {
  switch (p) {
    case 'Second':
      return DataType.TimestampSecond;
    case 'Millisecond':
      return DataType.TimestampMillisecond;
    case 'Microsecond':
      return DataType.TimestampMicrosecond;
    case 'Nanosecond':
      return DataType.TimestampNanosecond;
  }
}

export function isTimestampDataType(t: DataType): boolean {
  return (
    t === DataType.TimestampSecond ||
    t === DataType.TimestampMillisecond ||
    t === DataType.TimestampMicrosecond ||
    t === DataType.TimestampNanosecond
  );
}

/** Semantic type of a column within a GreptimeDB table. */
export type Semantic = 'tag' | 'field' | 'timestamp';

export function toProtoSemanticType(s: Semantic): SemanticType {
  switch (s) {
    case 'tag':
      return SemanticType.TAG;
    case 'field':
      return SemanticType.FIELD;
    case 'timestamp':
      return SemanticType.TIMESTAMP;
  }
}

export function toProtoDataType(t: DataType): ColumnDataType {
  switch (t) {
    case DataType.Int8:
      return ColumnDataType.INT8;
    case DataType.Int16:
      return ColumnDataType.INT16;
    case DataType.Int32:
      return ColumnDataType.INT32;
    case DataType.Int64:
      return ColumnDataType.INT64;
    case DataType.Uint8:
      return ColumnDataType.UINT8;
    case DataType.Uint16:
      return ColumnDataType.UINT16;
    case DataType.Uint32:
      return ColumnDataType.UINT32;
    case DataType.Uint64:
      return ColumnDataType.UINT64;
    case DataType.Float32:
      return ColumnDataType.FLOAT32;
    case DataType.Float64:
      return ColumnDataType.FLOAT64;
    case DataType.Bool:
      return ColumnDataType.BOOLEAN;
    case DataType.String:
      return ColumnDataType.STRING;
    case DataType.Binary:
      return ColumnDataType.BINARY;
    case DataType.Date:
      return ColumnDataType.DATE;
    // eslint-disable-next-line @typescript-eslint/no-deprecated -- switch must still map the legacy enum
    case DataType.Datetime:
      return ColumnDataType.DATETIME;
    case DataType.TimestampSecond:
      return ColumnDataType.TIMESTAMP_SECOND;
    case DataType.TimestampMillisecond:
      return ColumnDataType.TIMESTAMP_MILLISECOND;
    case DataType.TimestampMicrosecond:
      return ColumnDataType.TIMESTAMP_MICROSECOND;
    case DataType.TimestampNanosecond:
      return ColumnDataType.TIMESTAMP_NANOSECOND;
    case DataType.TimeSecond:
      return ColumnDataType.TIME_SECOND;
    case DataType.TimeMillisecond:
      return ColumnDataType.TIME_MILLISECOND;
    case DataType.TimeMicrosecond:
      return ColumnDataType.TIME_MICROSECOND;
    case DataType.TimeNanosecond:
      return ColumnDataType.TIME_NANOSECOND;
    case DataType.Json:
      return ColumnDataType.JSON;
  }
}
