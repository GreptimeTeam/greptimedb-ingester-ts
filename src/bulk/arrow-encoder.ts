// Turn (TableSchema + rows) into an Arrow Table. Column-wise construction via makeBuilder
// avoids the reflective overhead of tableFromJSON. Timestamps accept number | bigint | Date
// with automatic precision scaling; Date → microsecond/nanosecond loses sub-ms precision.

import {
  Binary,
  Bool,
  Field,
  Float32,
  Float64,
  Int16,
  Int32,
  Int64,
  Int8,
  Schema,
  Table as ArrowTable,
  TimeMicrosecond,
  TimeMillisecond,
  TimeNanosecond,
  TimeSecond,
  TimestampMicrosecond,
  TimestampMillisecond,
  TimestampNanosecond,
  TimestampSecond,
  Uint16,
  Uint32,
  Uint64,
  Uint8,
  Utf8,
  makeBuilder,
  type DataType as ArrowDataType,
  type Builder,
  type Vector,
} from 'apache-arrow';

import { ValueError } from '../errors.js';
import { DataType } from '../table/data-type.js';
import type { TableSchema } from '../table/schema.js';
import {
  I64_MAX,
  I64_MIN,
  U64_MAX,
  U64_MIN,
  asBigInt,
  asBinary,
  asBoolean,
  asIntInRange,
  asNumber,
  asString,
  numberToSafeBigInt,
  safeStringifyJson,
} from '../table/validators.js';

// Module-level singleton — shared across every JSON / Binary cell encoded on the bulk
// path. With wide tables and large batches this avoids ~1 allocation per cell.
const TEXT_ENCODER = /*@__PURE__*/ new TextEncoder();

// 32-bit small-int ranges reused for Arrow Int*/Uint* builders. Kept here instead
// of in the shared validators module because they're bulk-local.
const I8_MIN = -(2 ** 7);
const I8_MAX = 2 ** 7 - 1;
const I16_MIN = -(2 ** 15);
const I16_MAX = 2 ** 15 - 1;
const I32_MIN = -(2 ** 31);
const I32_MAX = 2 ** 31 - 1;
const U8_MAX = 2 ** 8 - 1;
const U16_MAX = 2 ** 16 - 1;
const U32_MAX = 2 ** 32 - 1;

function arrowTypeFor(dt: DataType): ArrowDataType {
  switch (dt) {
    case DataType.Int8:
      return new Int8();
    case DataType.Int16:
      return new Int16();
    case DataType.Int32:
      return new Int32();
    case DataType.Int64:
      return new Int64();
    case DataType.Uint8:
      return new Uint8();
    case DataType.Uint16:
      return new Uint16();
    case DataType.Uint32:
      return new Uint32();
    case DataType.Uint64:
      return new Uint64();
    case DataType.Float32:
      return new Float32();
    case DataType.Float64:
      return new Float64();
    case DataType.Bool:
      return new Bool();
    case DataType.String:
      return new Utf8();
    case DataType.Binary:
      return new Binary();
    case DataType.Date:
      return new Int32(); // Arrow DateDay is Int32 days; server-side expects Int32 here.
    case DataType.Datetime:
      return new Int64();
    case DataType.TimestampSecond:
      return new TimestampSecond();
    case DataType.TimestampMillisecond:
      return new TimestampMillisecond();
    case DataType.TimestampMicrosecond:
      return new TimestampMicrosecond();
    case DataType.TimestampNanosecond:
      return new TimestampNanosecond();
    case DataType.TimeSecond:
      return new TimeSecond();
    case DataType.TimeMillisecond:
      return new TimeMillisecond();
    case DataType.TimeMicrosecond:
      return new TimeMicrosecond();
    case DataType.TimeNanosecond:
      return new TimeNanosecond();
    case DataType.Json:
      return new Binary();
  }
}

function scaleTimestamp(v: unknown, dt: DataType): bigint {
  if (v instanceof Date) {
    const ms = BigInt(v.getTime());
    switch (dt) {
      case DataType.TimestampSecond:
        return ms / 1000n;
      case DataType.TimestampMillisecond:
        return ms;
      case DataType.TimestampMicrosecond:
        return ms * 1000n;
      case DataType.TimestampNanosecond:
        return ms * 1_000_000n;
      default:
        throw new ValueError(`unexpected timestamp dt ${dt}`);
    }
  }
  if (typeof v === 'bigint') return v;
  if (typeof v === 'number') {
    return numberToSafeBigInt(v, 'timestamp');
  }
  throw new ValueError(`timestamp expected number|bigint|Date, got ${typeof v}`);
}

function normalizeValue(v: unknown, dt: DataType): unknown {
  if (v === null || v === undefined) return null;
  // Every scalar type funnels through the same validators used by the unary path
  // (`src/table/value.ts`). Keeping validation here — before Arrow builders see
  // the value — prevents Arrow from silently coercing a wrong-typed value (e.g.
  // string -> Int8 typed array = NaN) and produces consistent ValueError messages
  // across the two write paths.
  switch (dt) {
    case DataType.Int8:
      return asIntInRange('Int8', v, I8_MIN, I8_MAX);
    case DataType.Int16:
      return asIntInRange('Int16', v, I16_MIN, I16_MAX);
    case DataType.Int32:
      return asIntInRange('Int32', v, I32_MIN, I32_MAX);
    case DataType.Int64:
      return asBigInt('Int64', v, I64_MIN, I64_MAX);
    case DataType.Uint8:
      return asIntInRange('Uint8', v, 0, U8_MAX);
    case DataType.Uint16:
      return asIntInRange('Uint16', v, 0, U16_MAX);
    case DataType.Uint32:
      return asIntInRange('Uint32', v, 0, U32_MAX);
    case DataType.Uint64:
      return asBigInt('Uint64', v, U64_MIN, U64_MAX);
    case DataType.Float32:
      return asNumber('Float32', v);
    case DataType.Float64:
      return asNumber('Float64', v);
    case DataType.Bool:
      return asBoolean('Bool', v);
    case DataType.String:
      return asString('String', v);
    case DataType.Binary:
      return asBinary(v);
    case DataType.Datetime:
      return v instanceof Date
        ? BigInt(v.getTime())
        : asBigInt('Datetime', v, I64_MIN, I64_MAX);
    case DataType.Date:
      return v instanceof Date
        ? Math.floor(v.getTime() / 86_400_000)
        : asIntInRange('Date', v, I32_MIN, I32_MAX);
    case DataType.TimestampSecond:
    case DataType.TimestampMillisecond:
    case DataType.TimestampMicrosecond:
    case DataType.TimestampNanosecond:
      return scaleTimestamp(v, dt);
    case DataType.TimeSecond:
      return asBigInt('TimeSecond', v, I64_MIN, I64_MAX);
    case DataType.TimeMillisecond:
      return asBigInt('TimeMillisecond', v, I64_MIN, I64_MAX);
    case DataType.TimeMicrosecond:
      return asBigInt('TimeMicrosecond', v, I64_MIN, I64_MAX);
    case DataType.TimeNanosecond:
      return asBigInt('TimeNanosecond', v, I64_MIN, I64_MAX);
    case DataType.Json:
      return TEXT_ENCODER.encode(safeStringifyJson(v));
  }
}

function buildColumnWithType(
  arrowType: ArrowDataType,
  dataType: DataType,
  rows: readonly (readonly unknown[])[],
  colIdx: number,
): Vector {
  const builder: Builder = makeBuilder({ type: arrowType, nullValues: [null, undefined] });
  for (const row of rows) {
    const raw = row[colIdx];
    const v = normalizeValue(raw, dataType);
    builder.append(v);
  }
  builder.finish();
  return builder.toVector();
}

/**
 * Pre-computed Arrow schema artifacts that can be hoisted out of the per-batch hot path.
 * `BulkStreamWriter` builds this once at construction and reuses it across every batch.
 */
export interface PrecomputedArrowSchema {
  readonly arrowSchema: Schema;
  readonly arrowTypes: readonly ArrowDataType[];
}

export function precomputeArrowSchema(schema: TableSchema): PrecomputedArrowSchema {
  const arrowTypes: ArrowDataType[] = schema.columns.map((c) => arrowTypeFor(c.dataType));
  const fields: Field[] = [];
  for (let i = 0; i < schema.columns.length; i++) {
    const column = schema.columns[i];
    const arrowType = arrowTypes[i];
    if (column === undefined || arrowType === undefined) {
      throw new ValueError('schema/arrow type length mismatch');
    }
    fields.push(new Field(column.name, arrowType, true));
  }
  return { arrowSchema: new Schema(fields), arrowTypes };
}

export function rowsToArrowTable(
  schema: TableSchema,
  rows: readonly (readonly unknown[])[],
  precomputed?: PrecomputedArrowSchema,
): ArrowTable {
  const { arrowSchema, arrowTypes } = precomputed ?? precomputeArrowSchema(schema);
  const vectors: Record<string, Vector> = {};
  for (let i = 0; i < arrowTypes.length; i++) {
    const arrowType = arrowTypes[i];
    const column = schema.columns[i];
    if (arrowType === undefined || column === undefined) {
      throw new ValueError('schema/arrow type length mismatch');
    }
    vectors[column.name] = buildColumnWithType(arrowType, column.dataType, rows, i);
  }
  // ArrowTable's constructor signature is variadic-generic over the column-bag shape;
  // a single `Record<string, Vector>` doesn't unify with the rest-parameter inference.
  // The `as never` is a type-only escape hatch — runtime contract is unchanged.
  return new ArrowTable(arrowSchema, vectors as never);
}
