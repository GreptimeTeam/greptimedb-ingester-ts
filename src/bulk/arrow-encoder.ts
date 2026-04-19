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

const MAX_SAFE_INT = Number.MAX_SAFE_INTEGER;
const MIN_SAFE_INT = Number.MIN_SAFE_INTEGER;

// Exact 64-bit range bounds. Must match the unary path's `asBigInt` bounds in
// src/table/value.ts so bulk and unary reject the same out-of-range values.
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;
const U64_MIN = 0n;
const U64_MAX = (1n << 64n) - 1n;

// Module-level singleton — shared across every JSON / Binary cell encoded on the bulk
// path. With wide tables and large batches this avoids ~1 allocation per cell.
const TEXT_ENCODER = /*@__PURE__*/ new TextEncoder();

function numberToSafeBigInt(v: number, name: string): bigint {
  if (!Number.isFinite(v) || !Number.isInteger(v)) {
    throw new ValueError(`${name} expected integer number, got ${v}`);
  }
  if (v > MAX_SAFE_INT || v < MIN_SAFE_INT) {
    throw new ValueError(
      `${name} received number ${v} outside safe integer range; pass a bigint instead`,
    );
  }
  return BigInt(v);
}

function toBoundedBigInt(v: unknown, name: string, min: bigint, max: bigint): bigint {
  let b: bigint;
  if (typeof v === 'bigint') {
    b = v;
  } else if (typeof v === 'number') {
    b = numberToSafeBigInt(v, name);
  } else {
    throw new ValueError(`${name} expected bigint|number, got ${typeof v}`);
  }
  if (b < min || b > max) {
    throw new ValueError(`${name} value ${b} out of range [${min}, ${max}]`);
  }
  return b;
}

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
  switch (dt) {
    case DataType.Int64:
      return toBoundedBigInt(v, 'Int64', I64_MIN, I64_MAX);
    case DataType.Uint64:
      return toBoundedBigInt(v, 'Uint64', U64_MIN, U64_MAX);
    case DataType.Datetime:
      return v instanceof Date
        ? BigInt(v.getTime())
        : (typeof v === 'number' ? numberToSafeBigInt(v, 'Datetime') : v);
    case DataType.Date:
      return v instanceof Date ? Math.floor(v.getTime() / 86_400_000) : v;
    case DataType.TimestampSecond:
    case DataType.TimestampMillisecond:
    case DataType.TimestampMicrosecond:
    case DataType.TimestampNanosecond:
      return scaleTimestamp(v, dt);
    case DataType.Json:
      return TEXT_ENCODER.encode(typeof v === 'string' ? v : JSON.stringify(v));
    case DataType.Binary:
      if (v instanceof Uint8Array) return v;
      if (typeof v === 'string') return TEXT_ENCODER.encode(v);
      throw new ValueError(`Binary expected Uint8Array|string, got ${typeof v}`);
    default:
      return v;
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
