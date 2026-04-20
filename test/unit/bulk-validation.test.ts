// Cross-path parity: identical bad inputs must be rejected identically by unary
// (value.ts:toProtoValue) and bulk (arrow-encoder.ts:rowsToArrowTable). Prior
// to this coverage the bulk path silently forwarded wrong-typed scalars into
// Arrow builders (string -> Int typed array coerced to NaN), creating drift
// between the two write modes.

import { describe, expect, it } from 'vitest';
import { DataType, Precision, Table, ValueError } from '../../src/index.js';
import { toProtoValue } from '../../src/table/value.js';
import { precomputeArrowSchema, rowsToArrowTable } from '../../src/bulk/arrow-encoder.js';
import type { TableSchema } from '../../src/table/schema.js';

function schemaFor(cellType: DataType): TableSchema {
  return Table.new('t')
    .addTagColumn('tag', DataType.String)
    .addFieldColumn('cell', cellType)
    .addTimestampColumn('ts', Precision.Millisecond)
    .schema();
}

function encodeBulk(schema: TableSchema, cellValue: unknown): void {
  const pre = precomputeArrowSchema(schema);
  rowsToArrowTable(schema, [['t', cellValue, 1n]], pre);
}

/** Assert both paths reject `value` for `dataType` with a ValueError. */
function expectBothReject(dataType: DataType, value: unknown): void {
  expect(() => {
    toProtoValue(value, dataType);
  }).toThrow(ValueError);
  expect(() => {
    encodeBulk(schemaFor(dataType), value);
  }).toThrow(ValueError);
}

/** Assert both paths accept `value` for `dataType` without throwing. */
function expectBothAccept(dataType: DataType, value: unknown): void {
  expect(() => {
    toProtoValue(value, dataType);
  }).not.toThrow();
  expect(() => {
    encodeBulk(schemaFor(dataType), value);
  }).not.toThrow();
}

describe('unary / bulk scalar parity', () => {
  describe('Int8', () => {
    it('accepts valid range', () => {
      expectBothAccept(DataType.Int8, -128);
      expectBothAccept(DataType.Int8, 127);
      expectBothAccept(DataType.Int8, 0);
    });
    it('rejects out-of-range', () => {
      expectBothReject(DataType.Int8, 128);
      expectBothReject(DataType.Int8, -129);
    });
    it('rejects non-numeric', () => {
      expectBothReject(DataType.Int8, 'hello');
      expectBothReject(DataType.Int8, true);
    });
  });

  describe('Int16 / Int32', () => {
    it('enforces Int16 bounds', () => {
      expectBothAccept(DataType.Int16, 32767);
      expectBothReject(DataType.Int16, 32768);
      expectBothReject(DataType.Int16, 'x');
    });
    it('enforces Int32 bounds', () => {
      expectBothAccept(DataType.Int32, 2_147_483_647);
      expectBothReject(DataType.Int32, 2_147_483_648);
    });
  });

  describe('Int64 / Uint64', () => {
    it('accepts in-range bigint', () => {
      expectBothAccept(DataType.Int64, 0n);
      expectBothAccept(DataType.Uint64, (1n << 64n) - 1n);
    });
    it('rejects Int64 over 2^63-1', () => {
      expectBothReject(DataType.Int64, 1n << 63n);
    });
    it('rejects Uint64 negative', () => {
      expectBothReject(DataType.Uint64, -1n);
      expectBothReject(DataType.Uint64, -1);
    });
  });

  describe('Uint8 / Uint16 / Uint32', () => {
    it('rejects negative', () => {
      expectBothReject(DataType.Uint8, -1);
      expectBothReject(DataType.Uint16, -1);
      expectBothReject(DataType.Uint32, -1);
    });
    it('rejects out-of-range', () => {
      expectBothReject(DataType.Uint8, 256);
      expectBothReject(DataType.Uint16, 65536);
      expectBothReject(DataType.Uint32, 2 ** 32);
    });
  });

  describe('Float32 / Float64', () => {
    it('accepts numbers and bigints', () => {
      expectBothAccept(DataType.Float64, 1.5);
      expectBothAccept(DataType.Float64, 42n);
    });
    it('rejects non-numeric', () => {
      expectBothReject(DataType.Float64, 'nan');
      expectBothReject(DataType.Float64, true);
    });
  });

  describe('Bool', () => {
    it('accepts boolean', () => {
      expectBothAccept(DataType.Bool, true);
    });
    it('rejects everything else', () => {
      expectBothReject(DataType.Bool, 1);
      expectBothReject(DataType.Bool, 'true');
      expectBothReject(DataType.Bool, 0n);
    });
  });

  describe('String', () => {
    it('accepts string', () => {
      expectBothAccept(DataType.String, 'x');
    });
    it('rejects non-string', () => {
      expectBothReject(DataType.String, 1);
      expectBothReject(DataType.String, true);
      expectBothReject(DataType.String, {});
    });
  });

  describe('Time* precisions', () => {
    it('enforces 64-bit range on TimeMicrosecond bigint', () => {
      expectBothReject(DataType.TimeMicrosecond, 1n << 65n);
    });
    it('rejects non-numeric on TimeSecond', () => {
      expectBothReject(DataType.TimeSecond, 'noon');
    });
  });
});
