/* eslint-disable @typescript-eslint/no-deprecated -- test suite exercises the deprecated Datetime alias */
import { describe, expect, it } from 'vitest';
import {
  Binary,
  Int64,
  TimeMicrosecond,
  TimeMillisecond,
  TimeNanosecond,
  TimeSecond,
  TimestampMicrosecond,
  Uint64,
} from 'apache-arrow';

import { DataType, Precision, Table, ValueError } from '../../src/index.js';
import { rowsToArrowTable } from '../../src/bulk/arrow-encoder.js';

describe('rowsToArrowTable', () => {
  it('maps JSON columns to Arrow Binary for bulk compatibility', () => {
    const table = Table.new('json_bulk')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('payload', DataType.Json)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['a', { ok: true }, 1]);

    const arrowTable = rowsToArrowTable(table.schema(), table.rows());
    expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(Binary);
  });

  it('rejects unsafe number inputs for Int64 in bulk mode', () => {
    const table = Table.new('bulk_i64')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('value', DataType.Int64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['a', Number.MAX_SAFE_INTEGER + 10, 1]);

    expect(() => rowsToArrowTable(table.schema(), table.rows())).toThrow(ValueError);
  });

  it('rejects unsafe number inputs for Uint64 in bulk mode', () => {
    const table = Table.new('bulk_u64')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('value', DataType.Uint64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['a', Number.MAX_SAFE_INTEGER + 10, 1]);

    expect(() => rowsToArrowTable(table.schema(), table.rows())).toThrow(ValueError);
  });

  it('maps DataType.Datetime to Arrow TimestampMicrosecond (alias)', () => {
    const table = Table.new('datetime_schema')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('when', DataType.Datetime)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['a', new Date('2024-01-01T00:00:00Z'), 1]);

    const arrowTable = rowsToArrowTable(table.schema(), table.rows());
    expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(TimestampMicrosecond);
  });

  it('uses Arrow 64-bit types for Int64 and Uint64 columns', () => {
    const table = Table.new('bulk_types')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('signed_value', DataType.Int64)
      .addFieldColumn('unsigned_value', DataType.Uint64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['a', 42n, 43n, 1]);

    const arrowTable = rowsToArrowTable(table.schema(), table.rows());
    expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(Int64);
    expect(arrowTable.schema.fields[2]?.type).toBeInstanceOf(Uint64);
  });

  // Regression: TimeSecond/TimeMillisecond are Int32Array-backed in apache-arrow.
  // Previously `normalizeValue` returned bigint for all four Time* types, and the
  // Int32 setter (`values[i] = bigint`) crashes with a TypeError at encode time.
  describe('Time* column widths map to the correct Arrow typed array', () => {
    it('TimeSecond accepts number and lands in an Int32Array', () => {
      const table = Table.new('time_s')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('t', DataType.TimeSecond)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow(['a', 42, 1]);

      const arrowTable = rowsToArrowTable(table.schema(), table.rows());
      expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(TimeSecond);
      const col = arrowTable.getChildAt(1);
      expect(col?.data[0]?.values).toBeInstanceOf(Int32Array);
      expect((col?.data[0]?.values as Int32Array)[0]).toBe(42);
    });

    it('TimeMillisecond accepts number and lands in an Int32Array', () => {
      const table = Table.new('time_ms')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('t', DataType.TimeMillisecond)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow(['a', 12345, 1]);

      const arrowTable = rowsToArrowTable(table.schema(), table.rows());
      expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(TimeMillisecond);
      const col = arrowTable.getChildAt(1);
      expect(col?.data[0]?.values).toBeInstanceOf(Int32Array);
      expect((col?.data[0]?.values as Int32Array)[0]).toBe(12345);
    });

    it('TimeSecond rejects out-of-Int32 bigint', () => {
      const table = Table.new('time_s_oob')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('t', DataType.TimeSecond)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow(['a', 1n << 40n, 1]);

      expect(() => rowsToArrowTable(table.schema(), table.rows())).toThrow(ValueError);
    });

    it('TimeMicrosecond accepts bigint and lands in a BigInt64Array', () => {
      const table = Table.new('time_us')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('t', DataType.TimeMicrosecond)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow(['a', 1_704_067_200_000_000n, 1]);

      const arrowTable = rowsToArrowTable(table.schema(), table.rows());
      expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(TimeMicrosecond);
      const col = arrowTable.getChildAt(1);
      expect(col?.data[0]?.values).toBeInstanceOf(BigInt64Array);
      expect((col?.data[0]?.values as BigInt64Array)[0]).toBe(1_704_067_200_000_000n);
    });

    it('TimeNanosecond accepts bigint and lands in a BigInt64Array', () => {
      const table = Table.new('time_ns')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('t', DataType.TimeNanosecond)
        .addTimestampColumn('ts', Precision.Millisecond)
        .addRow(['a', 1n, 1]);

      const arrowTable = rowsToArrowTable(table.schema(), table.rows());
      expect(arrowTable.schema.fields[1]?.type).toBeInstanceOf(TimeNanosecond);
      const col = arrowTable.getChildAt(1);
      expect(col?.data[0]?.values).toBeInstanceOf(BigInt64Array);
    });
  });
});
