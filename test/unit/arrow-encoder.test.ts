import { describe, expect, it } from 'vitest';
import { Binary, Int64, Uint64 } from 'apache-arrow';

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
});
