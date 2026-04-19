import { describe, expect, it } from 'vitest';
import { DataType, Precision, SchemaError, Table } from '../../src/index.js';
import { encodeTable } from '../../src/write/encode.js';

describe('Table builder', () => {
  const build = (): Table =>
    Table.new('metrics')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('usage', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond);

  it('builds a schema with tag, field, and timestamp columns', () => {
    const t = build();
    const s = t.schema();
    expect(s.tableName).toBe('metrics');
    expect(s.columns.map((c) => c.name)).toEqual(['host', 'usage', 'ts']);
    expect(s.columns.map((c) => c.semantic)).toEqual(['tag', 'field', 'timestamp']);
  });

  it('rejects duplicate column names', () => {
    expect(() =>
      Table.new('metrics')
        .addTagColumn('host', DataType.String)
        .addFieldColumn('host', DataType.Float64),
    ).toThrow(SchemaError);
  });

  it('rejects tables without a timestamp column', () => {
    expect(() => Table.new('metrics').addTagColumn('host', DataType.String).schema()).toThrow(
      SchemaError,
    );
  });

  it('rejects row length mismatch', () => {
    const t = build();
    expect(() => t.addRow(['h1', 1.5])).toThrow(SchemaError);
  });

  it('freezes schema after first row', () => {
    const t = build();
    t.addRow(['h1', 1.5, 1_700_000_000_000]);
    expect(() => t.addFieldColumn('extra', DataType.Int32)).toThrow(SchemaError);
  });

  it('accepts addRowObject and fills missing fields with null', () => {
    const t = build();
    t.addRowObject({ host: 'h1', ts: 1_700_000_000_000 });
    const req = encodeTable(t);
    expect(req.rows?.rows.length).toBe(1);
    const row0 = req.rows?.rows[0];
    expect(row0?.values.length).toBe(3);
    // usage is missing → empty valueData
    expect(row0?.values[1]?.valueData.case).toBeUndefined();
    expect(row0?.values[0]?.valueData.case).toBe('stringValue');
  });

  it('rejects unknown column name in addRowObject', () => {
    const t = build();
    expect(() => t.addRowObject({ not_a_column: 1, ts: 1 })).toThrow(SchemaError);
  });
});

describe('encodeTable', () => {
  it('produces a RowInsertRequest with the expected proto shape', () => {
    const t = Table.new('cpu')
      .addTagColumn('host', DataType.String)
      .addFieldColumn('usage', DataType.Float64)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['server-01', 75.3, 1_700_000_000_000])
      .addRow(['server-02', 82.1, 1_700_000_000_001]);

    const req = encodeTable(t);
    expect(req.tableName).toBe('cpu');
    expect(req.rows?.schema.length).toBe(3);
    expect(req.rows?.schema[0]?.columnName).toBe('host');
    expect(req.rows?.rows.length).toBe(2);
    const values0 = req.rows?.rows[0]?.values;
    expect(values0?.[0]?.valueData).toEqual({ case: 'stringValue', value: 'server-01' });
    expect(values0?.[1]?.valueData).toEqual({ case: 'f64Value', value: 75.3 });
    expect(values0?.[2]?.valueData).toEqual({
      case: 'timestampMillisecondValue',
      value: 1_700_000_000_000n,
    });
  });
});
