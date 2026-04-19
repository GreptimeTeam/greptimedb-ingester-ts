import { describe, expect, it } from 'vitest';
import { DataType, Precision, SchemaError } from '../../src/index.js';
import { field, getMetadata, tableName, tag, timestamp } from '../../src/decorators/index.js';
import { objectsToTables } from '../../src/decorators/object-mapper.js';

describe('decorators', () => {
  @tableName('cpu_usage')
  class CpuMetric {
    @tag(DataType.String) host!: string;
    @field(DataType.Float64) usage!: number;
    @timestamp({ precision: Precision.Millisecond }) ts!: number | Date;
  }

  it('registers metadata once per class despite multiple instances', () => {
    const a = new CpuMetric();
    const b = new CpuMetric();
    // Touch fields to ensure instantiation
    a.host = 'h1';
    b.host = 'h2';
    const m = getMetadata(CpuMetric);
    expect(m?.tableName).toBe('cpu_usage');
    expect(m?.columns.map((c) => c.columnName)).toEqual(['host', 'usage', 'ts']);
    expect(m?.columns.map((c) => c.semantic)).toEqual(['tag', 'field', 'timestamp']);
  });

  it('objectsToTables groups by class and builds a Table', () => {
    const m1 = Object.assign(new CpuMetric(), { host: 'server-01', usage: 10, ts: 1 });
    const m2 = Object.assign(new CpuMetric(), { host: 'server-02', usage: 20, ts: 2 });
    const tables = objectsToTables([m1, m2]);
    expect(tables).toHaveLength(1);
    const t = tables[0]!;
    expect(t.tableName()).toBe('cpu_usage');
    expect(t.rowCount()).toBe(2);
    expect(t.columns().map((c) => c.name)).toEqual(['host', 'usage', 'ts']);
  });

  it('uses lowercased class name when @tableName is omitted', () => {
    class MemoryStat {
      @tag(DataType.String) host!: string;
      @field(DataType.Int64) bytes!: bigint;
      @timestamp() ts!: number;
    }
    const m = Object.assign(new MemoryStat(), { host: 'h', bytes: 1n, ts: 1 });
    const [t] = objectsToTables([m]);
    expect(t?.tableName()).toBe('memorystat');
  });

  it('respects the column option to rename the DB column', () => {
    class Metric {
      @tag(DataType.String, { column: 'device_id' }) deviceId!: string;
      @field(DataType.Float64) value!: number;
      @timestamp() ts!: number;
    }
    const inst = Object.assign(new Metric(), { deviceId: 'x', value: 1, ts: 1 });
    const [t] = objectsToTables([inst]);
    expect(t?.columns().map((c) => c.name)).toEqual(['device_id', 'value', 'ts']);
  });

  it('throws when a class lacks column decorators', () => {
    expect(() => objectsToTables([{ plain: 1 }])).toThrow(SchemaError);
  });
});
