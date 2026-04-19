import { SchemaError } from '../errors.js';
import { Table } from '../table/table.js';
import { getMetadata, type ClassMetadata } from './metadata.js';

function resolveTableName(ctor: object, m: ClassMetadata): string {
  if (m.tableName !== undefined && m.tableName.length > 0) return m.tableName;
  const name = (ctor as { name?: string }).name;
  if (typeof name === 'string' && name.length > 0) return name.toLowerCase();
  throw new SchemaError('cannot derive table name from anonymous class; use @tableName');
}

/**
 * Turn a list of decorator-annotated instances into one `Table` per distinct class.
 * Rows are grouped by class; instances of the same class share a Table.
 */
export function objectsToTables(instances: readonly object[]): Table[] {
  const byCtor = new Map<object, object[]>();
  for (const inst of instances) {
    const ctor = (inst as { constructor: object }).constructor;
    const bucket = byCtor.get(ctor);
    if (bucket === undefined) byCtor.set(ctor, [inst]);
    else bucket.push(inst);
  }

  const tables: Table[] = [];
  for (const [ctor, rows] of byCtor) {
    const m = getMetadata(ctor);
    if (m === undefined || m.columns.length === 0) {
      throw new SchemaError(
        `class "${(ctor as { name?: string }).name ?? 'anonymous'}" has no column decorators`,
      );
    }
    const tableName = resolveTableName(ctor, m);
    const table = Table.new(tableName);
    for (const col of m.columns) {
      if (col.semantic === 'tag') table.addTagColumn(col.columnName, col.dataType);
      else if (col.semantic === 'field') table.addFieldColumn(col.columnName, col.dataType);
      else table.addTimestampColumn(col.columnName, col.precision);
    }
    for (const row of rows) {
      const values = m.columns.map((col) => (row as Record<string, unknown>)[col.fieldName]);
      table.addRow(values);
    }
    tables.push(table);
  }
  return tables;
}
