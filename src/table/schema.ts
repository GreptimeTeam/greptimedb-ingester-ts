import { SchemaError } from '../errors.js';
import type { DataType, Precision, Semantic } from './data-type.js';

export interface ColumnSpec {
  readonly name: string;
  readonly dataType: DataType;
  readonly semantic: Semantic;
  readonly precision?: Precision;
}

/**
 * The table schema definition
 */
export interface TableSchema {
  readonly tableName: string;
  readonly columns: readonly ColumnSpec[];
}

export function validateTableSchema(schema: TableSchema): void {
  if (schema.tableName.length === 0) {
    throw new SchemaError('table name must not be empty');
  }
  if (schema.columns.length === 0) {
    throw new SchemaError(`table "${schema.tableName}" has no columns`);
  }
  const seen = new Set<string>();
  let timestampCount = 0;
  for (const c of schema.columns) {
    if (c.name.length === 0) {
      throw new SchemaError(`table "${schema.tableName}" has an empty-name column`);
    }
    if (seen.has(c.name)) {
      throw new SchemaError(`table "${schema.tableName}" has duplicate column "${c.name}"`);
    }
    seen.add(c.name);
    if (c.semantic === 'timestamp') timestampCount++;
  }
  if (timestampCount !== 1) {
    throw new SchemaError(
      `table "${schema.tableName}" must have exactly one timestamp column, got ${timestampCount}`,
    );
  }
}

export function columnIndex(schema: TableSchema, name: string): number {
  const idx = schema.columns.findIndex((c) => c.name === name);
  if (idx < 0) throw new SchemaError(`column "${name}" not in table "${schema.tableName}"`);
  return idx;
}
