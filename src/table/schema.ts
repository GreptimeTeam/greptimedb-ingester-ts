import { SchemaError } from '../errors.js';
import { DataType, type Precision, type Semantic } from './data-type.js';
import { isValidDecimalParams } from './validators.js';

export interface ColumnSpec {
  readonly name: string;
  readonly dataType: DataType;
  readonly semantic: Semantic;
  readonly precision?: Precision;
  /** Required for `DataType.Decimal128`; ignored for every other type. */
  readonly decimal?: DecimalSpec;
}

export interface DecimalSpec {
  /** Total number of significant digits, 1..38. */
  readonly precision: number;
  /** Number of digits to the right of the decimal point, 0..precision. */
  readonly scale: number;
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
    if (c.dataType === DataType.Decimal128) {
      if (c.decimal === undefined) {
        throw new SchemaError(
          `Decimal128 column "${c.name}" requires precision/scale; use addDecimalFieldColumn()`,
        );
      }
      if (!isValidDecimalParams(c.decimal.precision, c.decimal.scale)) {
        throw new SchemaError(
          `Decimal128 column "${c.name}" has invalid precision/scale (precision=${c.decimal.precision}, ` +
            `scale=${c.decimal.scale}); require 1<=precision<=38 and 0<=scale<=precision`,
        );
      }
    } else if (c.decimal !== undefined) {
      throw new SchemaError(
        `column "${c.name}" carries decimal metadata but is not a Decimal128 column`,
      );
    }
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
