import { SchemaError } from '../errors.js';
import {
  Precision,
  isTimestampDataType,
  precisionToTimestampDataType,
  type DataType,
} from './data-type.js';
import { validateTableSchema, type ColumnSpec, type TableSchema } from './schema.js';

/**
 * Table builder — accumulates column definitions and rows, then hands off to the writer.
 *
 * Schema is mutable only until the first row is added; after that, further column mutations
 * throw `SchemaError`. This matches Rust's frozen-after-first-row behavior and prevents
 * accidental schema drift between batches.
 */
export class Table {
  private readonly _tableName: string;
  private readonly _columns: ColumnSpec[] = [];
  private readonly _rows: unknown[][] = [];
  private _frozen = false;

  private constructor(tableName: string) {
    if (tableName.length === 0) throw new SchemaError('table name must not be empty');
    this._tableName = tableName;
  }

  public static new(tableName: string): Table {
    return new Table(tableName);
  }

  /** Add a TAG column (part of the row's primary key). */
  public addTagColumn(name: string, dataType: DataType): this {
    return this.addColumn({ name, dataType, semantic: 'tag' });
  }

  /** Add a FIELD column (non-indexed value). */
  public addFieldColumn(name: string, dataType: DataType): this {
    return this.addColumn({ name, dataType, semantic: 'field' });
  }

  /**
   * Add the TIMESTAMP column. Exactly one per table is required. Default precision is
   * millisecond, matching Go's `types.TIMESTAMP_MILLISECOND` default.
   */
  public addTimestampColumn(name: string, precision: Precision = Precision.Millisecond): this {
    const dataType = precisionToTimestampDataType(precision);
    return this.addColumn({ name, dataType, semantic: 'timestamp', precision });
  }

  private addColumn(spec: ColumnSpec): this {
    if (this._frozen) {
      throw new SchemaError(
        `cannot add column "${spec.name}" to "${this._tableName}" after rows have been added`,
      );
    }
    if (spec.semantic === 'timestamp' && !isTimestampDataType(spec.dataType)) {
      throw new SchemaError(
        `timestamp column "${spec.name}" requires a Timestamp* dataType, got ${spec.dataType}`,
      );
    }
    if (this._columns.some((c) => c.name === spec.name)) {
      throw new SchemaError(
        `column "${spec.name}" already exists on table "${this._tableName}"`,
      );
    }
    this._columns.push(spec);
    return this;
  }

  /**
   * Append a row by positional values. `values.length` must equal `columns.length`.
   * `null` / `undefined` encode as proto-null.
   */
  public addRow(values: readonly unknown[]): this {
    this.freezeSchema();
    if (values.length !== this._columns.length) {
      throw new SchemaError(
        `row length ${values.length} does not match column count ${this._columns.length}`,
      );
    }
    this._rows.push([...values]);
    return this;
  }

  /**
   * Append a row by column name. Missing keys become null. Extra keys throw `SchemaError`.
   */
  public addRowObject(row: Readonly<Record<string, unknown>>): this {
    this.freezeSchema();
    const out: unknown[] = new Array(this._columns.length).fill(null) as unknown[];
    const knownNames = new Set(this._columns.map((c) => c.name));
    for (const key of Object.keys(row)) {
      if (!knownNames.has(key)) {
        throw new SchemaError(
          `unknown column "${key}" for table "${this._tableName}"`,
        );
      }
    }
    for (let i = 0; i < this._columns.length; i++) {
      const colSpec = this._columns[i];
      if (colSpec === undefined) continue;
      const v = row[colSpec.name];
      out[i] = v ?? null;
    }
    this._rows.push(out);
    return this;
  }

  public schema(): TableSchema {
    const schema: TableSchema = {
      tableName: this._tableName,
      columns: [...this._columns],
    };
    validateTableSchema(schema);
    return schema;
  }

  public rowCount(): number {
    return this._rows.length;
  }

  public columnCount(): number {
    return this._columns.length;
  }

  public tableName(): string {
    return this._tableName;
  }

  /** Read-only view of accumulated rows, for the writer to consume. */
  public rows(): readonly (readonly unknown[])[] {
    return this._rows;
  }

  public columns(): readonly ColumnSpec[] {
    return this._columns;
  }

  private freezeSchema(): void {
    if (this._frozen) return;
    const schema: TableSchema = { tableName: this._tableName, columns: [...this._columns] };
    validateTableSchema(schema);
    this._frozen = true;
  }
}
