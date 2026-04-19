// Stage-3 decorators (TS 5+, no experimentalDecorators). Each field decorator receives
// the class field decorator context and registers a column in the per-class metadata
// registry the first time any instance is constructed.

import { Precision as PrecisionValue, precisionToTimestampDataType } from '../table/data-type.js';
import type { DataType, Precision } from '../table/data-type.js';
import { getOrCreate, registerColumn } from './metadata.js';

type FieldDecorator = (value: undefined, context: ClassFieldDecoratorContext) => void;

export interface ColumnOptions {
  /** Override the DB column name. Defaults to the JS field name. */
  readonly column?: string;
}

export interface TimestampOptions extends ColumnOptions {
  /** Override the timestamp precision. Default: Millisecond. */
  readonly precision?: Precision;
}

/** Attach a table name to the class. */
export function tableName<TClass extends abstract new (...args: never[]) => unknown>(
  name: string,
): (target: TClass, context: ClassDecoratorContext<TClass>) => void {
  return (target, _ctx) => {
    const m = getOrCreate(target);
    m.tableName = name;
  };
}

/** Mark a class field as a TAG column of the given `DataType`. */
export function tag(dataType: DataType, opts: ColumnOptions = {}): FieldDecorator {
  return (_v, ctx) => {
    const fieldName = String(ctx.name);
    const columnName = opts.column ?? fieldName;
    ctx.addInitializer(function (this: unknown) {
      const ctor = (this as { constructor: object }).constructor;
      registerColumn(ctor, { fieldName, columnName, dataType, semantic: 'tag' });
    });
  };
}

/** Mark a class field as a FIELD column of the given `DataType`. */
export function field(dataType: DataType, opts: ColumnOptions = {}): FieldDecorator {
  return (_v, ctx) => {
    const fieldName = String(ctx.name);
    const columnName = opts.column ?? fieldName;
    ctx.addInitializer(function (this: unknown) {
      const ctor = (this as { constructor: object }).constructor;
      registerColumn(ctor, { fieldName, columnName, dataType, semantic: 'field' });
    });
  };
}

/** Mark a class field as the TIMESTAMP column. Exactly one per class. */
export function timestamp(opts: TimestampOptions = {}): FieldDecorator {
  const precision = opts.precision ?? PrecisionValue.Millisecond;
  const dataType = precisionToTimestampDataType(precision);
  return (_v, ctx) => {
    const fieldName = String(ctx.name);
    const columnName = opts.column ?? fieldName;
    ctx.addInitializer(function (this: unknown) {
      const ctor = (this as { constructor: object }).constructor;
      registerColumn(ctor, {
        fieldName,
        columnName,
        dataType,
        semantic: 'timestamp',
        precision,
      });
    });
  };
}

export { getMetadata, type ClassMetadata, type ColumnMetadata } from './metadata.js';
