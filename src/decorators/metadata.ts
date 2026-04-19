// Per-class metadata for the decorator-driven object mapper.
//
// Stage-3 decorators (TS 5, no experimentalDecorators) don't emit type metadata, so type
// info has to be passed explicitly via decorator arguments. We store it in a module-level
// WeakMap keyed by the class constructor. `addInitializer` fires per-instance; the
// `_registered` Set on each metadata object dedups across instances.

import type { DataType, Precision, Semantic } from '../table/data-type.js';

export interface ColumnMetadata {
  readonly fieldName: string;
  readonly columnName: string;
  readonly dataType: DataType;
  readonly semantic: Semantic;
  readonly precision?: Precision;
}

export interface ClassMetadata {
  tableName?: string;
  readonly columns: ColumnMetadata[];
  readonly _registered: Set<string>;
}

const REGISTRY = new WeakMap<object, ClassMetadata>();

export function getOrCreate(ctor: object): ClassMetadata {
  let m = REGISTRY.get(ctor);
  if (m === undefined) {
    m = { columns: [], _registered: new Set<string>() };
    REGISTRY.set(ctor, m);
  }
  return m;
}

export function getMetadata(ctor: object): ClassMetadata | undefined {
  return REGISTRY.get(ctor);
}

export function registerColumn(ctor: object, column: ColumnMetadata): void {
  const m = getOrCreate(ctor);
  if (m._registered.has(column.fieldName)) return;
  m._registered.add(column.fieldName);
  m.columns.push(column);
}
