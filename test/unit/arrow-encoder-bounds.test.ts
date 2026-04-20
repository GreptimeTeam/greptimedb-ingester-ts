// Regression test for a Copilot review comment: bulk `normalizeValue` used to
// validate Int64/Uint64 only via `numberToSafeBigInt` (safe-int range), leaving
// Uint64 open to negative bigints and Int64 open to out-of-range bigints. The
// unary path (`asBigInt` in src/table/value.ts) always enforced 64-bit bounds;
// this test asserts the bulk path now matches.

import { describe, expect, it } from 'vitest';
import { ValueError } from '../../src/errors.js';
import { precomputeArrowSchema, rowsToArrowTable } from '../../src/bulk/arrow-encoder.js';
import { DataType, Table } from '../../src/index.js';

function schemaFor(columnType: DataType) {
  return Table.new('t')
    .addTagColumn('host', DataType.String)
    .addFieldColumn('n', columnType)
    .addTimestampColumn('ts', undefined)
    .schema();
}

describe('bulk normalizeValue — 64-bit range enforcement (matches unary)', () => {
  it('accepts in-range Int64 bigint', () => {
    const s = schemaFor(DataType.Int64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', 0n, 1_700_000_000_000n]], pre)).not.toThrow();
    expect(() => rowsToArrowTable(s, [['h', (1n << 62n), 1n]], pre)).not.toThrow();
  });

  it('rejects Int64 bigint above 2^63-1', () => {
    const s = schemaFor(DataType.Int64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', 1n << 63n, 1n]], pre)).toThrow(ValueError);
  });

  it('rejects Int64 bigint below -2^63', () => {
    const s = schemaFor(DataType.Int64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', -(1n << 63n) - 1n, 1n]], pre)).toThrow(ValueError);
  });

  it('rejects Uint64 negative bigint', () => {
    const s = schemaFor(DataType.Uint64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', -1n, 1n]], pre)).toThrow(ValueError);
  });

  it('rejects Uint64 negative number', () => {
    const s = schemaFor(DataType.Uint64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', -1, 1n]], pre)).toThrow(ValueError);
  });

  it('accepts Uint64 up to 2^64-1', () => {
    const s = schemaFor(DataType.Uint64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', (1n << 64n) - 1n, 1n]], pre)).not.toThrow();
  });

  it('rejects Uint64 above 2^64-1', () => {
    const s = schemaFor(DataType.Uint64);
    const pre = precomputeArrowSchema(s);
    expect(() => rowsToArrowTable(s, [['h', 1n << 64n, 1n]], pre)).toThrow(ValueError);
  });
});
