// Shared scalar-value validators used by both the unary (`value.ts` → proto `Value`)
// and bulk (`arrow-encoder.ts` → Arrow builder) write paths. Keeping them in one
// place guarantees both paths reject the same bad inputs with identical messages,
// so callers can't depend on the bulk path being more forgiving than unary.

import { ValueError } from '../errors.js';

const MAX_SAFE_INT = Number.MAX_SAFE_INTEGER;
const MIN_SAFE_INT = Number.MIN_SAFE_INTEGER;

const TEXT_ENCODER = /*@__PURE__*/ new TextEncoder();

/**
 * Coerce a JS `number` to a safe `bigint`. Rejects NaN, infinity, non-integers,
 * and values outside the IEEE-754 safe-integer range (where adjacent integers
 * become indistinguishable). The range check is stricter than "i64 fits" because
 * a non-safe `number` can't reliably be round-tripped as an integer.
 */
export function numberToSafeBigInt(v: number, name: string): bigint {
  if (!Number.isFinite(v) || !Number.isInteger(v)) {
    throw new ValueError(`${name} expected integer number, got ${v}`);
  }
  if (v > MAX_SAFE_INT || v < MIN_SAFE_INT) {
    throw new ValueError(
      `${name} received number ${v} outside safe integer range; pass a bigint instead`,
    );
  }
  return BigInt(v);
}

/** Validate a `number | bigint` fits in a small-int range. Returns a `number`. */
export function asIntInRange(name: string, v: unknown, min: number, max: number): number {
  let n: number;
  if (typeof v === 'number') {
    if (!Number.isInteger(v)) throw new ValueError(`${name} expected integer, got ${v}`);
    n = v;
  } else if (typeof v === 'bigint') {
    if (v > BigInt(max) || v < BigInt(min)) {
      throw new ValueError(`${name} bigint ${v} out of range [${min}, ${max}]`);
    }
    n = Number(v);
  } else {
    throw new ValueError(`${name} expected number|bigint, got ${typeof v}`);
  }
  if (n < min || n > max) throw new ValueError(`${name} value ${n} out of range [${min}, ${max}]`);
  return n;
}

/** Validate a `number | bigint` fits in a 64-bit range. Returns a `bigint`. */
export function asBigInt(name: string, v: unknown, min: bigint, max: bigint): bigint {
  let b: bigint;
  if (typeof v === 'bigint') {
    b = v;
  } else if (typeof v === 'number') {
    b = numberToSafeBigInt(v, name);
  } else {
    throw new ValueError(`${name} expected bigint|number, got ${typeof v}`);
  }
  if (b < min || b > max) {
    throw new ValueError(`${name} value ${b} out of range [${min}, ${max}]`);
  }
  return b;
}

/** Validate a numeric column input. Allows both `number` and `bigint`. Returns a `number`. */
export function asNumber(name: string, v: unknown): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'bigint') return Number(v);
  throw new ValueError(`${name} expected number, got ${typeof v}`);
}

export function asBoolean(name: string, v: unknown): boolean {
  if (typeof v === 'boolean') return v;
  throw new ValueError(`${name} expected boolean, got ${typeof v}`);
}

export function asString(name: string, v: unknown): string {
  if (typeof v === 'string') return v;
  throw new ValueError(`${name} expected string, got ${typeof v}`);
}

export function asBinary(v: unknown): Uint8Array {
  if (v instanceof Uint8Array) return v; // Buffer is-a Uint8Array, passes through
  if (typeof v === 'string') return TEXT_ENCODER.encode(v);
  throw new ValueError(`binary expected Uint8Array|Buffer|string, got ${typeof v}`);
}

/**
 * JSON-stringify a value, wrapping the native `TypeError` (bigint/circular) as
 * `ValueError` so the SDK error taxonomy is honored.
 */
export function safeStringifyJson(v: unknown, name = 'Json'): string {
  if (typeof v === 'string') return v;
  try {
    return JSON.stringify(v);
  } catch (err) {
    throw new ValueError(
      `${name} value could not be JSON-serialized: ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }
}

/** Common 64-bit range constants. */
export const I64_MIN = -(1n << 63n);
export const I64_MAX = (1n << 63n) - 1n;
export const U64_MIN = 0n;
export const U64_MAX = (1n << 64n) - 1n;
