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
 * `ValueError` so the SDK error taxonomy is honored. Also rejects values that
 * `JSON.stringify` returns `undefined` for — functions, symbols, and raw
 * `undefined` — because silently treating those as "no output" sends garbage
 * downstream: the unary path would set `stringValue = undefined` on the proto,
 * and the bulk path's `TextEncoder.encode(undefined)` would write the literal
 * ASCII bytes `"undefined"` into the JSON column. Fail loudly instead.
 */
export function safeStringifyJson(v: unknown, name = 'Json'): string {
  if (typeof v === 'string') return v;
  let out: string | undefined;
  try {
    // TS lib.d.ts types JSON.stringify as `: string`, but the runtime returns
    // `undefined` for top-level function/symbol/undefined. Cast to reflect reality.
    out = JSON.stringify(v) as string | undefined;
  } catch (err) {
    throw new ValueError(
      `${name} value could not be JSON-serialized: ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }
  if (out === undefined) {
    throw new ValueError(
      `${name} value is not JSON-serializable (typeof ${typeof v}); ` +
        'functions, symbols, and top-level undefined produce no JSON output',
    );
  }
  return out;
}

/** Common 64-bit range constants. */
export const I64_MIN = -(1n << 63n);
export const I64_MAX = (1n << 63n) - 1n;
export const U64_MIN = 0n;
export const U64_MAX = (1n << 64n) - 1n;

/**
 * Expand an exponential decimal string (e.g. `"1e-7"`, `"1.5E3"`) into plain form. Both
 * `String(1e-7)` and decimal.js `.toString()` can emit exponential notation, which the strict
 * decimal parser would otherwise reject. Pass-through when there is no exponent.
 */
// Upper bound on zero-padding when expanding exponential notation. Any value needing more
// digits than this cannot fit DECIMAL(<=38, <=38), so the cap rejects crafted exponents
// (e.g. "1e1000000000") that would otherwise allocate a multi-GB string before the range
// check could reject them.
const MAX_DECIMAL_EXPANSION = 1024;

function expandExponent(s: string): string {
  // `\d*` (not `\d+`) on the integer part so leading-dot forms like ".5e1" expand too; an
  // empty mantissa ("e5", ".e5") is left for the caller's parser to reject.
  const m = /^([+-]?)(\d*)(?:\.(\d*))?[eE]([+-]?\d+)$/.exec(s);
  if (!m) return s;
  const sign = m[1] ?? '';
  const intPart = m[2] ?? '';
  const fracPart = m[3] ?? '';
  if (intPart === '' && fracPart === '') return s;
  const exp = parseInt(m[4] ?? '0', 10);
  const digits = intPart + fracPart;
  // Position of the decimal point measured from the left, after applying the exponent.
  const pointPos = intPart.length + exp;
  const pad = pointPos <= 0 ? -pointPos : pointPos - digits.length;
  if (pad > MAX_DECIMAL_EXPANSION) {
    throw new ValueError(`Decimal128 exponent in "${s}" is out of range`);
  }
  if (pointPos <= 0) {
    return `${sign}0.${'0'.repeat(-pointPos)}${digits}`;
  }
  if (pointPos >= digits.length) {
    return `${sign}${digits}${'0'.repeat(pointPos - digits.length)}`;
  }
  return `${sign}${digits.slice(0, pointPos)}.${digits.slice(pointPos)}`;
}

/** Decimal128 precision/scale rule: integers with `1<=precision<=38` and `0<=scale<=precision`. */
export function isValidDecimalParams(precision: number, scale: number): boolean {
  return (
    Number.isInteger(precision) &&
    Number.isInteger(scale) &&
    precision >= 1 &&
    precision <= 38 &&
    scale >= 0 &&
    scale <= precision
  );
}

function decimalDigitCount(n: bigint): number {
  const abs = n < 0n ? -n : n;
  return abs === 0n ? 0 : abs.toString().length;
}

/**
 * Convert a decimal value to the unscaled 128-bit integer that GreptimeDB stores for a
 * `DECIMAL(precision, scale)` column (i.e. `round(value * 10^scale)`). Strings are parsed
 * exactly; numbers are stringified via `String(value)` then {@link expandExponent} (a `number`
 * may already have lost precision before reaching us). Excess fractional digits are rounded
 * half-up on the magnitude (away from zero for negatives, matching Java `BigDecimal.HALF_UP`).
 */
export function decimalToUnscaled(
  value: string | number | bigint,
  precision: number,
  scale: number,
): bigint {
  if (!isValidDecimalParams(precision, scale)) {
    throw new ValueError(
      `Decimal128 invalid precision/scale (precision=${precision}, scale=${scale}); require 1<=precision<=38 and 0<=scale<=precision`,
    );
  }

  let s: string;
  if (typeof value === 'bigint') {
    s = value.toString();
  } else if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new ValueError(`Decimal128 expected a finite number, got ${value}`);
    }
    s = expandExponent(String(value));
  } else if (typeof value === 'string') {
    s = expandExponent(value.trim());
  } else {
    throw new ValueError(`Decimal128 expected string|number|bigint, got ${typeof value}`);
  }

  const m = /^([+-]?)(\d*)(?:\.(\d*))?$/.exec(s);
  if (!m || ((m[2] ?? '') === '' && (m[3] ?? '') === '')) {
    throw new ValueError(`Decimal128 cannot parse "${value}" as a decimal`);
  }
  const negative = m[1] === '-';
  const intPart = m[2] ?? '';
  const fracPart = m[3] ?? '';

  let magnitude: bigint;
  if (fracPart.length <= scale) {
    magnitude = BigInt(`${intPart}${fracPart}${'0'.repeat(scale - fracPart.length)}` || '0');
  } else {
    // More fractional digits than the column scale: keep `scale` digits, round half-up
    // on the first dropped digit.
    const kept = fracPart.slice(0, scale);
    const roundDigit = fracPart.charCodeAt(scale) - 48;
    magnitude = BigInt(`${intPart}${kept}` || '0');
    if (roundDigit >= 5) magnitude += 1n;
  }

  const unscaled = negative ? -magnitude : magnitude;
  if (decimalDigitCount(unscaled) > precision) {
    throw new ValueError(
      `Decimal128 value "${value}" exceeds DECIMAL(${precision}, ${scale}) range`,
    );
  }
  return unscaled;
}

/**
 * Split a signed unscaled integer into the proto `Decimal128 { hi, lo }` pair. `hi`/`lo`
 * are the high/low 64 bits of the two's-complement 128-bit representation, each carried as a
 * signed int64 on the wire (the server reinterprets `lo` as unsigned when reconstructing).
 */
export function decimal128Parts(unscaled: bigint): { hi: bigint; lo: bigint } {
  const u128 = BigInt.asUintN(128, unscaled);
  return {
    lo: BigInt.asIntN(64, u128),
    hi: BigInt.asIntN(64, u128 >> 64n),
  };
}

/**
 * Return `Date.getTime()` as a finite number or throw `ValueError`. A `Date`
 * constructed from bad input (e.g. `new Date('not a date')`) has `.getTime()`
 * === NaN, which silently pollutes downstream arithmetic: `BigInt(NaN)` throws
 * a raw `RangeError`, `Math.floor(NaN / x)` yields NaN, and the Arrow typed
 * array then stores 0 or NaN. Checking once here keeps unary and bulk in sync.
 */
export function dateToMs(d: Date, name: string): number {
  const ms = d.getTime();
  if (!Number.isFinite(ms)) {
    throw new ValueError(`${name} received an invalid Date (getTime() = NaN)`);
  }
  return ms;
}
