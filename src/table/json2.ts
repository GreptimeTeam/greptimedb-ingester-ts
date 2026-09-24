// JSON text → proto `JsonValue` for JSON2 columns.
//
// A hand-written strict parser instead of `JSON.parse`: JSON2 stores int64/uint64/float64
// as distinct types, so numbers are classified by their source lexeme (`1` is uint, `1.0`
// is float, `18446744073709551615` stays exact). `JSON.parse` loses both the lexeme and
// integer precision beyond 2^53. The classification matches the Java and Rust ingesters.

import type { JsonObject_Entry, JsonValue } from '../generated/greptime/v1/row_pb.js';
import { ValueError } from '../errors.js';
import { I64_MIN, U64_MAX } from './validators.js';

// Same default as serde_json, which the Rust ingester uses.
const MAX_DEPTH = 128;

// Messages are built as plain objects instead of via `create()`: protobuf-es v2 messages are
// plain objects, every field here is set explicitly, and `create()` was ~70% of parse time.
function jsonValue(value: JsonValue['value']): JsonValue {
  return { $typeName: 'greptime.v1.JsonValue', value };
}

const NUMBER_RE = /-?(?:0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?/y;

/**
 * Parse a JSON2 payload. Returns `undefined` for JSON `null` (SQL NULL); throws
 * `ValueError` for invalid JSON or a top-level value that is not an object.
 */
export function parseJson2(text: string): JsonValue | undefined {
  const parser = new Parser(text);
  parser.skipWhitespace();
  const value = parser.parseValue(0);
  parser.skipWhitespace();
  if (parser.pos !== text.length) parser.fail('unexpected trailing characters');
  if (value.value.case === undefined) return undefined;
  if (value.value.case !== 'object') {
    throw new ValueError('Json2 expected a JSON object or null at the top level');
  }
  return value;
}

class Parser {
  public pos = 0;

  public constructor(private readonly text: string) {}

  public fail(reason: string): never {
    throw new ValueError(`Json2 invalid JSON at position ${this.pos}: ${reason}`);
  }

  public skipWhitespace(): void {
    const s = this.text;
    while (this.pos < s.length) {
      const c = s.charCodeAt(this.pos);
      if (c !== 0x20 && c !== 0x0a && c !== 0x0d && c !== 0x09) break;
      this.pos++;
    }
  }

  public parseValue(depth: number): JsonValue {
    const c = this.text[this.pos];
    switch (c) {
      case '{':
        return this.parseObject(depth + 1);
      case '[':
        return this.parseArray(depth + 1);
      case '"':
        return jsonValue({ case: 'str', value: this.parseString() });
      case 't':
        this.expectLiteral('true');
        return jsonValue({ case: 'boolean', value: true });
      case 'f':
        this.expectLiteral('false');
        return jsonValue({ case: 'boolean', value: false });
      case 'n':
        this.expectLiteral('null');
        return jsonValue({ case: undefined });
      default:
        if (c === '-' || (c !== undefined && c >= '0' && c <= '9')) return this.parseNumber();
        return this.fail(c === undefined ? 'unexpected end of input' : `unexpected '${c}'`);
    }
  }

  private parseObject(depth: number): JsonValue {
    if (depth > MAX_DEPTH) this.fail(`nesting deeper than ${MAX_DEPTH}`);
    this.pos++;
    const entries: JsonObject_Entry[] = [];
    this.skipWhitespace();
    if (this.text[this.pos] === '}') {
      this.pos++;
    } else {
      for (;;) {
        if (this.text[this.pos] !== '"') this.fail('expected string key');
        const key = this.parseString();
        this.skipWhitespace();
        this.expectChar(':');
        this.skipWhitespace();
        const value = this.parseValue(depth);
        entries.push({ $typeName: 'greptime.v1.JsonObject.Entry', key, value });
        this.skipWhitespace();
        if (this.text[this.pos] === ',') {
          this.pos++;
          this.skipWhitespace();
          continue;
        }
        this.expectChar('}');
        break;
      }
    }
    return jsonValue({ case: 'object', value: { $typeName: 'greptime.v1.JsonObject', entries } });
  }

  private parseArray(depth: number): JsonValue {
    if (depth > MAX_DEPTH) this.fail(`nesting deeper than ${MAX_DEPTH}`);
    this.pos++;
    const items: JsonValue[] = [];
    this.skipWhitespace();
    if (this.text[this.pos] === ']') {
      this.pos++;
    } else {
      for (;;) {
        items.push(this.parseValue(depth));
        this.skipWhitespace();
        if (this.text[this.pos] === ',') {
          this.pos++;
          this.skipWhitespace();
          continue;
        }
        this.expectChar(']');
        break;
      }
    }
    return jsonValue({ case: 'array', value: { $typeName: 'greptime.v1.JsonList', items } });
  }

  private parseString(): string {
    const s = this.text;
    const start = this.pos;
    let escaped = false;
    let i = start + 1;
    for (;;) {
      if (i >= s.length) {
        this.pos = i;
        this.fail('unterminated string');
      }
      const c = s.charCodeAt(i);
      if (c === 0x22) break;
      if (c === 0x5c) {
        escaped = true;
        i += 2;
        continue;
      }
      if (c < 0x20) {
        this.pos = i;
        this.fail('unescaped control character in string');
      }
      i++;
    }
    this.pos = i + 1;
    if (!escaped) return s.slice(start + 1, i);
    // Escape decoding and validation are delegated to the native parser.
    try {
      return JSON.parse(s.slice(start, i + 1)) as string;
    } catch {
      this.pos = start;
      return this.fail('invalid escape sequence in string');
    }
  }

  private parseNumber(): JsonValue {
    NUMBER_RE.lastIndex = this.pos;
    const m = NUMBER_RE.exec(this.text);
    if (m === null) return this.fail('invalid number');
    const lexeme = m[0];
    this.pos += lexeme.length;
    if (m[1] === undefined && m[2] === undefined) {
      const n = BigInt(lexeme);
      if (n >= 0n && n <= U64_MAX) {
        return jsonValue({ case: 'uint', value: n });
      }
      if (n < 0n && n >= I64_MIN) {
        return jsonValue({ case: 'int', value: n });
      }
    }
    const f = Number(lexeme);
    if (!Number.isFinite(f)) {
      this.pos -= lexeme.length;
      this.fail(`number ${lexeme} is out of float64 range`);
    }
    return jsonValue({ case: 'float', value: f });
  }

  private expectLiteral(literal: string): void {
    if (!this.text.startsWith(literal, this.pos)) this.fail(`expected '${literal}'`);
    this.pos += literal.length;
  }

  private expectChar(c: string): void {
    const got = this.text[this.pos];
    if (got !== c) {
      this.fail(
        got === undefined ? `expected '${c}', got end of input` : `expected '${c}', got '${got}'`,
      );
    }
    this.pos++;
  }
}
