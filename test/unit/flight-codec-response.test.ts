// Regression: `parseDoPutResponse` used to cast JSON.parse output unchecked. A
// malformed ack (wrong types, missing fields, non-JSON) would reach
// RequestTracker.resolve() with an invalid id, silently miss the pending entry,
// and hang the real request forever. Every malformed shape must throw BulkError
// so the drain loop can surface a real error.

import { describe, expect, it } from 'vitest';

import { BulkError } from '../../src/index.js';
import { parseDoPutResponse } from '../../src/bulk/flight-codec.js';

const encode = (o: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(o));

describe('parseDoPutResponse — shape validation', () => {
  it('accepts a well-formed ack', () => {
    const out = parseDoPutResponse(encode({ request_id: 1, affected_rows: 100 }));
    expect(out).toEqual({ request_id: 1, affected_rows: 100 });
  });

  it('rejects non-JSON bytes', () => {
    expect(() => parseDoPutResponse(new TextEncoder().encode('not json'))).toThrow(BulkError);
  });

  it('rejects a JSON array (not an object)', () => {
    expect(() => parseDoPutResponse(encode([1, 2]))).toThrow(BulkError);
  });

  it('rejects missing request_id', () => {
    expect(() => parseDoPutResponse(encode({ affected_rows: 10 }))).toThrow(BulkError);
  });

  it('rejects missing affected_rows', () => {
    expect(() => parseDoPutResponse(encode({ request_id: 1 }))).toThrow(BulkError);
  });

  it('rejects request_id as string', () => {
    expect(() => parseDoPutResponse(encode({ request_id: '1', affected_rows: 1 }))).toThrow(
      BulkError,
    );
  });

  it('rejects null in numeric fields', () => {
    // JSON syntax has no NaN / Infinity — they serialize to `null` on the wire.
    // `typeof null === 'object'`, so the number-type guard rejects it.
    const raw = new TextEncoder().encode('{"request_id":1,"affected_rows":null}');
    expect(() => parseDoPutResponse(raw)).toThrow(BulkError);
  });

  // Regression P2: fractional / non-integer / out-of-safe-range numeric fields.
  // Without strict integer validation, a fractional request_id (e.g. 1.5) would
  // miss every key in RequestTracker (which only allocates integers) and the real
  // write would hang forever; a fractional affected_rows would corrupt totals.
  it('rejects fractional request_id', () => {
    expect(() => parseDoPutResponse(encode({ request_id: 1.5, affected_rows: 1 }))).toThrow(
      BulkError,
    );
  });

  it('rejects fractional affected_rows', () => {
    expect(() => parseDoPutResponse(encode({ request_id: 1, affected_rows: 1.5 }))).toThrow(
      BulkError,
    );
  });

  it('rejects negative request_id', () => {
    expect(() => parseDoPutResponse(encode({ request_id: -1, affected_rows: 1 }))).toThrow(
      BulkError,
    );
  });

  it('rejects negative affected_rows', () => {
    expect(() => parseDoPutResponse(encode({ request_id: 1, affected_rows: -5 }))).toThrow(
      BulkError,
    );
  });

  it('rejects request_id beyond MAX_SAFE_INTEGER', () => {
    // JSON.stringify emits these as full digit strings; JSON.parse restores them
    // as floats that lose precision. isSafeInteger rules them out.
    const raw = new TextEncoder().encode(
      '{"request_id":9007199254740993,"affected_rows":1}', // 2^53 + 1
    );
    expect(() => parseDoPutResponse(raw)).toThrow(BulkError);
  });

  it('rejects empty body', () => {
    expect(() => parseDoPutResponse(new Uint8Array(0))).toThrow(BulkError);
  });
});
