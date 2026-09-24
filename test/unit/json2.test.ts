import { toJson } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { DataType, Precision, SchemaError, Table, ValueError } from '../../src/index.js';
import { rowsToArrowTable } from '../../src/bulk/arrow-encoder.js';
import { JsonValueSchema } from '../../src/generated/greptime/v1/row_pb.js';
import { ColumnDataType } from '../../src/generated/greptime/v1/common_pb.js';
import { toProtoValue } from '../../src/table/value.js';
import { encodeTable } from '../../src/write/encode.js';

function encodeJson2(v: unknown): unknown {
  const value = toProtoValue(v, DataType.Json2);
  if (value.valueData.case === undefined) return null;
  if (value.valueData.case !== 'jsonValue') throw new Error(`got ${value.valueData.case}`);
  return toJson(JsonValueSchema, value.valueData.value);
}

function field(value: unknown): unknown {
  return { object: { entries: [{ key: 'v', value }] } };
}

describe('Json2 values', () => {
  it('encodes nested objects, arrays, scalars and JSON null', () => {
    expect(encodeJson2('{"a":[true,"x\\n\\u4f60",null,{},[]],"b":{"c":false}}')).toEqual({
      object: {
        entries: [
          {
            key: 'a',
            value: {
              array: {
                items: [{ boolean: true }, { str: 'x\n你' }, {}, { object: {} }, { array: {} }],
              },
            },
          },
          { key: 'b', value: { object: { entries: [{ key: 'c', value: { boolean: false } }] } } },
        ],
      },
    });
  });

  it('classifies numbers by lexeme like the Java and Rust ingesters', () => {
    expect(encodeJson2('{"v":1}')).toEqual(field({ uint: '1' }));
    expect(encodeJson2('{"v":-0}')).toEqual(field({ uint: '0' }));
    expect(encodeJson2('{"v":1.0}')).toEqual(field({ float: 1 }));
    expect(encodeJson2('{"v":1e2}')).toEqual(field({ float: 100 }));
    expect(encodeJson2('{"v":18446744073709551615}')).toEqual(
      field({ uint: '18446744073709551615' }),
    );
    expect(encodeJson2('{"v":-9223372036854775808}')).toEqual(
      field({ int: '-9223372036854775808' }),
    );
    expect(encodeJson2('{"v":18446744073709551616}')).toEqual(
      field({ float: 18446744073709552000 }),
    );
    expect(encodeJson2('{"v":-9223372036854775809}')).toEqual(
      field({ float: -9223372036854776000 }),
    );
  });

  it('treats null, undefined and the JSON text "null" as SQL NULL', () => {
    expect(encodeJson2(null)).toBeNull();
    expect(encodeJson2(undefined)).toBeNull();
    expect(encodeJson2(' null ')).toBeNull();
  });

  it('serializes non-string values with JSON.stringify semantics', () => {
    expect(encodeJson2({ v: 1.5, d: new Date(0), skip: undefined })).toEqual({
      object: {
        entries: [
          { key: 'v', value: { float: 1.5 } },
          { key: 'd', value: { str: '1970-01-01T00:00:00.000Z' } },
        ],
      },
    });
    expect(() => encodeJson2({ v: 1n })).toThrow(ValueError);
  });

  it('rejects non-object top-level values', () => {
    for (const v of ['[]', '1', 'true', '"text"', [1], 1, true]) {
      expect(() => encodeJson2(v)).toThrow(ValueError);
    }
  });

  it('rejects invalid JSON', () => {
    for (const v of [
      '',
      '{',
      '{"a":1} trailing',
      '{"a":1,}',
      '{a:1}',
      "{'a':1}",
      '{"a":01}',
      '{"a":1.}',
      '{"a":-}',
      '{"a":NaN}',
      '{"a":1e400}',
      '{"a":"\\x"}',
      '{"a":"\t"}',
      '{"a":"unterminated}',
      '{"a":tru}',
    ]) {
      expect(() => encodeJson2(v), v).toThrow(ValueError);
    }
  });

  it('rejects nesting deeper than 128 without overflowing the stack', () => {
    const ok = '{"a":'.repeat(128) + 'null' + '}'.repeat(128);
    expect(() => encodeJson2(ok)).not.toThrow();
    const deep = '{"a":'.repeat(129) + 'null' + '}'.repeat(129);
    expect(() => encodeJson2(deep)).toThrow(ValueError);
    expect(() => encodeJson2('{"a":' + '['.repeat(100_000))).toThrow(ValueError);
  });
});

describe('Json2 schema', () => {
  it('ships the JSON2 type extension and column options for auto-create', () => {
    const t = Table.new('t')
      .addFieldColumn('payload', DataType.Json2)
      .addTimestampColumn('ts', Precision.Millisecond)
      .addRow(['{}', 0]);
    const col = encodeTable(t).rows?.schema[0];
    expect(col?.datatype).toBe(ColumnDataType.JSON);
    expect(col?.datatypeExtension?.typeExt.case).toBe('jsonNativeType');
    expect(col?.datatypeExtension?.typeExt.value).toMatchObject({
      datatype: ColumnDataType.JSON,
    });
    expect(col?.options?.options['ARROW:extension:name']).toBe('greptime.json2');
    expect(JSON.parse(col?.options?.options['ARROW:extension:metadata'] ?? '')).toEqual({
      json_settings: { type_hints: [], max_auto_expanded_paths: 100 },
      layout_version: 2,
    });
  });

  it('only allows FIELD columns', () => {
    expect(() => Table.new('t').addTagColumn('payload', DataType.Json2)).toThrow(SchemaError);
  });

  it('is rejected on the bulk Arrow path', () => {
    const schema = {
      tableName: 't',
      columns: [
        { name: 'payload', dataType: DataType.Json2, semantic: 'field' as const },
        { name: 'ts', dataType: DataType.TimestampMillisecond, semantic: 'timestamp' as const },
      ],
    };
    expect(() => rowsToArrowTable(schema, [['{}', 0]])).toThrow(ValueError);
  });
});
