// Encode Tables into proto `RowInsertRequests`. Shared by the unary and streaming paths.

import { create, type MessageInitShape } from '@bufbuild/protobuf';
import {
  ColumnSchemaSchema,
  RowSchema,
  RowsSchema,
  type ColumnSchema,
  type Row,
  type Rows,
} from '../generated/greptime/v1/row_pb.js';
import {
  RowInsertRequestSchema,
  RowInsertRequestsSchema,
  type RowInsertRequest,
  type RowInsertRequests,
} from '../generated/greptime/v1/database_pb.js';
import {
  ColumnDataType,
  ColumnDataTypeExtensionSchema,
  ColumnOptionsSchema,
  DecimalTypeExtensionSchema,
  JsonNativeTypeExtensionSchema,
} from '../generated/greptime/v1/common_pb.js';
import { SchemaError } from '../errors.js';
import { DataType, toProtoDataType, toProtoSemanticType } from '../table/data-type.js';
import { toProtoValue } from '../table/value.js';
import type { ColumnSpec } from '../table/schema.js';
import type { Table } from '../table/table.js';

// Same column metadata as a SQL-declared JSON2 column, so auto-create builds a JSON2 column
// (matches the Java and Rust ingesters).
const JSON2_COLUMN_OPTIONS = {
  'ARROW:extension:name': 'greptime.json2',
  'ARROW:extension:metadata':
    '{"json_settings":{"type_hints":[],"max_auto_expanded_paths":100},"layout_version":2}',
};

function columnTypeExtras(
  spec: ColumnSpec,
): Pick<MessageInitShape<typeof ColumnSchemaSchema>, 'datatypeExtension' | 'options'> {
  if (spec.dataType === DataType.Decimal128 && spec.decimal) {
    return {
      datatypeExtension: create(ColumnDataTypeExtensionSchema, {
        typeExt: {
          case: 'decimalType',
          value: create(DecimalTypeExtensionSchema, {
            precision: spec.decimal.precision,
            scale: spec.decimal.scale,
          }),
        },
      }),
    };
  }
  if (spec.dataType === DataType.Json2) {
    return {
      datatypeExtension: create(ColumnDataTypeExtensionSchema, {
        typeExt: {
          case: 'jsonNativeType',
          value: create(JsonNativeTypeExtensionSchema, { datatype: ColumnDataType.JSON }),
        },
      }),
      options: create(ColumnOptionsSchema, { options: JSON2_COLUMN_OPTIONS }),
    };
  }
  return {};
}

export function encodeTable(table: Table): RowInsertRequest {
  const cols = table.columns();
  const columnSchemas: ColumnSchema[] = cols.map((spec) =>
    create(ColumnSchemaSchema, {
      columnName: spec.name,
      datatype: toProtoDataType(spec.dataType),
      semanticType: toProtoSemanticType(spec.semantic),
      ...columnTypeExtras(spec),
    }),
  );
  const rows: Row[] = table.rows().map((rowValues) => {
    const values = rowValues.map((v, colIdx) => {
      const spec = cols[colIdx];
      if (spec === undefined) {
        throw new SchemaError(`internal: row has more values than columns (col ${colIdx})`);
      }
      return toProtoValue(v, spec.dataType, spec.decimal);
    });
    return create(RowSchema, { values });
  });
  const rowsMsg: Rows = create(RowsSchema, { schema: columnSchemas, rows });
  return create(RowInsertRequestSchema, {
    tableName: table.tableName(),
    rows: rowsMsg,
  });
}

export function encodeTables(tables: readonly Table[]): RowInsertRequests {
  const inserts = tables.map(encodeTable);
  return create(RowInsertRequestsSchema, { inserts });
}
