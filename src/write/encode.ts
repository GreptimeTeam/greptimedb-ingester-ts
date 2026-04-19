// Encode Tables into proto `RowInsertRequests`. Shared by the unary and streaming paths.

import { create } from '@bufbuild/protobuf';
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
import { toProtoDataType, toProtoSemanticType } from '../table/data-type.js';
import { toProtoValue } from '../table/value.js';
import type { Table } from '../table/table.js';

export function encodeTable(table: Table): RowInsertRequest {
  const cols = table.columns();
  const columnSchemas: ColumnSchema[] = cols.map((spec) =>
    create(ColumnSchemaSchema, {
      columnName: spec.name,
      datatype: toProtoDataType(spec.dataType),
      semanticType: toProtoSemanticType(spec.semantic),
    }),
  );
  const rows: Row[] = table.rows().map((rowValues) => {
    const values = rowValues.map((v, colIdx) => {
      const spec = cols[colIdx];
      if (spec === undefined) {
        throw new Error(`internal: row has more values than columns (col ${colIdx})`);
      }
      return toProtoValue(v, spec.dataType);
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
