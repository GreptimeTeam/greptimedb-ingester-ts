// Unary Client.write() path — builds a GreptimeRequest, sends it via the Handle RPC,
// and unpacks the AffectedRows response.

import { create } from '@bufbuild/protobuf';

import { buildHintsMetadata, buildRequestHeader } from '../auth.js';
import type { ClientConfig } from '../config.js';
import {
  GreptimeRequestSchema,
  type GreptimeRequest,
} from '../generated/greptime/v1/database_pb.js';
import { HandleMethod } from '../transport/grpc-services.js';
import type { Channel } from '../transport/channel.js';
import { unaryCall } from '../transport/promise-adapter.js';
import type { Table } from '../table/table.js';
import { encodeTables } from './encode.js';
import type { AffectedRows } from './affected-rows.js';
import { extractAffected } from './response.js';
import type { WriteOptions } from './request.js';

export type { WriteOptions } from './request.js';

export async function performUnaryWrite(
  channel: Channel,
  cfg: ClientConfig,
  tables: readonly Table[],
  opts: WriteOptions | undefined,
): Promise<AffectedRows> {
  const header = buildRequestHeader(cfg);
  const rowInserts = encodeTables(tables);
  const request: GreptimeRequest = create(GreptimeRequestSchema, {
    header,
    request: { case: 'rowInserts', value: rowInserts },
  });
  const res = await unaryCall(channel.unwrap(), HandleMethod, request, {
    metadata: buildHintsMetadata(opts?.hints),
    deadlineMs: opts?.timeoutMs ?? cfg.timeoutMs,
    ...(opts?.signal !== undefined && { signal: opts.signal }),
  });
  return extractAffected(res);
}
