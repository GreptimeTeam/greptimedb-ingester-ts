// Unary Client.write() path — builds a GreptimeRequest, sends it via the Handle RPC,
// and unpacks the AffectedRows response.

import { create } from '@bufbuild/protobuf';
import { Metadata } from '@grpc/grpc-js';

import { buildRequestHeader } from '../auth.js';
import type { ClientConfig } from '../config.js';
import { ServerError, TransportError } from '../errors.js';
import {
  GreptimeRequestSchema,
  type GreptimeRequest,
  type GreptimeResponse,
} from '../generated/greptime/v1/database_pb.js';
import { HandleMethod } from '../transport/grpc-services.js';
import type { Channel } from '../transport/channel.js';
import { unaryCall } from '../transport/promise-adapter.js';
import type { Table } from '../table/table.js';
import { encodeTables } from './encode.js';
import type { AffectedRows } from './affected-rows.js';

export interface WriteOptions {
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
  readonly hints?: Record<string, string>;
}

function buildMetadata(hints?: Record<string, string>): Metadata {
  const md = new Metadata();
  if (hints !== undefined) {
    for (const [k, v] of Object.entries(hints)) md.set(`x-greptime-hint-${k}`, v);
  }
  return md;
}

function extractAffected(res: GreptimeResponse): AffectedRows {
  const statusCode = res.header?.status?.statusCode ?? 0;
  if (statusCode !== 0) {
    const msg = res.header?.status?.errMsg ?? `server returned status ${statusCode}`;
    throw new ServerError(msg, statusCode);
  }
  if (res.response.case !== 'affectedRows') {
    throw new TransportError('Handle returned empty response', 2);
  }
  return { value: res.response.value.value };
}

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
    metadata: buildMetadata(opts?.hints),
    deadlineMs: opts?.timeoutMs ?? cfg.timeoutMs,
    ...(opts?.signal !== undefined && { signal: opts.signal }),
  });
  return extractAffected(res);
}
