import { ServerError, TransportError } from '../errors.js';
import type { GreptimeResponse } from '../generated/greptime/v1/database_pb.js';
import type { AffectedRows } from './affected-rows.js';

export function extractAffected(res: GreptimeResponse): AffectedRows {
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
