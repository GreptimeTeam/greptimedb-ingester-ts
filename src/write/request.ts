import { create } from '@bufbuild/protobuf';
import {
  AuthHeaderSchema,
  BasicSchema,
  RequestHeaderSchema,
  type AuthHeader,
  type RequestHeader,
} from '../generated/greptime/v1/common_pb.js';
import type { AuthConfig } from '../config-shared.js';
import { ValueError } from '../errors.js';

export interface WriteOptions {
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
  readonly hints?: Record<string, string>;
}

function buildAuthHeader(auth: AuthConfig): AuthHeader {
  const basic = create(BasicSchema, { username: auth.username, password: auth.password });
  return create(AuthHeaderSchema, {
    authScheme: { case: 'basic', value: basic },
  });
}

/** Build a proto `RequestHeader` for unary / streaming (HandleRequests) calls. */
export function buildRequestHeader(
  cfg: { database: string; auth?: AuthConfig },
  extras?: { tracingContext?: Record<string, string>; timezone?: string },
): RequestHeader {
  const header = create(RequestHeaderSchema, {
    dbname: cfg.database,
    ...(extras?.timezone !== undefined && { timezone: extras.timezone }),
    ...(extras?.tracingContext !== undefined && { tracingContext: extras.tracingContext }),
  });
  if (cfg.auth !== undefined) {
    header.authorization = buildAuthHeader(cfg.auth);
  }
  return header;
}

export function encodeHints(hints?: Record<string, string>): string | undefined {
  if (hints === undefined) return undefined;
  const entries = Object.entries(hints);
  if (entries.length === 0) return undefined;
  const parts: string[] = [];
  for (const [k, v] of entries) {
    if (k.length === 0) {
      throw new ValueError('hint key must not be empty');
    }
    if (k.includes(',') || k.includes('=')) {
      throw new ValueError(`hint key "${k}" contains illegal character ',' or '='`);
    }
    if (v.includes(',') || v.includes('=')) {
      throw new ValueError(`hint value for "${k}" contains illegal character ',' or '='`);
    }
    parts.push(`${k}=${v}`);
  }
  return parts.join(',');
}
