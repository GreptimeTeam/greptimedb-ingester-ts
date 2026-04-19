// Two auth/context paths — unary/streaming go via proto RequestHeader; bulk via gRPC metadata.
// Bulk cannot carry RequestHeader, so auth/dbname must travel in transport metadata instead.

import { create } from '@bufbuild/protobuf';
import { Metadata } from '@grpc/grpc-js';

import {
  AuthHeaderSchema,
  BasicSchema,
  RequestHeaderSchema,
  type AuthHeader,
  type RequestHeader,
} from './generated/greptime/v1/common_pb.js';
import type { AuthConfig, ClientConfig } from './config.js';
import { ValueError } from './errors.js';

/**
 * Frozen empty Metadata reused for hint-less calls so the unary/streaming hot paths
 * don't allocate a new Metadata per write. grpc-js clones request metadata before
 * mutation, so sharing this instance is safe.
 */
export const EMPTY_METADATA: Metadata = (() => {
  const md = new Metadata();
  Object.freeze(md);
  return md;
})();

function buildAuthHeader(auth: AuthConfig): AuthHeader {
  // Only `basic` is supported; see AuthConfig docs.
  const basic = create(BasicSchema, { username: auth.username, password: auth.password });
  return create(AuthHeaderSchema, {
    authScheme: { case: 'basic', value: basic },
  });
}

/** Build a proto `RequestHeader` for unary / streaming (HandleRequests) calls. */
export function buildRequestHeader(
  cfg: ClientConfig,
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

/**
 * Build gRPC `Metadata` for the Arrow Flight DoPut bulk path.
 *
 * Arrow Flight messages carry no `RequestHeader`, so dbname + auth travel in transport-level
 * metadata. This matches Rust `src/database.rs:136-153`.
 */
export function buildFlightMetadata(cfg: ClientConfig): Metadata {
  const md = new Metadata();
  md.set('x-greptime-db-name', cfg.database);
  if (cfg.auth !== undefined) {
    md.set('x-greptime-auth', encodeAuthMetadata(cfg.auth));
  }
  return md;
}

function encodeAuthMetadata(auth: AuthConfig): string {
  const encoded = Buffer.from(`${auth.username}:${auth.password}`, 'utf8').toString('base64');
  return `Basic ${encoded}`;
}

/**
 * Build a `Metadata` carrying GreptimeDB hints for the unary/streaming path.
 *
 * Wire format matches Rust `database.rs:198-211`: a single `x-greptime-hints` header
 * with comma-joined `key=value` pairs. The protocol has no escaping mechanism, so any
 * key or value containing `,` or `=` is rejected with `ValueError` — silently mangling
 * such inputs would produce wrong server-side behavior with no diagnostic.
 *
 * Returns the shared frozen `EMPTY_METADATA` when `hints` is undefined or empty so
 * callers don't allocate per write.
 */
export function buildHintsMetadata(hints?: Record<string, string>): Metadata {
  if (hints === undefined) return EMPTY_METADATA;
  const entries = Object.entries(hints);
  if (entries.length === 0) return EMPTY_METADATA;
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
  const md = new Metadata();
  md.set('x-greptime-hints', parts.join(','));
  return md;
}
