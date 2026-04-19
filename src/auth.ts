// Two auth/context paths — unary/streaming go via proto RequestHeader; bulk via gRPC metadata.
// See plan §"认证与上下文分两条通路".

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

function buildAuthHeader(auth: AuthConfig): AuthHeader {
  if (auth.kind === 'basic') {
    const basic = create(BasicSchema, { username: auth.username, password: auth.password });
    return create(AuthHeaderSchema, {
      authScheme: { case: 'basic', value: basic },
    });
  }
  return create(AuthHeaderSchema, {
    authScheme: { case: 'token', value: { token: auth.token } },
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
  if (auth.kind === 'basic') {
    const encoded = Buffer.from(`${auth.username}:${auth.password}`, 'utf8').toString('base64');
    return `Basic ${encoded}`;
  }
  return `Bearer ${auth.token}`;
}
