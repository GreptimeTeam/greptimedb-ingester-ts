import { Metadata } from '@grpc/grpc-js';
import type { AuthConfig, ClientConfig } from './config.js';
import { encodeHints } from './write/request.js';
export { buildRequestHeader } from './write/request.js';

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
 * Returns a fresh `Metadata` instance every call. A shared singleton is tempting but
 * brittle — it makes correctness depend on grpc-js cloning request metadata before
 * mutation, an internal invariant we shouldn't rely on. The sub-microsecond allocation
 * cost is invisible next to the surrounding gRPC call.
 */
export function buildHintsMetadata(hints?: Record<string, string>): Metadata {
  const md = new Metadata();
  const encoded = encodeHints(hints);
  if (encoded !== undefined) md.set('x-greptime-hints', encoded);
  return md;
}
