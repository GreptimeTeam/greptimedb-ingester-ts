// Injects Arrow IPC BodyCompression into a RecordBatch message produced by arrow-js.
//
// apache-arrow 18.x ships the generated flatbuffer classes for `BodyCompression`,
// `RecordBatch.addCompression`, etc. — but the high-level `RecordBatchWriter` never
// calls them and writes body buffers raw. We post-process each RecordBatch message:
//
//   1. Parse the Message/RecordBatch flatbuffer to recover FieldNodes and BufferRegions.
//   2. Compress each buffer with the arrow-rs wire format: int64 LE uncompressed-length
//      prefix + codec-compressed bytes; or `-1` + raw bytes when `8 + compressed.len >
//      uncompressed.len` (matches arrow-rs `arrow-ipc/src/compression.rs` exactly, incl.
//      the strict `>` threshold that keeps equal-size payloads compressed).
//   3. Rebuild the RecordBatch flatbuffer with the same length/nodes, new BufferRegions,
//      and a `BodyCompression{method:BUFFER, codec:LZ4_FRAME|ZSTD}` attached.
//   4. Rewrap in a new Message preserving version + customMetadata, with the new
//      bodyLength.
//
// All `apache-arrow/fb/*` subpath imports live in this one file so an arrow-js upgrade
// that shuffles the generated layout fails at a single point.
//
// Spec references:
//   - arrow-ipc-37.0.0/src/compression.rs (per-buffer wire format, -1 sentinel)
//   - arrow-ipc-37.0.0/src/writer.rs       (8-byte inter-buffer padding)

import { ByteBuffer, Builder } from 'flatbuffers';
// apache-arrow exposes its generated flatbuffer classes via `"./*"` in its exports map,
// where `*` must capture the file stem (no `.js` extension). The map's `types` rule
// resolves `apache-arrow/fb/foo` → `.../fb/foo.d.ts` and `default` → `.../fb/foo.mjs`.
import { BodyCompression } from 'apache-arrow/fb/body-compression';
import { BodyCompressionMethod } from 'apache-arrow/fb/body-compression-method';
import { Buffer as FbBuffer } from 'apache-arrow/fb/buffer';
import { CompressionType } from 'apache-arrow/fb/compression-type';
import { FieldNode } from 'apache-arrow/fb/field-node';
import { KeyValue } from 'apache-arrow/fb/key-value';
import { Message as FbMessage } from 'apache-arrow/fb/message';
import { MessageHeader } from 'apache-arrow/fb/message-header';
import { RecordBatch as FbRecordBatch } from 'apache-arrow/fb/record-batch';

import { BulkError } from '../errors.js';
import type { Compressor } from './codec-loader.js';
import { loadLz4, loadZstd } from './codec-loader.js';
import { BulkCompression } from './compression.js';
import type { IpcMessage } from './flight-codec.js';

const PREFIX_LEN = 8;
const BODY_ALIGNMENT = 8;
const LENGTH_NO_COMPRESSION = -1n;

/**
 * Resolve a user-facing `BulkCompression` to the matching flatbuffer codec enum and
 * load its compressor on demand. Returns `null` for `None` — callers should skip the
 * compression step entirely when this is null.
 */
export async function resolveCompressor(
  codec: BulkCompression,
): Promise<{ fbCodec: CompressionType; compressor: Compressor } | null> {
  switch (codec) {
    case BulkCompression.None:
      return null;
    case BulkCompression.Lz4:
      return { fbCodec: CompressionType.LZ4_FRAME, compressor: await loadLz4() };
    case BulkCompression.Zstd:
      return { fbCodec: CompressionType.ZSTD, compressor: await loadZstd() };
  }
}

interface ParsedBatch {
  readonly version: number;
  readonly rowCount: bigint;
  readonly fieldNodes: readonly { length: bigint; nullCount: bigint }[];
  readonly bufferRegions: readonly { offset: number; length: number }[];
  readonly customMetadata: readonly { key: string; value: string }[];
}

function parseRecordBatchMessage(metadata: Uint8Array): ParsedBatch {
  // flatbuffers.ByteBuffer requires a Uint8Array *starting at offset 0* of the view
  // it's handed; copy defensively if the caller gave us a subarray whose byteOffset
  // != 0. Cheap for typical IPC metadata (< 1 KiB).
  const owned =
    metadata.byteOffset === 0 && metadata.byteLength === metadata.buffer.byteLength
      ? metadata
      : new Uint8Array(metadata);
  const bb = new ByteBuffer(owned);
  const msg = FbMessage.getRootAsMessage(bb);

  if (msg.headerType() === MessageHeader.DictionaryBatch) {
    throw new BulkError(
      'dictionary batches with IPC body compression are not supported by this SDK',
    );
  }
  if (msg.headerType() !== MessageHeader.RecordBatch) {
    throw new BulkError(
      `expected RecordBatch message for compression, got headerType=${msg.headerType()}`,
    );
  }

  const rb = msg.header(new FbRecordBatch()) as FbRecordBatch | null;
  if (rb === null) {
    throw new BulkError('RecordBatch header missing in Message flatbuffer');
  }

  const fieldNodes: { length: bigint; nullCount: bigint }[] = [];
  for (let i = 0; i < rb.nodesLength(); i++) {
    const node = rb.nodes(i);
    if (node === null) throw new BulkError(`FieldNode[${i}] missing`);
    fieldNodes.push({ length: node.length(), nullCount: node.nullCount() });
  }

  const bufferRegions: { offset: number; length: number }[] = [];
  for (let i = 0; i < rb.buffersLength(); i++) {
    const b = rb.buffers(i);
    if (b === null) throw new BulkError(`Buffer[${i}] missing`);
    // Arrow bodies are well under 2^53 bytes in practice; bigint → number is safe
    // here and keeps downstream arithmetic readable.
    bufferRegions.push({ offset: Number(b.offset()), length: Number(b.length()) });
  }

  const customMetadata: { key: string; value: string }[] = [];
  for (let i = 0; i < msg.customMetadataLength(); i++) {
    const kv = msg.customMetadata(i);
    if (kv === null) continue;
    const key = kv.key();
    const value = kv.value();
    // Surface malformed IPC loudly: silently substituting '' would round-trip as a
    // legitimate-looking empty-string entry and mask upstream corruption.
    if (key === null || value === null) {
      throw new BulkError(`customMetadata[${i}] missing key or value`);
    }
    customMetadata.push({ key, value });
  }

  return {
    version: msg.version(),
    rowCount: rb.length(),
    fieldNodes,
    bufferRegions,
    customMetadata,
  };
}

/**
 * Compress a single buffer per arrow-rs wire format:
 *   - empty input → empty output (BufferRegion.length stays 0)
 *   - normal case → 8-byte LE uncompressed-length prefix + compressed bytes
 *   - if `prefix + compressed > uncompressed` (strict `>`), fall back to `-1` sentinel
 *     + raw uncompressed bytes, matching `compression.rs:73` behavior.
 */
async function compressOneBuffer(
  input: Uint8Array,
  compressor: Compressor,
): Promise<Uint8Array> {
  if (input.byteLength === 0) return new Uint8Array(0);
  const uncompressed = BigInt(input.byteLength);
  const compressed = await compressor(input);
  const emittedLen = PREFIX_LEN + compressed.byteLength;
  if (emittedLen > input.byteLength) {
    const out = new Uint8Array(PREFIX_LEN + input.byteLength);
    new DataView(out.buffer, out.byteOffset, out.byteLength).setBigInt64(
      0,
      LENGTH_NO_COMPRESSION,
      true,
    );
    out.set(input, PREFIX_LEN);
    return out;
  }
  const out = new Uint8Array(emittedLen);
  new DataView(out.buffer, out.byteOffset, out.byteLength).setBigInt64(
    0,
    uncompressed,
    true,
  );
  out.set(compressed, PREFIX_LEN);
  return out;
}

function padTo(n: number, alignment: number): number {
  const rem = n % alignment;
  return rem === 0 ? 0 : alignment - rem;
}

/**
 * Compress one RecordBatch IPC message. Returns the input unchanged when `codec`
 * is `BulkCompression.None`; otherwise compresses every buffer and rebuilds the
 * metadata with `BodyCompression` attached (zero-buffer batches still get the
 * `BodyCompression` marker — the empty body is a no-op to decompress). Schema
 * messages are filtered upstream and must not reach this function. Throws
 * `BulkError` for unsupported message types (Dictionary/Tensor/...) to avoid
 * silent corruption.
 */
export async function compressBatchMessage(
  msg: IpcMessage,
  codec: BulkCompression,
  fbCodec: CompressionType,
  compressor: Compressor,
): Promise<IpcMessage> {
  if (codec === BulkCompression.None) return msg;

  const parsed = parseRecordBatchMessage(msg.metadata);

  // Compress buffers in the flatbuffer-declared order (NOT sorted by offset — arrow-js
  // emits zero-length validity buffers at offset 0 which breaks monotonicity).
  const compressedBuffers = await Promise.all(
    parsed.bufferRegions.map((r) =>
      compressOneBuffer(msg.body.subarray(r.offset, r.offset + r.length), compressor),
    ),
  );

  // Lay them out sequentially in the new body with 8-byte padding between buffers.
  let cursor = 0;
  const newRegions: { offset: number; length: number }[] = [];
  for (const buf of compressedBuffers) {
    newRegions.push({ offset: cursor, length: buf.byteLength });
    cursor += buf.byteLength;
    cursor += padTo(cursor, BODY_ALIGNMENT);
  }
  const newBody = new Uint8Array(cursor);
  {
    let c = 0;
    for (const buf of compressedBuffers) {
      newBody.set(buf, c);
      c += buf.byteLength;
      c += padTo(c, BODY_ALIGNMENT);
    }
  }

  const newMetadata = buildRecordBatchMessage(parsed, newRegions, newBody.byteLength, fbCodec);
  return { metadata: newMetadata, body: newBody };
}

function buildRecordBatchMessage(
  parsed: ParsedBatch,
  newRegions: readonly { offset: number; length: number }[],
  newBodyLength: number,
  fbCodec: CompressionType,
): Uint8Array {
  const b = new Builder(256);

  // customMetadata (if any): serialize strings first, then build the KeyValue vector.
  let customMetadataOffset = 0;
  if (parsed.customMetadata.length > 0) {
    const kvOffsets: number[] = [];
    for (const { key, value } of parsed.customMetadata) {
      const kOff = b.createString(key);
      const vOff = b.createString(value);
      KeyValue.startKeyValue(b);
      KeyValue.addKey(b, kOff);
      KeyValue.addValue(b, vOff);
      kvOffsets.push(KeyValue.endKeyValue(b));
    }
    customMetadataOffset = FbMessage.createCustomMetadataVector(b, kvOffsets);
  }

  // BodyCompression table.
  const bodyCompressionOffset = BodyCompression.createBodyCompression(
    b,
    fbCodec,
    BodyCompressionMethod.BUFFER,
  );

  // FieldNode and Buffer are inline flatbuffer *structs* — write inside the vector
  // region in reverse so they land forward-order in the final buffer. apache-arrow's
  // own encoder follows the same reverse-iteration pattern.
  FbRecordBatch.startNodesVector(b, parsed.fieldNodes.length);
  for (let i = parsed.fieldNodes.length - 1; i >= 0; i--) {
    const n = parsed.fieldNodes[i];
    if (n === undefined) throw new BulkError(`FieldNode[${i}] missing during rebuild`);
    FieldNode.createFieldNode(b, n.length, n.nullCount);
  }
  const nodesVectorOffset = b.endVector();

  FbRecordBatch.startBuffersVector(b, newRegions.length);
  for (let i = newRegions.length - 1; i >= 0; i--) {
    const r = newRegions[i];
    if (r === undefined) throw new BulkError(`Buffer[${i}] missing during rebuild`);
    FbBuffer.createBuffer(b, BigInt(r.offset), BigInt(r.length));
  }
  const buffersVectorOffset = b.endVector();

  FbRecordBatch.startRecordBatch(b);
  FbRecordBatch.addLength(b, parsed.rowCount);
  FbRecordBatch.addNodes(b, nodesVectorOffset);
  FbRecordBatch.addBuffers(b, buffersVectorOffset);
  FbRecordBatch.addCompression(b, bodyCompressionOffset);
  const rbOffset = FbRecordBatch.endRecordBatch(b);

  FbMessage.startMessage(b);
  FbMessage.addVersion(b, parsed.version);
  FbMessage.addHeaderType(b, MessageHeader.RecordBatch);
  FbMessage.addHeader(b, rbOffset);
  FbMessage.addBodyLength(b, BigInt(newBodyLength));
  if (customMetadataOffset !== 0) FbMessage.addCustomMetadata(b, customMetadataOffset);
  const msgOffset = FbMessage.endMessage(b);

  b.finish(msgOffset);
  return b.asUint8Array();
}
