// Convert Arrow tables into arrow.flight.protocol.FlightData messages, matching what
// Rust's `FlightEncoder` produces. We reuse Arrow IPC stream serialization then strip the
// 8-byte framing `[continuation:0xFFFFFFFF][metadata_len:uint32LE]` to obtain
// `data_header` (the Flatbuffer Message bytes) and `data_body` (the buffers blob).

import { create } from '@bufbuild/protobuf';
import {
  FlightDataSchema,
  FlightDescriptorSchema,
  FlightDescriptor_DescriptorType,
  type FlightData,
} from '../generated/arrow/flight/Flight_pb.js';
import {
  Message as ArrowMessage,
  Table as ArrowTable,
  tableToIPC,
  type Schema as ArrowSchema,
} from 'apache-arrow';
import type { Compressor } from './codec-loader.js';
import type { CompressionType } from 'apache-arrow/fb/compression-type';
import { BulkCompression } from './compression.js';
import { compressBatchMessage } from './ipc-compression.js';

const CONTINUATION = 0xffffffff;

// Module-level singletons — TextEncoder/TextDecoder are stateless and safe to share.
// Avoids one allocation per FlightData frame on both the encode and ack-decode paths.
const TEXT_ENCODER = /*@__PURE__*/ new TextEncoder();
const TEXT_DECODER = /*@__PURE__*/ new TextDecoder();

export interface IpcMessage {
  readonly metadata: Uint8Array;
  readonly body: Uint8Array;
}

/**
 * Parse an Arrow IPC stream byte buffer into a list of Flight-ready (metadata, body) pairs.
 * Drops the end-of-stream sentinel (metadata_len=0) and any padding.
 */
export function splitIpcStream(bytes: Uint8Array): IpcMessage[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const messages: IpcMessage[] = [];
  let i = 0;
  while (i < bytes.length) {
    const first = view.getUint32(i, true);
    let metaLenOffset: number;
    if (first === CONTINUATION) {
      metaLenOffset = i + 4;
    } else {
      // Pre-0.15.0 IPC format writes metadata_len directly; stay compatible.
      metaLenOffset = i;
    }
    const metaLen = view.getUint32(metaLenOffset, true);
    if (metaLen === 0) {
      // End-of-stream marker.
      break;
    }
    const metaStart = metaLenOffset + 4;
    const metaEnd = metaStart + metaLen;
    const metadata = bytes.subarray(metaStart, metaEnd);

    // Parse bodyLength out of the flatbuffer Message. Offset derivation using
    // apache-arrow's Message class keeps this robust across schema evolutions.
    const bodyLength = readBodyLength(metadata);
    const bodyStart = metaEnd;
    const body = bytes.subarray(bodyStart, bodyStart + bodyLength);

    messages.push({ metadata, body });

    i = bodyStart + bodyLength;
    // Arrow IPC pads messages to an 8-byte boundary.
    const padding = (8 - (i % 8)) % 8;
    i += padding;
  }
  return messages;
}

/**
 * Read bodyLength from an Arrow Message flatbuffer. Layout (from arrow-format/Message.fbs):
 *   table Message {
 *     version: MetadataVersion;
 *     header: MessageHeader;
 *     bodyLength: long;
 *     custom_metadata: [KeyValue];
 *   }
 * flatbuffer root offset is the first 4 bytes (little-endian uoffset_t). The vtable pattern
 * is fiddly; the simplest portable approach is to delegate to apache-arrow which already
 * exposes a `Message` parser.
 */
function readBodyLength(metadata: Uint8Array): number {
  const msg = ArrowMessage.decode(metadata);
  const bl = msg.bodyLength;
  return typeof bl === 'bigint' ? Number(bl) : bl;
}

function doPutMetadataBytes(requestId: number): Uint8Array {
  return TEXT_ENCODER.encode(JSON.stringify({ request_id: requestId }));
}

export interface DoPutResponseJson {
  readonly request_id: number;
  readonly affected_rows: number;
}

export function parseDoPutResponse(appMetadata: Uint8Array): DoPutResponseJson {
  return JSON.parse(TEXT_DECODER.decode(appMetadata)) as DoPutResponseJson;
}

function pathDescriptor(tableName: string) {
  return create(FlightDescriptorSchema, {
    type: FlightDescriptor_DescriptorType.PATH,
    path: [tableName],
  });
}

/** Extract IPC messages from an Arrow Table, packaged for DoPut. */
export interface FlightEncodingBundle {
  readonly schemaMessage: IpcMessage;
  readonly batchMessages: readonly IpcMessage[];
}

export interface EncodeCompressionContext {
  readonly codec: BulkCompression;
  readonly fbCodec: CompressionType;
  readonly compressor: Compressor;
}

export async function encodeTableForFlight(
  table: ArrowTable,
  compression?: EncodeCompressionContext,
): Promise<FlightEncodingBundle> {
  const ipc = tableToIPC(table, 'stream');
  const messages = splitIpcStream(ipc);
  if (messages.length === 0) {
    throw new Error('Arrow IPC produced no messages');
  }
  const [schema, ...batches] = messages;
  if (schema === undefined) throw new Error('Arrow IPC missing schema message');

  // Schema messages have no body, so compression is a no-op there. Only RecordBatch
  // messages need BodyCompression injected.
  if (compression !== undefined && compression.codec !== BulkCompression.None) {
    const compressed = await Promise.all(
      batches.map((msg) =>
        compressBatchMessage(msg, compression.codec, compression.fbCodec, compression.compressor),
      ),
    );
    return { schemaMessage: schema, batchMessages: compressed };
  }
  return { schemaMessage: schema, batchMessages: batches };
}

/**
 * Build a Flight schema frame. This is sent once, at start of the DoPut stream, to tell
 * the server the table name + Arrow schema. `app_metadata` is JSON with `request_id = 0`.
 */
export function buildSchemaFlightData(
  tableName: string,
  schemaMsg: IpcMessage,
): FlightData {
  return create(FlightDataSchema, {
    flightDescriptor: pathDescriptor(tableName),
    dataHeader: schemaMsg.metadata,
    dataBody: schemaMsg.body,
    appMetadata: doPutMetadataBytes(0),
  });
}

/** Build a Flight batch frame. `app_metadata` is JSON with the user-allocated request_id. */
export function buildBatchFlightData(
  batchMsg: IpcMessage,
  requestId: number,
): FlightData {
  return create(FlightDataSchema, {
    dataHeader: batchMsg.metadata,
    dataBody: batchMsg.body,
    appMetadata: doPutMetadataBytes(requestId),
  });
}

/**
 * Encode a bare Arrow schema (no batches) to an IpcMessage. Useful for sending the schema
 * frame up front when the first user batch isn't available yet.
 */
export function schemaToIpcMessage(schema: ArrowSchema): IpcMessage {
  const table = new ArrowTable(schema);
  const bytes = tableToIPC(table, 'stream');
  const msgs = splitIpcStream(bytes);
  if (msgs.length === 0 || msgs[0] === undefined) {
    throw new Error('failed to serialize Arrow schema');
  }
  return msgs[0];
}
