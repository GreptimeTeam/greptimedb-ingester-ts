// Hand-written gRPC method definitions for @grpc/grpc-js.
//
// `@bufbuild/protobuf` generates message types + `GenService` descriptors but does NOT
// produce `grpc.MethodDefinition` objects usable by @grpc/grpc-js. This file bridges the
// two: we wire buf-generated `toBinary`/`fromBinary` into grpc-js's MethodDefinition shape.
//
// Only the RPCs actually used by the ingester are exposed. Adding more is a 10-line change.

import { fromBinary, toBinary, type DescMessage, type MessageShape } from '@bufbuild/protobuf';
import type { MethodDefinition } from '@grpc/grpc-js';

import {
  GreptimeRequestSchema,
  GreptimeResponseSchema,
  type GreptimeRequest,
  type GreptimeResponse,
} from '../generated/greptime/v1/database_pb.js';
import {
  FlightDataSchema,
  PutResultSchema,
  type FlightData,
  type PutResult,
} from '../generated/arrow/flight/Flight_pb.js';

function makeMethod<Req extends DescMessage, Res extends DescMessage>(
  path: string,
  requestSchema: Req,
  responseSchema: Res,
  requestStream: boolean,
  responseStream: boolean,
): MethodDefinition<MessageShape<Req>, MessageShape<Res>> {
  return {
    path,
    requestStream,
    responseStream,
    requestSerialize: (value: MessageShape<Req>): Buffer =>
      Buffer.from(toBinary(requestSchema, value)),
    requestDeserialize: (bytes: Buffer): MessageShape<Req> =>
      fromBinary(requestSchema, bytes) as MessageShape<Req>,
    responseSerialize: (value: MessageShape<Res>): Buffer =>
      Buffer.from(toBinary(responseSchema, value)),
    responseDeserialize: (bytes: Buffer): MessageShape<Res> =>
      fromBinary(responseSchema, bytes) as MessageShape<Res>,
  };
}

export const HandleMethod: MethodDefinition<GreptimeRequest, GreptimeResponse> = makeMethod(
  '/greptime.v1.GreptimeDatabase/Handle',
  GreptimeRequestSchema,
  GreptimeResponseSchema,
  false,
  false,
);

export const HandleRequestsMethod: MethodDefinition<GreptimeRequest, GreptimeResponse> = makeMethod(
  '/greptime.v1.GreptimeDatabase/HandleRequests',
  GreptimeRequestSchema,
  GreptimeResponseSchema,
  true,
  false,
);

export const DoPutMethod: MethodDefinition<FlightData, PutResult> = makeMethod(
  '/arrow.flight.protocol.FlightService/DoPut',
  FlightDataSchema,
  PutResultSchema,
  true,
  true,
);
