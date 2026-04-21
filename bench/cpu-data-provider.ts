// Shared CPU schema + row generator for cross-protocol benches.
//
// Mirrors https://github.com/killme2008/greptimedb-ingestion-benchmark
// (benchmark/data.go):
//   - tags:   host (configurable), region (5), datacenter (10), service (20)
//   - fields: cpu, memory, disk_util, net_in, net_out (all Float64)
//   - ts:     ms precision, incremented per row
//   - series: numHosts × 5 × 10 × 20, distributed round-robin
//
// Timestamps grow monotonically from BASE_TIME_MS so each row has a unique
// (series, ts) pair — otherwise GreptimeDB will dedup on primary key and the
// verify step will report fewer rows than sent.

import { DataType, Precision, Table, type TableSchema } from '../src/index.js';

export const REGIONS = [
  'us-east-1',
  'us-west-2',
  'eu-west-1',
  'ap-southeast-1',
  'ap-northeast-1',
] as const;
export const DATACENTERS = Array.from({ length: 10 }, (_, i) => `dc-${i}`);
export const SERVICES = Array.from({ length: 20 }, (_, i) => `svc-${String(i).padStart(2, '0')}`);

export const BASE_TIME_MS = Date.UTC(2024, 0, 1, 0, 0, 0, 0);

export interface CpuRow {
  readonly host: string;
  readonly region: string;
  readonly datacenter: string;
  readonly service: string;
  readonly cpu: number;
  readonly memory: number;
  readonly diskUtil: number;
  readonly netIn: number;
  readonly netOut: number;
  readonly tsMs: number;
}

export function buildCpuSchemaTable(tableName: string): Table {
  return Table.new(tableName)
    .addTagColumn('host', DataType.String)
    .addTagColumn('region', DataType.String)
    .addTagColumn('datacenter', DataType.String)
    .addTagColumn('service', DataType.String)
    .addFieldColumn('cpu', DataType.Float64)
    .addFieldColumn('memory', DataType.Float64)
    .addFieldColumn('disk_util', DataType.Float64)
    .addFieldColumn('net_in', DataType.Float64)
    .addFieldColumn('net_out', DataType.Float64)
    .addTimestampColumn('ts', Precision.Millisecond);
}

export function cpuSchema(tableName: string): TableSchema {
  return buildCpuSchemaTable(tableName).schema();
}

export function generateCpuRows(count: number, numHosts: number, offset: number): CpuRow[] {
  if (!Number.isInteger(numHosts) || numHosts < 1) {
    throw new RangeError(`numHosts must be a positive integer, got ${numHosts}`);
  }
  if (!Number.isInteger(count) || count < 0) {
    throw new RangeError(`count must be a non-negative integer, got ${count}`);
  }
  const dcServices = DATACENTERS.length * SERVICES.length;
  const regionBlock = REGIONS.length * dcServices;
  const seriesCount = numHosts * regionBlock;
  const rows: CpuRow[] = new Array(count);
  for (let k = 0; k < count; k++) {
    const i = offset + k;
    const seriesIdx = i % seriesCount;
    const hostIdx = Math.floor(seriesIdx / regionBlock);
    let rem = seriesIdx % regionBlock;
    const regionIdx = Math.floor(rem / dcServices);
    rem = rem % dcServices;
    const dcIdx = Math.floor(rem / SERVICES.length);
    const svcIdx = rem % SERVICES.length;
    rows[k] = {
      host: `host-${hostIdx}`,
      region: REGIONS[regionIdx]!,
      datacenter: DATACENTERS[dcIdx]!,
      service: SERVICES[svcIdx]!,
      cpu: Math.random() * 100,
      memory: Math.random() * 100,
      diskUtil: Math.random() * 100,
      netIn: Math.random() * 1e9,
      netOut: Math.random() * 1e9,
      tsMs: BASE_TIME_MS + i,
    };
  }
  return rows;
}

// Flat array form for the gRPC bulk bench, which takes `unknown[][]`.
export function cpuRowsAsArrays(rows: readonly CpuRow[]): unknown[][] {
  const out: unknown[][] = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i]!;
    out[i] = [
      r.host,
      r.region,
      r.datacenter,
      r.service,
      r.cpu,
      r.memory,
      r.diskUtil,
      r.netIn,
      r.netOut,
      r.tsMs,
    ];
  }
  return out;
}
