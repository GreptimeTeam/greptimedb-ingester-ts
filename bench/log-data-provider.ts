// 22-column synthetic log table, mirroring Rust `bench/log_table_data_provider.rs`.
//
// Cardinality tiers (approximate):
//   high-card:   trace_id, span_id
//   medium-card: host, service, pod, region, env
//   low-card:    log_level, http_method, http_status_class
//   continuous:  latency_ms, bytes_in, bytes_out
// Timestamps: ingest_ts, log_ts, received_ts (ms precision).

import { DataType, Precision, Table } from '../src/index.js';

const HOSTS = Array.from({ length: 32 }, (_, i) => `host-${i}`);
const SERVICES = ['api', 'auth', 'billing', 'search', 'stream', 'worker'];
const PODS = Array.from({ length: 64 }, (_, i) => `pod-${i}`);
const REGIONS = ['us-east', 'us-west', 'eu-west', 'ap-south'];
const ENVS = ['prod', 'staging', 'canary', 'dev'];
const LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG'];
const METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'];
const STATUS_CLASSES = ['2xx', '3xx', '4xx', '5xx'];
const ENDPOINTS = ['/api/v1/users', '/api/v1/orders', '/api/v1/search', '/health', '/metrics'];
const UA = ['Mozilla/5.0', 'curl/8.0', 'Go-http-client/1.1', 'greptime-agent/0.3'];

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)] as T;
}

function randHex(len: number): string {
  const b = new Uint8Array(len / 2);
  for (let i = 0; i < b.length; i++) b[i] = Math.floor(Math.random() * 256);
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
}

export const TABLE_NAME = 'bench_logs';

export function buildSchemaTable(): Table {
  return Table.new(TABLE_NAME)
    .addTagColumn('service', DataType.String)
    .addTagColumn('host', DataType.String)
    .addTagColumn('region', DataType.String)
    .addTagColumn('env', DataType.String)
    .addFieldColumn('pod', DataType.String)
    .addFieldColumn('log_level', DataType.String)
    .addFieldColumn('message', DataType.String)
    .addFieldColumn('trace_id', DataType.String)
    .addFieldColumn('span_id', DataType.String)
    .addFieldColumn('http_method', DataType.String)
    .addFieldColumn('http_path', DataType.String)
    .addFieldColumn('http_status_class', DataType.String)
    .addFieldColumn('user_agent', DataType.String)
    .addFieldColumn('client_ip', DataType.String)
    .addFieldColumn('caller', DataType.String)
    .addFieldColumn('latency_ms', DataType.Float64)
    .addFieldColumn('bytes_in', DataType.Int64)
    .addFieldColumn('bytes_out', DataType.Int64)
    .addFieldColumn('error_flag', DataType.Bool)
    .addFieldColumn('retry_flag', DataType.Bool)
    .addTimestampColumn('log_ts', Precision.Millisecond)
    .addFieldColumn('ingest_ts', DataType.Int64);
}

export function generateRow(i: number): unknown[] {
  const level = pick(LEVELS);
  const statusClass = pick(STATUS_CLASSES);
  return [
    pick(SERVICES), // service
    pick(HOSTS), // host
    pick(REGIONS), // region
    pick(ENVS), // env
    pick(PODS), // pod
    level, // log_level
    `request ${i} handled via ${pick(ENDPOINTS)}`, // message
    randHex(32), // trace_id
    randHex(16), // span_id
    pick(METHODS), // http_method
    pick(ENDPOINTS), // http_path
    statusClass, // http_status_class
    pick(UA), // user_agent
    `10.${Math.floor(Math.random() * 256)}.${Math.floor(Math.random() * 256)}.${Math.floor(Math.random() * 256)}`,
    `src/handler/${pick(SERVICES)}.ts:${Math.floor(Math.random() * 500)}`, // caller
    Math.random() * 200, // latency_ms
    BigInt(Math.floor(Math.random() * 100_000)), // bytes_in
    BigInt(Math.floor(Math.random() * 200_000)), // bytes_out
    level === 'ERROR', // error_flag
    Math.random() < 0.05, // retry_flag
    Date.now(), // log_ts
    BigInt(Date.now()), // ingest_ts
  ];
}

export function generateBatch(size: number, offset = 0): unknown[][] {
  const rows: unknown[][] = [];
  for (let i = 0; i < size; i++) rows.push(generateRow(offset + i));
  return rows;
}
