export { VERSION } from './version.js';

export { Client } from './client.js';

export {
  ConfigBuilder,
  DEFAULT_RETRY_POLICY,
  type AuthConfig,
  type ClientConfig,
  type GrpcCompression,
  type KeepAliveConfig,
  type RetryPolicy,
  type TlsConfig,
} from './config.js';

export {
  AbortedError,
  BulkError,
  ConfigError,
  IngesterError,
  SchemaError,
  ServerError,
  StateError,
  TimeoutError,
  TransportError,
  ValueError,
  isRetriable,
  type RetryMode,
} from './errors.js';

export {
  DataType,
  Precision,
  Table,
  validateTableSchema,
  type ColumnSpec,
  type Semantic,
  type TableSchema,
} from './table/index.js';

export type { WriteOptions } from './write/unary.js';
export type { AffectedRows } from './write/affected-rows.js';
export { StreamWriter, type StreamOptions } from './write/stream-writer.js';

export {
  BulkStreamWriter,
  type BulkWriteOptions,
  type RowBatch,
  type BulkFinishSummary,
} from './bulk/bulk-stream-writer.js';
export { BulkCompression } from './bulk/compression.js';
export type { BulkWriteResponse } from './bulk/request-tracker.js';

export {
  field,
  tableName,
  tag,
  timestamp,
  type ColumnOptions,
  type TimestampOptions,
} from './decorators/index.js';

export { consoleLogger, NOOP_LOGGER, type LogLevel, type Logger } from './internal/logger.js';
