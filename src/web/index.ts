export { VERSION } from '../version.js';
export { Client, type ClientConfig } from './client.js';
export { DEFAULT_RETRY_POLICY, type AuthConfig, type RetryPolicy } from '../config-shared.js';
export {
  AbortedError,
  ConfigError,
  GreptimeStatusCode,
  IngesterError,
  SchemaError,
  ServerError,
  StateError,
  TimeoutError,
  TransportError,
  ValueError,
  isRetriable,
  isRetryableStatusCode,
  type RetryMode,
} from '../errors.js';

export {
  DataType,
  Precision,
  Table,
  validateTableSchema,
  type ColumnSpec,
  type DecimalSpec,
  type Semantic,
  type TableSchema,
} from '../table/index.js';

export type { WriteOptions } from '../write/request.js';
export type { AffectedRows } from '../write/affected-rows.js';
export {
  field,
  tableName,
  tag,
  timestamp,
  type ColumnOptions,
  type TimestampOptions,
} from '../decorators/index.js';

export { consoleLogger, NOOP_LOGGER, type LogLevel, type Logger } from '../internal/logger.js';
