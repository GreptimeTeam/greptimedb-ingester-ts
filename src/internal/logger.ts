export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  log(level: LogLevel, msg: string, meta?: Record<string, unknown>): void;
}

export const NOOP_LOGGER: Logger = {
  log(): void {
    // intentionally empty
  },
};

export function consoleLogger(minLevel: LogLevel = 'info'): Logger {
  const order: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
  return {
    log(level, msg, meta) {
      if (order[level] < order[minLevel]) return;
      const ts = new Date().toISOString();
      const line = `[${ts}] ${level.toUpperCase()} ${msg}`;
      const payload = meta ? `${line} ${JSON.stringify(meta)}` : line;
      if (level === 'error') console.error(payload);
      else if (level === 'warn') console.warn(payload);
      else console.log(payload);
    },
  };
}
