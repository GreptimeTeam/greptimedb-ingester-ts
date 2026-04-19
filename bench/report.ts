import { build } from 'hdr-histogram-js';

export interface RunResult {
  readonly rows: number;
  readonly elapsedMs: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
}

export function createLatencyHistogram(): ReturnType<typeof build> {
  return build({
    lowestDiscernibleValue: 1,
    highestTrackableValue: 600_000,
    numberOfSignificantValueDigits: 3,
    bitBucketSize: 32,
  });
}

export function printReport(label: string, result: RunResult): void {
  const rps = result.rows / (result.elapsedMs / 1000);
  console.log(
    `[${label}] rows=${result.rows} elapsed=${result.elapsedMs.toFixed(1)}ms ` +
      `rows/s=${rps.toFixed(0)} p50=${result.p50Ms.toFixed(1)}ms ` +
      `p95=${result.p95Ms.toFixed(1)}ms p99=${result.p99Ms.toFixed(1)}ms`,
  );
}

const ARG_RE = /^--([^=]+)=(.*)$/;

export function parseArgs(): Record<string, string> {
  const out: Record<string, string> = {};
  for (const raw of process.argv.slice(2)) {
    const match = ARG_RE.exec(raw);
    const key = match?.[1];
    const value = match?.[2];
    if (key !== undefined && value !== undefined) out[key] = value;
  }
  return out;
}

export function numArg(args: Record<string, string>, key: string, fallback: number): number {
  const v = args[key];
  if (v === undefined) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}
