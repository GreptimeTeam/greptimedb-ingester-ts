// Pluggable endpoint selection for multi-endpoint failover.
//
// A selector turns the static `endpoints` list into one chosen "host:port" per call.
// Three built-ins ship here:
//   - RandomSelector       — stateless uniform pick (the default; preserves prior behavior)
//   - RoundRobinSelector   — stateless rotation via a monotonic counter
//   - OutlierDetectingSelector — wraps a base selector and ejects endpoints that produce
//                                consecutive failures (Envoy-style consecutive-5xx ejection)
//
// `SelectContext.exclude` lets a retry loop steer away from peers that already failed in the
// current attempt sequence so a single dead endpoint cannot burn the whole retry budget. It is
// best-effort: when every endpoint is excluded the selector falls open to the full set rather
// than failing, because a retry against the least-bad peer beats no retry at all.
//
// Health hooks (`reportSuccess` / `reportFailure`) are driven only by endpoint-level failures
// (see `isEndpointFailure`), never by server business errors — a healthy frontend must not be
// ejected for a datanode-side condition.

import { ConfigError } from '../errors.js';

export interface SelectContext {
  /** Peers already tried and failed in the current retry sequence; avoid them if possible. */
  readonly exclude?: ReadonlySet<string>;
}

/**
 * Reports the outcome of a single endpoint-bound session (a stream or bulk writer). Invoked at
 * most once at the terminal transition: with the error on failure, or with no argument on
 * success. The Client maps this to `reportSuccess` / `reportFailure` via `isEndpointFailure`, so
 * only genuine transport failures eject an endpoint — server business errors do not.
 */
export type EndpointOutcomeHook = (error?: unknown) => void;

/**
 * Picks one endpoint per call. Implementations must tolerate a non-empty `endpoints` list and
 * honor `ctx.exclude` on a best-effort basis. The optional health hooks are no-ops for the
 * stateless built-ins; a stateful selector uses them to learn which peers are unhealthy.
 */
export interface EndpointSelector {
  select(endpoints: readonly string[], ctx?: SelectContext): string;
  reportSuccess?(endpoint: string): void;
  reportFailure?(endpoint: string): void;
}

function pick(pool: readonly string[], idx: number): string {
  const ep = pool[idx];
  if (ep === undefined) throw new ConfigError('no endpoints available');
  return ep;
}

/**
 * Apply `exclude` as a best-effort filter: drop excluded peers, but fall open to the full list
 * when that would leave nothing to choose from.
 */
function applyExclude(
  endpoints: readonly string[],
  exclude: ReadonlySet<string> | undefined,
): readonly string[] {
  if (exclude === undefined || exclude.size === 0) return endpoints;
  const kept = endpoints.filter((ep) => !exclude.has(ep));
  return kept.length > 0 ? kept : endpoints;
}

/** Uniform random pick. Stateless; the default selector. */
export class RandomSelector implements EndpointSelector {
  public select(endpoints: readonly string[], ctx?: SelectContext): string {
    const pool = applyExclude(endpoints, ctx?.exclude);
    return pick(pool, Math.floor(Math.random() * pool.length));
  }
}

/** Rotates through endpoints in order via a monotonic counter. */
export class RoundRobinSelector implements EndpointSelector {
  private counter = 0;

  public select(endpoints: readonly string[], ctx?: SelectContext): string {
    const pool = applyExclude(endpoints, ctx?.exclude);
    const idx = this.counter % pool.length;
    this.counter = (this.counter + 1) % Number.MAX_SAFE_INTEGER;
    return pick(pool, idx);
  }
}

export interface OutlierDetectionOptions {
  /** Base selector that chooses among the currently-healthy peers. Default: RandomSelector. */
  readonly base?: EndpointSelector;
  /** Consecutive failures before an endpoint is ejected. Default: 5. */
  readonly consecutiveFailures?: number;
  /** First ejection duration; doubles on each re-ejection up to `maxEjectionMs`. Default: 30s. */
  readonly baseEjectionMs?: number;
  /** Ceiling for a single ejection duration. Default: 300s. */
  readonly maxEjectionMs?: number;
  /** Injectable clock for tests. Default: `Date.now`. */
  readonly now?: () => number;
}

interface EndpointHealth {
  consecutiveFailures: number;
  ejectedUntil: number;
  ejectionCount: number;
}

/**
 * Wraps a base selector and removes endpoints that have failed `consecutiveFailures` times in a
 * row from the candidate set for a back-off window. Health is fed by `reportSuccess` /
 * `reportFailure` from call outcomes. Ejection is time-based and re-admission is lazy (compared
 * against `now()` at select time, no timers). If every endpoint is ejected the selector falls
 * open to the full set so writes never hard-stall on transient cluster-wide failures.
 */
export class OutlierDetectingSelector implements EndpointSelector {
  private readonly base: EndpointSelector;
  private readonly threshold: number;
  private readonly baseEjectionMs: number;
  private readonly maxEjectionMs: number;
  private readonly now: () => number;
  private readonly health = new Map<string, EndpointHealth>();

  public constructor(opts: OutlierDetectionOptions = {}) {
    this.base = opts.base ?? new RandomSelector();
    this.threshold = opts.consecutiveFailures ?? 5;
    this.baseEjectionMs = opts.baseEjectionMs ?? 30_000;
    this.maxEjectionMs = opts.maxEjectionMs ?? 300_000;
    this.now = opts.now ?? Date.now;
    if (!Number.isInteger(this.threshold) || this.threshold < 1) {
      throw new ConfigError(
        `consecutiveFailures must be a positive integer, got ${String(this.threshold)}`,
      );
    }
    if (this.baseEjectionMs <= 0 || this.maxEjectionMs <= 0) {
      throw new ConfigError('ejection durations must be > 0');
    }
  }

  public select(endpoints: readonly string[], ctx?: SelectContext): string {
    const now = this.now();
    const healthy = endpoints.filter((ep) => {
      const h = this.health.get(ep);
      return h === undefined || h.ejectedUntil <= now;
    });
    const candidates = healthy.length > 0 ? healthy : endpoints;
    return this.base.select(candidates, ctx);
  }

  public reportSuccess(endpoint: string): void {
    const h = this.health.get(endpoint);
    if (h !== undefined) {
      // A fresh success outweighs stale failures: clear the streak and re-admit immediately.
      // During an active ejection the endpoint is not selected, so a success can only arrive
      // via a fall-open window — exactly when we want to re-admit the one peer that works.
      h.consecutiveFailures = 0;
      h.ejectedUntil = 0;
    }
    this.base.reportSuccess?.(endpoint);
  }

  public reportFailure(endpoint: string): void {
    const h = this.health.get(endpoint) ?? {
      consecutiveFailures: 0,
      ejectedUntil: 0,
      ejectionCount: 0,
    };
    this.health.set(endpoint, h);
    h.consecutiveFailures++;
    const now = this.now();
    // Only (re)eject when not already ejected, so failures observed during a fall-open window
    // don't compound the back-off prematurely.
    if (h.consecutiveFailures >= this.threshold && h.ejectedUntil <= now) {
      const duration = Math.min(this.maxEjectionMs, this.baseEjectionMs * 2 ** h.ejectionCount);
      h.ejectedUntil = now + duration;
      h.ejectionCount++;
      h.consecutiveFailures = 0;
    }
    this.base.reportFailure?.(endpoint);
  }
}

/** Stateless uniform random selector (the default). */
export function randomSelector(): EndpointSelector {
  return new RandomSelector();
}

/** Stateless round-robin selector. */
export function roundRobinSelector(): EndpointSelector {
  return new RoundRobinSelector();
}

/** Health-aware selector with Envoy-style consecutive-failure outlier ejection. */
export function outlierDetectingSelector(opts?: OutlierDetectionOptions): EndpointSelector {
  return new OutlierDetectingSelector(opts);
}
