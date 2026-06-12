import { describe, expect, it } from 'vitest';
import {
  ConfigError,
  OutlierDetectingSelector,
  RandomSelector,
  RoundRobinSelector,
} from '../../src/index.js';

const EPS = ['a:1', 'b:1', 'c:1'];

describe('RandomSelector', () => {
  it('throws on an empty endpoint list', () => {
    expect(() => new RandomSelector().select([])).toThrow(ConfigError);
  });

  it('always returns the single endpoint', () => {
    const s = new RandomSelector();
    for (let i = 0; i < 10; i++) expect(s.select(['only:1'])).toBe('only:1');
  });

  it('honors exclude and only returns non-excluded peers', () => {
    const s = new RandomSelector();
    const exclude = new Set(['a:1', 'b:1']);
    for (let i = 0; i < 50; i++) expect(s.select(EPS, { exclude })).toBe('c:1');
  });

  it('falls open to the full set when every peer is excluded', () => {
    const s = new RandomSelector();
    const exclude = new Set(EPS);
    for (let i = 0; i < 50; i++) expect(EPS).toContain(s.select(EPS, { exclude }));
  });
});

describe('RoundRobinSelector', () => {
  it('rotates through endpoints in order', () => {
    const s = new RoundRobinSelector();
    expect([s.select(EPS), s.select(EPS), s.select(EPS), s.select(EPS)]).toEqual([
      'a:1',
      'b:1',
      'c:1',
      'a:1',
    ]);
  });

  it('skips excluded peers while rotating', () => {
    const s = new RoundRobinSelector();
    const exclude = new Set(['b:1']);
    const got = [
      s.select(EPS, { exclude }),
      s.select(EPS, { exclude }),
      s.select(EPS, { exclude }),
    ];
    for (const ep of got) expect(ep).not.toBe('b:1');
  });

  it('falls open when every peer is excluded', () => {
    const s = new RoundRobinSelector();
    const exclude = new Set(EPS);
    expect(EPS).toContain(s.select(EPS, { exclude }));
  });
});

// A controllable clock so ejection windows are deterministic.
function fakeClock(start = 0): { now: () => number; advance: (ms: number) => void } {
  let t = start;
  return {
    now: () => t,
    advance: (ms: number) => {
      t += ms;
    },
  };
}

describe('OutlierDetectingSelector', () => {
  it('rejects invalid options', () => {
    expect(() => new OutlierDetectingSelector({ consecutiveFailures: 0 })).toThrow(ConfigError);
    expect(() => new OutlierDetectingSelector({ baseEjectionMs: 0 })).toThrow(ConfigError);
  });

  it('ejects a peer after consecutive failures and re-admits after the window', () => {
    const clock = fakeClock();
    const s = new OutlierDetectingSelector({
      base: new RoundRobinSelector(),
      consecutiveFailures: 3,
      baseEjectionMs: 1000,
      now: clock.now,
    });

    for (let i = 0; i < 3; i++) s.reportFailure('a:1');
    // 'a:1' is ejected: round-robin over the healthy {b,c} only.
    const during = [s.select(EPS), s.select(EPS), s.select(EPS), s.select(EPS)];
    expect(during).not.toContain('a:1');

    clock.advance(1000);
    // After the window the peer is re-admitted lazily.
    const after = new Set([s.select(EPS), s.select(EPS), s.select(EPS)]);
    expect(after).toContain('a:1');
  });

  it('does not eject below the threshold, and reportSuccess clears the streak', () => {
    const clock = fakeClock();
    const s = new OutlierDetectingSelector({
      consecutiveFailures: 3,
      baseEjectionMs: 1000,
      now: clock.now,
    });
    s.reportFailure('a:1');
    s.reportFailure('a:1');
    s.reportSuccess('a:1'); // resets the streak
    s.reportFailure('a:1');
    s.reportFailure('a:1'); // only 2 in a row — still healthy
    for (let i = 0; i < 20; i++) expect(EPS).toContain(s.select(EPS));
    // The peer is never excluded by ejection, so over many picks it must appear.
    const seen = new Set(Array.from({ length: 60 }, () => s.select(EPS)));
    expect(seen).toContain('a:1');
  });

  it('reportSuccess re-admits an ejected peer immediately', () => {
    const clock = fakeClock();
    const s = new OutlierDetectingSelector({
      base: new RoundRobinSelector(),
      consecutiveFailures: 2,
      baseEjectionMs: 10_000,
      now: clock.now,
    });
    s.reportFailure('a:1');
    s.reportFailure('a:1');
    expect([s.select(EPS), s.select(EPS), s.select(EPS)]).not.toContain('a:1');
    s.reportSuccess('a:1');
    const seen = new Set([s.select(EPS), s.select(EPS), s.select(EPS)]);
    expect(seen).toContain('a:1');
  });

  it('grows the ejection window exponentially, capped at maxEjectionMs', () => {
    const clock = fakeClock();
    const s = new OutlierDetectingSelector({
      consecutiveFailures: 1,
      baseEjectionMs: 100,
      maxEjectionMs: 250,
      now: clock.now,
    });
    const eject = (): void => {
      s.reportFailure('a:1');
    };
    const ejectedNow = (): boolean => {
      // 'a:1' ejected iff a single-endpoint select on it falls open to the only peer
      // but a multi-endpoint select never returns it.
      const picks = new Set(Array.from({ length: 30 }, () => s.select(EPS)));
      return !picks.has('a:1');
    };

    eject(); // window 100ms
    expect(ejectedNow()).toBe(true);
    clock.advance(100);
    expect(ejectedNow()).toBe(false);

    eject(); // window 200ms
    expect(ejectedNow()).toBe(true);
    clock.advance(100);
    expect(ejectedNow()).toBe(true); // still within 200ms
    clock.advance(100);
    expect(ejectedNow()).toBe(false);

    eject(); // window would be 400ms, capped at 250ms
    clock.advance(250);
    expect(ejectedNow()).toBe(false);
  });

  it('falls open when every peer is ejected', () => {
    const clock = fakeClock();
    const s = new OutlierDetectingSelector({
      consecutiveFailures: 1,
      baseEjectionMs: 1000,
      now: clock.now,
    });
    for (const ep of EPS) s.reportFailure(ep);
    // All ejected — selection must still return one of them rather than throwing.
    expect(EPS).toContain(s.select(EPS));
  });
});
