/**
 * Return an AbortSignal that fires when any of the given signals abort. If a `timeoutMs`
 * is provided, the combined signal also aborts after that timeout. Returned signals are
 * linked via AbortController.
 */
export function combineSignals(
  signals: readonly (AbortSignal | undefined)[],
  timeoutMs?: number,
): { signal: AbortSignal; cleanup: () => void } {
  const controller = new AbortController();
  const handlers: { signal: AbortSignal; handler: () => void }[] = [];
  let timeoutHandle: NodeJS.Timeout | undefined;

  const abortFromSignal = (s: AbortSignal): void => {
    controller.abort(s.reason);
  };

  for (const s of signals) {
    if (s === undefined) continue;
    if (s.aborted) {
      controller.abort(s.reason);
      break;
    }
    const handler = (): void => {
      abortFromSignal(s);
    };
    s.addEventListener('abort', handler, { once: true });
    handlers.push({ signal: s, handler });
  }
  if (timeoutMs !== undefined) {
    timeoutHandle = setTimeout(() => {
      controller.abort(new Error(`timeout ${timeoutMs}ms`));
    }, timeoutMs);
  }

  const cleanup = (): void => {
    for (const { signal, handler } of handlers) signal.removeEventListener('abort', handler);
    if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
  };

  return { signal: controller.signal, cleanup };
}
