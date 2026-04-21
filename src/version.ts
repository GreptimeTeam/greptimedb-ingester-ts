// Single source of truth for the SDK version in the code path. Kept in sync with
// the `version` field in `package.json` by a drift guard in `test/unit/smoke.test.ts`.
// Bump this together with `package.json` on every release.
export const VERSION = '0.1.0';
