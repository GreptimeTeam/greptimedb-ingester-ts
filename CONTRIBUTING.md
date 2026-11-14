# Contributing

Thanks for the interest. The codebase is small and the bar is high for changes to public API; most PRs do well to start with an issue describing the motivation.

## Prerequisites

- Node.js ≥ 20
- pnpm ≥ 9 (`corepack enable` is the easiest way)
- Docker (only needed for integration tests)

## Getting set up

```bash
pnpm install
./scripts/vendor-proto.sh       # refresh proto from ../rust/greptime-proto
pnpm codegen                    # regenerate src/generated/
```

The codegen step is optional day-to-day — CI gates it via `codegen:check`, so PRs with stale generated code fail fast.

## Usual loop

```bash
pnpm typecheck
pnpm lint
pnpm format
pnpm test

./scripts/run-greptimedb.sh     # spin up local docker
pnpm example 01-simple-insert   # smoke-test against it
```

## Integration tests

```bash
INTEGRATION=1 pnpm test:integration
```

Uses `testcontainers` to pull `greptime/greptimedb:v1.2.1`. Don't bump that tag casually — version bumps should be intentional and reviewed in their own PR so server-behavior drift doesn't hide inside an unrelated change.

## Browser integration tests

```bash
pnpm exec playwright install chromium
pnpm test:browser
```

Requires Docker and a current Node.js 22 patch release. The tests start an isolated GreptimeDB container with authentication and CORS, allocate host ports dynamically, and stop the container after the tests. The server image is pinned to `v1.4.0-nightly-20261109` because `v1.3.1` does not include gRPC CORS configuration. The browser bundle and declarations are checked without Node.js polyfills or Node.js type definitions.

## Benchmarks

```bash
pnpm bench bulk-api --rows=200000 --batch-size=5000 --parallelism=8 --endpoint=localhost:4001
```

See [docs/benchmarking.md](./docs/benchmarking.md) for the full harness.

## Pull requests

- Branch from `main`, keep PRs focused; large reformatting and a behavior fix in the same PR is a hard review.
- Link the related issue.
- If the change affects public API, update `CHANGELOG.md` and call it out in the PR description.
- New non-trivial logic needs tests. One-liners and obvious refactors don't.
- Run `pnpm typecheck && pnpm lint && pnpm format:check && pnpm test` before pushing — CI runs the same set.

## Release

Maintainers only. Bump `version` in `package.json`, add a dated entry to `CHANGELOG.md`, tag `vX.Y.Z`, and push the tag. The release workflow publishes to npm.
