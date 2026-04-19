#!/usr/bin/env bash
# Refresh vendored proto files from upstream sources.
# - greptime-proto: ../../rust/greptime-proto
# - Apache Arrow Flight: apache/arrow main
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GREPTIME_PROTO_SRC="${GREPTIME_PROTO_SRC:-$ROOT/../../rust/greptime-proto/proto}"

if [[ ! -d "$GREPTIME_PROTO_SRC/greptime/v1" ]]; then
  echo "greptime-proto source not found at $GREPTIME_PROTO_SRC" >&2
  exit 1
fi

echo "Syncing greptime protos from $GREPTIME_PROTO_SRC"
mkdir -p "$ROOT/proto/greptime/v1"
for f in column common database ddl health prom row; do
  cp "$GREPTIME_PROTO_SRC/greptime/v1/$f.proto" "$ROOT/proto/greptime/v1/$f.proto"
done

echo "Downloading Arrow Flight.proto"
mkdir -p "$ROOT/proto/arrow/flight"
curl -sSL https://raw.githubusercontent.com/apache/arrow/main/format/Flight.proto \
  -o "$ROOT/proto/arrow/flight/Flight.proto"

echo "Done. Next: run 'pnpm codegen'."
