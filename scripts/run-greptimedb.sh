#!/usr/bin/env bash
# Start a throwaway GreptimeDB instance for local dev / manual testing.
# Uses the official image; exposes 4001 (gRPC) and 4000 (HTTP).
set -euo pipefail

NAME="${GREPTIMEDB_CONTAINER_NAME:-greptimedb-dev}"
IMAGE="${GREPTIMEDB_IMAGE:-greptime/greptimedb:latest}"

if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
  echo "Stopping existing $NAME..."
  docker rm -f "$NAME" >/dev/null
fi

echo "Starting $NAME ($IMAGE)..."
docker run -d \
  --name "$NAME" \
  -p 4000:4000 \
  -p 4001:4001 \
  -p 4002:4002 \
  "$IMAGE" standalone start \
    --http-addr 0.0.0.0:4000 \
    --rpc-bind-addr 0.0.0.0:4001 \
    --mysql-addr 0.0.0.0:4002 >/dev/null

echo "Waiting for health..."
for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:4000/health >/dev/null 2>&1; then
    echo "Ready at grpc=127.0.0.1:4001 http=127.0.0.1:4000"
    exit 0
  fi
  sleep 1
done
echo "GreptimeDB failed to become ready" >&2
docker logs "$NAME" >&2
exit 1
