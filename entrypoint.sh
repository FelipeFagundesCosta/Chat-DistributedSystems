#!/usr/bin/env bash
set -euo pipefail

cd /app

python3 server.py &
SERVER_PID=$!

python3 proxy.py &
PROXY_PID=$!

cleanup() {
  kill -TERM "$SERVER_PID" "$PROXY_PID" 2>/dev/null || true
}
trap cleanup SIGINT SIGTERM EXIT

wait -n
exit_code=$?
cleanup
wait "$SERVER_PID" "$PROXY_PID" 2>/dev/null || true
exit "$exit_code"
