#!/usr/bin/env bash
set -euo pipefail

DB=/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3
STATE_ROOT=/Users/mike/.local/state/primeradiant/t328-live-watcher
RUNNER=/Users/mike/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh
TENANT=00000000-0000-0000-0000-00000000t328
LOG_DIR="$STATE_ROOT/logs"

mkdir -p "$LOG_DIR"

while true; do
  RUN_ID="live-$(date -u +%Y%m%dT%H%M%SZ)"
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] starting $RUN_ID cursor=$(cat "$STATE_ROOT/cursor.txt" 2>/dev/null || true)" >> "$LOG_DIR/loop.log"
  set +e
  "$RUNNER" \
    --source-db "$DB" \
    --tenant "$TENANT" \
    --state-root "$STATE_ROOT" \
    --eurisko-target clu@eurisko \
    --eurisko-repo /home/clu/src/primeradiant \
    --ssh-key /Users/mike/.ssh/id_ed25519_clu \
    --limit 20 \
    --timeout-seconds 600 \
    --poll-interval-seconds 2 \
    --debounce-seconds 2 \
    --run-id "$RUN_ID" >> "$LOG_DIR/loop.log" 2>&1
  rc=$?
  set -e
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $RUN_ID exited rc=$rc cursor=$(cat "$STATE_ROOT/cursor.txt" 2>/dev/null || true)" >> "$LOG_DIR/loop.log"
  sleep 3
done
