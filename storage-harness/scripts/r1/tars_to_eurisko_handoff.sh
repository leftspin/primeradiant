#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tars_to_eurisko_handoff.sh --source-db PATH --eurisko-handoff-root PATH [--run-id ID] [--tars SSH] [--eurisko SSH]

Manual/test-scoped R1 transport. It runs push_snapshot.sh on TARS against the
source-owned daemon-news DB, then streams the resulting handoff directory into
EURISKO's primeradiant-owned handoff root. It installs no service/autostart.
USAGE
}

SOURCE_DB=""
EURISKO_HANDOFF_ROOT=""
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
TARS_SSH="mike@tars"
EURISKO_SSH="clu@eurisko"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --eurisko-handoff-root) EURISKO_HANDOFF_ROOT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --tars) TARS_SSH="$2"; shift 2 ;;
    --eurisko) EURISKO_SSH="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$EURISKO_HANDOFF_ROOT" ]]; then
  usage >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUSH_SCRIPT="$SCRIPT_DIR/push_snapshot.sh"

REMOTE_WORK="/tmp/primeradiant-r1-push-$RUN_ID"
REMOTE_HANDOFF_ROOT="$REMOTE_WORK/handoffs"
REMOTE_SCRIPT="$REMOTE_WORK/push_snapshot.sh"
REMOTE_HANDOFF_DIR="$REMOTE_HANDOFF_ROOT/$RUN_ID"
EURISKO_HANDOFF_DIR="$EURISKO_HANDOFF_ROOT/$RUN_ID"

ssh "$TARS_SSH" "rm -rf '$REMOTE_WORK' && mkdir -p '$REMOTE_WORK'"
scp "$PUSH_SCRIPT" "$TARS_SSH:$REMOTE_SCRIPT" >/dev/null
ssh "$TARS_SSH" "chmod +x '$REMOTE_SCRIPT' && '$REMOTE_SCRIPT' --source-db '$SOURCE_DB' --handoff-root '$REMOTE_HANDOFF_ROOT' --run-id '$RUN_ID' >/dev/null"

ssh "$EURISKO_SSH" "mkdir -p '$EURISKO_HANDOFF_ROOT'"
ssh "$TARS_SSH" "cd '$REMOTE_HANDOFF_ROOT' && tar -cf - '$RUN_ID'" |
  ssh "$EURISKO_SSH" "cd '$EURISKO_HANDOFF_ROOT' && rm -rf '$RUN_ID' && tar -xf -"

ssh "$TARS_SSH" "rm -rf '$REMOTE_WORK'"

echo "$EURISKO_HANDOFF_DIR"
