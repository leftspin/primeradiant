#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  consume_event_package.sh --package-dir DIR --run-root DIR --tenant TENANT [--actor ACTOR] [--story-agents]

Consumes a bounded daemon-news R1 event package and writes Primeradiant-owned
soup/output under RUN_ROOT. The source event, not a timer or snapshot cadence,
drives admission.
USAGE
}

PACKAGE_DIR=""
RUN_ROOT=""
TENANT=""
ACTOR="flynn"
STORY_AGENTS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --package-dir) PACKAGE_DIR="$2"; shift 2 ;;
    --run-root) RUN_ROOT="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --story-agents) STORY_AGENTS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$PACKAGE_DIR" || -z "$RUN_ROOT" || -z "$TENANT" ]]; then
  usage >&2
  exit 2
fi

EVENT_PATH="$PACKAGE_DIR/event.json"
if [[ ! -r "$EVENT_PATH" ]]; then
  echo "event package is missing event.json: $PACKAGE_DIR" >&2
  exit 1
fi

EVENT_ID="$(jq -r '.event_id' "$EVENT_PATH")"
if [[ -z "$EVENT_ID" || "$EVENT_ID" == "null" ]]; then
  echo "event package has no event_id: $EVENT_PATH" >&2
  exit 1
fi

OUT_DIR="$RUN_ROOT/$EVENT_ID"
SOUP_DB="$OUT_DIR/soup.sqlite3"
REPORT="$OUT_DIR/changed-stories-report.json"
MIX_OUTPUT="$OUT_DIR/mix-output.log"
mkdir -p "$OUT_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

pushd "$HARNESS_DIR" >/dev/null
MIX_ARGS=(
  primeradiant.daemon_news_event
  --event "$EVENT_PATH"
  --raw-root "$PACKAGE_DIR"
  --tenant "$TENANT"
  --actor "$ACTOR"
  --soup-db "$SOUP_DB"
)
if [[ "$STORY_AGENTS" -eq 1 ]]; then
  MIX_ARGS+=(--story-agents)
fi
mix "${MIX_ARGS[@]}" > "$MIX_OUTPUT"
popd >/dev/null

awk 'found || /^\{/ {found=1; print}' "$MIX_OUTPUT" > "$REPORT"

printf "%s\n" "$OUT_DIR"
