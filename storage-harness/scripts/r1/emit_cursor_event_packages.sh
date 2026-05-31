#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  emit_cursor_event_packages.sh --source-db PATH --tenant TENANT --package-root DIR [--after-cursor RECEIVED_AT|MESSAGE_ID] [--limit N] [--run-id ID]

Reads daemon-news rows after a Primeradiant-owned cursor with sqlite3 -readonly
and emits bounded committed-source-item event packages. The daemon-news DB is
the source-owned event log; this script writes only Primeradiant-owned handoff
packages and a manifest under DIR.
USAGE
}

SOURCE_DB=""
TENANT=""
PACKAGE_ROOT=""
AFTER_CURSOR=""
LIMIT="50"
RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --package-root) PACKAGE_ROOT="$2"; shift 2 ;;
    --after-cursor) AFTER_CURSOR="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$TENANT" || -z "$PACKAGE_ROOT" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -r "$SOURCE_DB" ]]; then
  echo "source DB is not readable: $SOURCE_DB" >&2
  exit 1
fi

if ! [[ "$LIMIT" =~ ^[0-9]+$ ]] || [[ "$LIMIT" -lt 1 ]]; then
  echo "limit must be a positive integer" >&2
  exit 2
fi

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="cursor-$(date -u +%Y%m%dT%H%M%SZ)"
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

AFTER_RECEIVED=""
AFTER_MESSAGE=""
if [[ -n "$AFTER_CURSOR" ]]; then
  AFTER_RECEIVED="${AFTER_CURSOR%%|*}"
  AFTER_MESSAGE="${AFTER_CURSOR#*|}"
  if [[ "$AFTER_RECEIVED" == "$AFTER_MESSAGE" ]]; then
    echo "after cursor must be RECEIVED_AT|MESSAGE_ID" >&2
    exit 2
  fi
fi

WHERE="message_type = 'swarm.channel.news.report.v0'"
if [[ -n "$AFTER_CURSOR" ]]; then
  WHERE="$WHERE AND (received_at > '$(sql_escape "$AFTER_RECEIVED")' OR (received_at = '$(sql_escape "$AFTER_RECEIVED")' AND message_id > '$(sql_escape "$AFTER_MESSAGE")'))"
fi

mkdir -p "$PACKAGE_ROOT/packages"
ROWS_FILE="$PACKAGE_ROOT/cursor-rows.tsv"
PACKAGES_JSONL="$PACKAGE_ROOT/packages.jsonl"
: > "$PACKAGES_JSONL"

sqlite3 -readonly -separator $'\t' "$SOURCE_DB" "
  SELECT message_id, received_at
    FROM messages
   WHERE $WHERE
   ORDER BY received_at ASC, message_id ASC
   LIMIT $LIMIT;
" > "$ROWS_FILE"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMITTER="$SCRIPT_DIR/emit_committed_event_package.sh"

COUNT=0
NEXT_CURSOR="$AFTER_CURSOR"

while IFS=$'\t' read -r MESSAGE_ID RECEIVED_AT; do
  [[ -z "$MESSAGE_ID" ]] && continue
  COUNT=$((COUNT + 1))
  PACKAGE_DIR="$PACKAGE_ROOT/packages/$(printf '%04d' "$COUNT")-$MESSAGE_ID"
  EVENT_ID="$RUN_ID-$COUNT"
  "$EMITTER" \
    --source-db "$SOURCE_DB" \
    --message-id "$MESSAGE_ID" \
    --tenant "$TENANT" \
    --event-id "$EVENT_ID" \
    --package-dir "$PACKAGE_DIR" >/dev/null
  NEXT_CURSOR="$RECEIVED_AT|$MESSAGE_ID"
  jq -n \
    --arg message_id "$MESSAGE_ID" \
    --arg received_at "$RECEIVED_AT" \
    --arg event_id "$EVENT_ID" \
    --arg package_dir "$PACKAGE_DIR" \
    --arg cursor "$NEXT_CURSOR" \
    '{message_id: $message_id, received_at: $received_at, event_id: $event_id, package_dir: $package_dir, cursor: $cursor}' \
    >> "$PACKAGES_JSONL"
done < "$ROWS_FILE"

jq -s \
  --arg run_id "$RUN_ID" \
  --arg source_db "$SOURCE_DB" \
  --arg after_cursor "$AFTER_CURSOR" \
  --arg next_cursor "$NEXT_CURSOR" \
  --argjson count "$COUNT" \
  '{
    run_id: $run_id,
    source_adapter: "daemon-news",
    source_db_path: $source_db,
    source_mode: "read_only_db_cursor",
    after_cursor: $after_cursor,
    next_cursor: $next_cursor,
    emitted_count: $count,
    persistent_service_installed: false,
    packages: .
  }' "$PACKAGES_JSONL" > "$PACKAGE_ROOT/manifest.json"

printf "%s\n" "$PACKAGE_ROOT/manifest.json"
