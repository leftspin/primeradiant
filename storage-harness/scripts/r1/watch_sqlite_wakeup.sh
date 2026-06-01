#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  watch_sqlite_wakeup.sh --source-db PATH --tenant TENANT --cursor-file PATH --package-root DIR --run-root DIR [--actor ACTOR] [--limit N] [--run-id ID] [--timeout-seconds N] [--poll-interval-seconds N]

Observes daemon-news SQLite DB/WAL/SHM file changes as a wakeup bell, then runs
the normal read-only cursor importer and consumes emitted event packages into
Primeradiant-owned soup/output. The file change is not trusted as story data.
USAGE
}

SOURCE_DB=""
TENANT=""
CURSOR_FILE=""
PACKAGE_ROOT=""
RUN_ROOT=""
ACTOR="flynn"
LIMIT="50"
RUN_ID=""
TIMEOUT_SECONDS="30"
POLL_INTERVAL_SECONDS="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --cursor-file) CURSOR_FILE="$2"; shift 2 ;;
    --package-root) PACKAGE_ROOT="$2"; shift 2 ;;
    --run-root) RUN_ROOT="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --poll-interval-seconds) POLL_INTERVAL_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$TENANT" || -z "$CURSOR_FILE" || -z "$PACKAGE_ROOT" || -z "$RUN_ROOT" ]]; then
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

if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT_SECONDS" -lt 1 ]]; then
  echo "timeout-seconds must be a positive integer" >&2
  exit 2
fi

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="watch-$(date -u +%Y%m%dT%H%M%SZ)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMIT_CURSOR="$SCRIPT_DIR/emit_cursor_event_packages.sh"
CONSUME_PACKAGE="$SCRIPT_DIR/consume_event_package.sh"

signature() {
  for path in "$SOURCE_DB" "$SOURCE_DB-wal" "$SOURCE_DB-shm"; do
    if [[ -e "$path" ]]; then
      if stat -f '%N|%m|%z' "$path" >/dev/null 2>&1; then
        stat -f '%N|%m|%z' "$path"
      else
        stat -c '%n|%Y|%s' "$path"
      fi
    else
      printf "%s|missing\n" "$path"
    fi
  done
}

START_SIGNATURE="$(signature)"
DEADLINE=$((SECONDS + TIMEOUT_SECONDS))
WAKE_SIGNATURE=""

while [[ "$SECONDS" -lt "$DEADLINE" ]]; do
  sleep "$POLL_INTERVAL_SECONDS"
  CURRENT_SIGNATURE="$(signature)"
  if [[ "$CURRENT_SIGNATURE" != "$START_SIGNATURE" ]]; then
    WAKE_SIGNATURE="$CURRENT_SIGNATURE"
    break
  fi
done

if [[ -z "$WAKE_SIGNATURE" ]]; then
  echo "timed out waiting for SQLite DB/WAL change: $SOURCE_DB" >&2
  exit 3
fi

mkdir -p "$(dirname "$CURSOR_FILE")" "$PACKAGE_ROOT" "$RUN_ROOT"

AFTER_CURSOR=""
if [[ -s "$CURSOR_FILE" ]]; then
  AFTER_CURSOR="$(tr -d '\r\n' < "$CURSOR_FILE")"
fi

EMIT_ARGS=(
  --source-db "$SOURCE_DB"
  --tenant "$TENANT"
  --package-root "$PACKAGE_ROOT"
  --limit "$LIMIT"
  --run-id "$RUN_ID"
)

if [[ -n "$AFTER_CURSOR" ]]; then
  EMIT_ARGS+=(--after-cursor "$AFTER_CURSOR")
fi

MANIFEST_PATH="$("$EMIT_CURSOR" "${EMIT_ARGS[@]}")"
MANIFEST_PATH="$(printf "%s" "$MANIFEST_PATH" | tail -n 1)"

while IFS= read -r package_dir; do
  [[ -z "$package_dir" || "$package_dir" == "null" ]] && continue
  "$CONSUME_PACKAGE" \
    --package-dir "$package_dir" \
    --run-root "$RUN_ROOT" \
    --tenant "$TENANT" \
    --actor "$ACTOR" >/dev/null
done < <(jq -r '.packages[].package_dir' "$MANIFEST_PATH")

NEXT_CURSOR="$(jq -r '.next_cursor' "$MANIFEST_PATH")"
if [[ -n "$NEXT_CURSOR" && "$NEXT_CURSOR" != "null" ]]; then
  printf "%s\n" "$NEXT_CURSOR" > "$CURSOR_FILE"
fi

WATCH_REPORT="$PACKAGE_ROOT/wakeup-report.json"
jq -n \
  --arg run_id "$RUN_ID" \
  --arg source_db "$SOURCE_DB" \
  --arg cursor_file "$CURSOR_FILE" \
  --arg after_cursor "$AFTER_CURSOR" \
  --arg next_cursor "$NEXT_CURSOR" \
  --arg manifest_path "$MANIFEST_PATH" \
  --arg wake_kind "sqlite_file_change" \
  --arg start_signature "$START_SIGNATURE" \
  --arg wake_signature "$WAKE_SIGNATURE" \
  --argjson emitted_count "$(jq -r '.emitted_count' "$MANIFEST_PATH")" \
  '{
    run_id: $run_id,
    source_adapter: "daemon-news",
    source_db_path: $source_db,
    source_mode: "sqlite_file_wakeup_read_only_cursor",
    wake_kind: $wake_kind,
    wake_payload_trusted_as_story_fact: false,
    source_db_mutated_by_primeradiant: false,
    persistent_service_installed: false,
    cursor_file: $cursor_file,
    after_cursor: $after_cursor,
    next_cursor: $next_cursor,
    emitted_count: $emitted_count,
    manifest_path: $manifest_path,
    observed_file_signature_before: $start_signature,
    observed_file_signature_after: $wake_signature
  }' > "$WATCH_REPORT"

printf "%s\n" "$WATCH_REPORT"
