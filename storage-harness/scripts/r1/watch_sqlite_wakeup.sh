#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  watch_sqlite_wakeup.sh --source-db PATH --tenant TENANT --cursor-file PATH --package-root DIR --run-root DIR [--actor ACTOR] [--limit N] [--run-id ID] [--timeout-seconds N] [--poll-interval-seconds N] [--debounce-seconds N]

Observes daemon-news SQLite DB/WAL/SHM file changes as a wakeup bell, then runs
the normal read-only cursor importer in serialized bounded passes until caught
up. The file change is not trusted as story data.
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
DEBOUNCE_SECONDS="1"

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
    --debounce-seconds) DEBOUNCE_SECONDS="$2"; shift 2 ;;
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

if ! [[ "$DEBOUNCE_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "debounce-seconds must be a non-negative integer" >&2
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
LOCK_DIR="$CURSOR_FILE.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Primeradiant importer already running for cursor file: $CURSOR_FILE" >&2
  exit 4
fi
cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if [[ "$DEBOUNCE_SECONDS" -gt 0 ]]; then
  sleep "$DEBOUNCE_SECONDS"
fi

AFTER_CURSOR=""
if [[ -s "$CURSOR_FILE" ]]; then
  AFTER_CURSOR="$(tr -d '\r\n' < "$CURSOR_FILE")"
fi

PASS_MANIFESTS="$PACKAGE_ROOT/pass-manifests.jsonl"
: > "$PASS_MANIFESTS"
TOTAL_EMITTED=0
PASS_COUNT=0
CURRENT_CURSOR="$AFTER_CURSOR"
LAST_IMPORT_SIGNATURE="$WAKE_SIGNATURE"

while :; do
  PASS_COUNT=$((PASS_COUNT + 1))
  PASS_ROOT="$PACKAGE_ROOT/passes/$(printf '%04d' "$PASS_COUNT")"
  PASS_RUN_ID="$RUN_ID-pass-$(printf '%04d' "$PASS_COUNT")"
  PASS_START_SIGNATURE="$(signature)"

  EMIT_ARGS=(
    --source-db "$SOURCE_DB"
    --tenant "$TENANT"
    --package-root "$PASS_ROOT"
    --limit "$LIMIT"
    --run-id "$PASS_RUN_ID"
  )

  if [[ -n "$CURRENT_CURSOR" ]]; then
    EMIT_ARGS+=(--after-cursor "$CURRENT_CURSOR")
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

  EMITTED_COUNT="$(jq -r '.emitted_count' "$MANIFEST_PATH")"
  NEXT_CURSOR="$(jq -r '.next_cursor' "$MANIFEST_PATH")"
  PASS_END_SIGNATURE="$(signature)"
  TOTAL_EMITTED=$((TOTAL_EMITTED + EMITTED_COUNT))

  jq -n \
    --argjson pass "$PASS_COUNT" \
    --arg manifest_path "$MANIFEST_PATH" \
    --arg after_cursor "$CURRENT_CURSOR" \
    --arg next_cursor "$NEXT_CURSOR" \
    --arg start_signature "$PASS_START_SIGNATURE" \
    --arg end_signature "$PASS_END_SIGNATURE" \
    --argjson emitted_count "$EMITTED_COUNT" \
    '{
      pass: $pass,
      manifest_path: $manifest_path,
      after_cursor: $after_cursor,
      next_cursor: $next_cursor,
      emitted_count: $emitted_count,
      observed_file_signature_before_import: $start_signature,
      observed_file_signature_after_import: $end_signature
    }' >> "$PASS_MANIFESTS"

  if [[ -n "$NEXT_CURSOR" && "$NEXT_CURSOR" != "null" ]]; then
    CURRENT_CURSOR="$NEXT_CURSOR"
    printf "%s\n" "$CURRENT_CURSOR" > "$CURSOR_FILE"
  fi

  LAST_IMPORT_SIGNATURE="$PASS_END_SIGNATURE"

  if [[ "$EMITTED_COUNT" -lt "$LIMIT" && "$PASS_START_SIGNATURE" == "$PASS_END_SIGNATURE" ]]; then
    break
  fi
done

WATCH_REPORT="$PACKAGE_ROOT/wakeup-report.json"
jq -s \
  --arg run_id "$RUN_ID" \
  --arg source_db "$SOURCE_DB" \
  --arg cursor_file "$CURSOR_FILE" \
  --arg after_cursor "$AFTER_CURSOR" \
  --arg next_cursor "$CURRENT_CURSOR" \
  --arg wake_kind "sqlite_file_change" \
  --arg start_signature "$START_SIGNATURE" \
  --arg wake_signature "$WAKE_SIGNATURE" \
  --arg final_signature "$LAST_IMPORT_SIGNATURE" \
  --argjson pass_count "$PASS_COUNT" \
  --argjson emitted_count "$TOTAL_EMITTED" \
  '{
    run_id: $run_id,
    source_adapter: "daemon-news",
    source_db_path: $source_db,
    source_mode: "sqlite_file_wakeup_read_only_cursor",
    wake_kind: $wake_kind,
    wake_signal_debounced: true,
    importer_single_flight: true,
    importer_catch_up_until_empty: true,
    wake_payload_trusted_as_story_fact: false,
    source_db_mutated_by_primeradiant: false,
    persistent_service_installed: false,
    cursor_file: $cursor_file,
    after_cursor: $after_cursor,
    next_cursor: $next_cursor,
    emitted_count: $emitted_count,
    pass_count: $pass_count,
    passes: .,
    observed_file_signature_before: $start_signature,
    observed_file_signature_after_wake: $wake_signature,
    observed_file_signature_after_import: $final_signature
  }' "$PASS_MANIFESTS" > "$WATCH_REPORT"

printf "%s\n" "$WATCH_REPORT"
