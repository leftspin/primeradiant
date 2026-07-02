#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  live_subspace_daemon_watcher_once.sh --source-db PATH --tenant TENANT --state-root DIR --eurisko-target USER@HOST --eurisko-repo DIR [--actor ACTOR] [--story-agents true|false] [--ssh-key PATH] [--eurisko-mix PATH] [--eurisko-erlang-bin DIR] [--eurisko-sqlite-bin DIR] [--limit N] [--timeout-seconds N] [--poll-interval-seconds N] [--debounce-seconds N] [--run-id ID] [--eurisko-soup-db PATH]

Runs one live Subspace daemon SQLite DB/WAL wakeup pass on the source host,
ships bounded Primeradiant event packages to EURISKO, and consumes them there
into Primeradiant-owned soup/output. The source DB is read with sqlite3
-readonly and is not copied or mutated.
USAGE
}

SOURCE_DB=""
TENANT=""
STATE_ROOT=""
EURISKO_TARGET=""
EURISKO_REPO=""
ACTOR="flynn"
STORY_AGENTS="true"
SSH_KEY="${HOME}/.ssh/id_ed25519_clu"
EURISKO_MIX="/home/clu/.local/share/mise/installs/elixir/1.19.5-otp-28/bin/mix"
EURISKO_ERLANG_BIN="/home/clu/.local/share/mise/installs/erlang/28.5/bin"
EURISKO_SQLITE_BIN="/home/clu/.local/share/mise/installs/sqlite/3.45.1/bin"
LIMIT="50"
TIMEOUT_SECONDS="300"
POLL_INTERVAL_SECONDS="2"
DEBOUNCE_SECONDS="2"
RUN_ID=""
EURISKO_SOUP_DB=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --state-root) STATE_ROOT="$2"; shift 2 ;;
    --eurisko-target) EURISKO_TARGET="$2"; shift 2 ;;
    --eurisko-repo) EURISKO_REPO="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --story-agents) STORY_AGENTS="$2"; shift 2 ;;
    --ssh-key) SSH_KEY="$2"; shift 2 ;;
    --eurisko-mix) EURISKO_MIX="$2"; shift 2 ;;
    --eurisko-erlang-bin) EURISKO_ERLANG_BIN="$2"; shift 2 ;;
    --eurisko-sqlite-bin) EURISKO_SQLITE_BIN="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --poll-interval-seconds) POLL_INTERVAL_SECONDS="$2"; shift 2 ;;
    --debounce-seconds) DEBOUNCE_SECONDS="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --eurisko-soup-db) EURISKO_SOUP_DB="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$TENANT" || -z "$STATE_ROOT" || -z "$EURISKO_TARGET" || -z "$EURISKO_REPO" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -r "$SOURCE_DB" ]]; then
  echo "source DB is not readable: $SOURCE_DB" >&2
  exit 1
fi

if [[ ! -r "$SSH_KEY" ]]; then
  echo "SSH key is not readable: $SSH_KEY" >&2
  exit 1
fi

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="live-subspace-$(date -u +%Y%m%dT%H%M%SZ)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATCHER="$SCRIPT_DIR/watch_sqlite_wakeup.sh"
EMITTER="$SCRIPT_DIR/emit_subspace_daemon_cursor_event_packages.sh"

CURSOR_FILE="$STATE_ROOT/cursor.txt"
PACKAGE_ROOT="$STATE_ROOT/packages/$RUN_ID"
LOCAL_RUN_ROOT="$STATE_ROOT/local-consume-placeholder/$RUN_ID"
EURISKO_HANDOFF_ROOT="/home/clu/primeradiant-handoffs/$RUN_ID"
EURISKO_RUN_ROOT="/home/clu/primeradiant-runs/$RUN_ID"
if [[ -z "$EURISKO_SOUP_DB" ]]; then
  EURISKO_SOUP_DB="/home/clu/.local/state/primeradiant/soup-api/soup.sqlite3"
fi

mkdir -p "$STATE_ROOT" "$PACKAGE_ROOT" "$LOCAL_RUN_ROOT"

MANIFESTS_JSONL="$STATE_ROOT/$RUN_ID-manifests.jsonl"
: > "$MANIFESTS_JSONL"

CURRENT_CURSOR=""
if [[ -s "$CURSOR_FILE" ]]; then
  CURRENT_CURSOR="$(tr -d '\r\n' < "$CURSOR_FILE")"
fi

CATCHUP_COUNT=0
CATCHUP_PASS=0
while :; do
  CATCHUP_PASS=$((CATCHUP_PASS + 1))
  CATCHUP_ROOT="$PACKAGE_ROOT/catchup-passes/$(printf '%04d' "$CATCHUP_PASS")"
  EMIT_ARGS=(
    --source-db "$SOURCE_DB"
    --tenant "$TENANT"
    --package-root "$CATCHUP_ROOT"
    --limit "$LIMIT"
    --run-id "$RUN_ID-catchup-$(printf '%04d' "$CATCHUP_PASS")"
  )
  if [[ -n "$CURRENT_CURSOR" ]]; then
    EMIT_ARGS+=(--after-cursor "$CURRENT_CURSOR")
  fi

  set +e
  CATCHUP_OUTPUT="$("$EMITTER" "${EMIT_ARGS[@]}" 2>&1)"
  CATCHUP_STATUS=$?
  set -e
  CATCHUP_MANIFEST="$(printf "%s" "$CATCHUP_OUTPUT" | tail -n 1)"
  if [[ "$CATCHUP_STATUS" -ne 0 ]]; then
    CATCHUP_FAILURE_MANIFEST="$CATCHUP_ROOT/manifest.json"
    FAILED_REPORT="$STATE_ROOT/$RUN_ID-failed-report.json"
    jq -n \
      --arg run_id "$RUN_ID" \
      --arg source_db "$SOURCE_DB" \
      --arg after_cursor "$CURRENT_CURSOR" \
      --arg failure_manifest "$CATCHUP_FAILURE_MANIFEST" \
      --arg emitter_output "$CATCHUP_OUTPUT" \
      --argjson exit_status "$CATCHUP_STATUS" \
      '{
        run_id: $run_id,
        source_db_path: $source_db,
        source_host: "tars",
        consume_host: "eurisko",
        source_mode: "live_subspace_daemon_watcher_once",
        status: "source_db_cursor_read_failed",
        after_cursor: $after_cursor,
        failed_stage: "cursor_catchup",
        failure_manifest_path: $failure_manifest,
        error: {
          command: "emit_subspace_daemon_cursor_event_packages.sh",
          exit_status: $exit_status,
          message: $emitter_output
        }
      }' > "$FAILED_REPORT"
    printf "live watcher cursor catchup failed; report: %s\n" "$FAILED_REPORT" >&2
    exit "$CATCHUP_STATUS"
  fi
  jq -n --arg kind "cursor_catchup" --arg manifest_path "$CATCHUP_MANIFEST" \
    '{kind: $kind, manifest_path: $manifest_path}' >> "$MANIFESTS_JSONL"

  EMITTED_COUNT="$(jq -r '.emitted_count' "$CATCHUP_MANIFEST")"
  NEXT_CURSOR="$(jq -r '.next_cursor' "$CATCHUP_MANIFEST")"
  CATCHUP_COUNT=$((CATCHUP_COUNT + EMITTED_COUNT))
  if [[ -n "$NEXT_CURSOR" && "$NEXT_CURSOR" != "null" ]]; then
    CURRENT_CURSOR="$NEXT_CURSOR"
    printf "%s\n" "$CURRENT_CURSOR" > "$CURSOR_FILE"
  fi
  if [[ "$EMITTED_COUNT" -lt "$LIMIT" ]]; then
    break
  fi
done

WATCH_REPORT=""
if [[ "$CATCHUP_COUNT" -eq 0 ]]; then
  set +e
  WATCH_OUTPUT="$(
    "$WATCHER" \
      --source-db "$SOURCE_DB" \
      --tenant "$TENANT" \
      --cursor-file "$CURSOR_FILE" \
      --package-root "$PACKAGE_ROOT/watch" \
      --run-root "$LOCAL_RUN_ROOT" \
      --actor "$ACTOR" \
      --limit "$LIMIT" \
      --run-id "$RUN_ID" \
      --timeout-seconds "$TIMEOUT_SECONDS" \
      --poll-interval-seconds "$POLL_INTERVAL_SECONDS" \
      --debounce-seconds "$DEBOUNCE_SECONDS" \
      --emit-cursor-script "$EMITTER" \
      --consume-packages false 2>&1
  )"
  WATCH_STATUS=$?
  set -e
  WATCH_REPORT="$(printf "%s" "$WATCH_OUTPUT" | tail -n 1)"
  if [[ "$WATCH_STATUS" -ne 0 ]]; then
    WATCH_FAILURE_REPORT="$PACKAGE_ROOT/watch/wakeup-failed-report.json"
    FAILED_REPORT="$STATE_ROOT/$RUN_ID-failed-report.json"
    jq -n \
      --arg run_id "$RUN_ID" \
      --arg source_db "$SOURCE_DB" \
      --arg after_cursor "$CURRENT_CURSOR" \
      --arg watch_failure_report "$WATCH_FAILURE_REPORT" \
      --arg watcher_output "$WATCH_OUTPUT" \
      --argjson exit_status "$WATCH_STATUS" \
      '{
        run_id: $run_id,
        source_db_path: $source_db,
        source_host: "tars",
        consume_host: "eurisko",
        source_mode: "live_subspace_daemon_watcher_once",
        status: "source_db_cursor_read_failed",
        after_cursor: $after_cursor,
        failed_stage: "cursor_wakeup",
        watch_failure_report_path: $watch_failure_report,
        error: {
          command: "watch_sqlite_wakeup.sh",
          exit_status: $exit_status,
          message: $watcher_output
        }
      }' > "$FAILED_REPORT"
    printf "live watcher cursor wakeup failed; report: %s\n" "$FAILED_REPORT" >&2
    exit "$WATCH_STATUS"
  fi
  jq -r '.passes[].manifest_path' "$WATCH_REPORT" |
    while IFS= read -r manifest_path; do
      [[ -z "$manifest_path" || "$manifest_path" == "null" ]] && continue
      jq -n --arg kind "wakeup" --arg manifest_path "$manifest_path" \
        '{kind: $kind, manifest_path: $manifest_path}' >> "$MANIFESTS_JSONL"
    done
fi

ssh -i "$SSH_KEY" "$EURISKO_TARGET" "mkdir -p '$EURISKO_HANDOFF_ROOT' '$EURISKO_RUN_ROOT'"

PACKAGE_LIST="$STATE_ROOT/$RUN_ID-packages.txt"
jq -r '.manifest_path' "$MANIFESTS_JSONL" |
  while IFS= read -r manifest; do
    [[ -z "$manifest" || "$manifest" == "null" ]] && continue
    jq -r '.packages[].package_dir' "$manifest"
  done > "$PACKAGE_LIST"

CONSUMED_JSONL="$STATE_ROOT/$RUN_ID-consumed.jsonl"
: > "$CONSUMED_JSONL"

while IFS= read -r package_dir; do
  [[ -z "$package_dir" ]] && continue
  package_name="$(basename "$package_dir")"
  remote_package="$EURISKO_HANDOFF_ROOT/$package_name"

  tar -C "$package_dir" -cf - . |
    ssh -i "$SSH_KEY" "$EURISKO_TARGET" "mkdir -p '$remote_package' && tar -C '$remote_package' -xf -"

  remote_out="$(
    if [[ "$STORY_AGENTS" == "true" ]]; then
      ssh -n -i "$SSH_KEY" "$EURISKO_TARGET" "cd '$EURISKO_REPO/storage-harness' && PATH='$(dirname "$EURISKO_MIX")':'$EURISKO_ERLANG_BIN':'$EURISKO_SQLITE_BIN':\$PATH scripts/r1/consume_event_package.sh --package-dir '$remote_package' --run-root '$EURISKO_RUN_ROOT' --tenant '$TENANT' --actor '$ACTOR' --story-agents --soup-db '$EURISKO_SOUP_DB'"
    else
      ssh -n -i "$SSH_KEY" "$EURISKO_TARGET" "cd '$EURISKO_REPO/storage-harness' && PATH='$(dirname "$EURISKO_MIX")':'$EURISKO_ERLANG_BIN':'$EURISKO_SQLITE_BIN':\$PATH scripts/r1/consume_event_package.sh --package-dir '$remote_package' --run-root '$EURISKO_RUN_ROOT' --tenant '$TENANT' --actor '$ACTOR' --soup-db '$EURISKO_SOUP_DB'"
    fi
  )"
  remote_out="$(printf "%s" "$remote_out" | tail -n 1)"
  jq -n \
    --arg package_dir "$package_dir" \
    --arg remote_package "$remote_package" \
    --arg remote_out "$remote_out" \
    '{package_dir: $package_dir, remote_package: $remote_package, remote_out: $remote_out}' \
    >> "$CONSUMED_JSONL"
done < "$PACKAGE_LIST"

REPORT="$STATE_ROOT/$RUN_ID-live-report.json"
jq -s \
  --arg run_id "$RUN_ID" \
  --arg source_db "$SOURCE_DB" \
  --arg watch_report "$WATCH_REPORT" \
  --argjson catchup_count "$CATCHUP_COUNT" \
  --arg eurisko_target "$EURISKO_TARGET" \
  --arg eurisko_repo "$EURISKO_REPO" \
  --arg eurisko_handoff_root "$EURISKO_HANDOFF_ROOT" \
  --arg eurisko_run_root "$EURISKO_RUN_ROOT" \
  --arg eurisko_soup_db "$EURISKO_SOUP_DB" \
  '{
    run_id: $run_id,
    source_db_path: $source_db,
    source_db_mutated_by_primeradiant: false,
    source_db_copied_by_primeradiant: false,
    wake_signal_role: "sqlite_db_wal_shm_wakeup_only",
    wake_payload_trusted_as_story_fact: false,
    source_host: "tars",
    consume_host: "eurisko",
    eurisko_target: $eurisko_target,
    eurisko_repo: $eurisko_repo,
    eurisko_handoff_root: $eurisko_handoff_root,
    eurisko_run_root: $eurisko_run_root,
    eurisko_soup_db: $eurisko_soup_db,
    cursor_catchup_before_wait_count: $catchup_count,
    local_watch_report: $watch_report,
    consumed: .
  }' "$CONSUMED_JSONL" > "$REPORT"

printf "%s\n" "$REPORT"
