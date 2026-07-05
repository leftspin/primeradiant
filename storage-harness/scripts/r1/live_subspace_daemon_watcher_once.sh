#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  live_subspace_daemon_watcher_once.sh --source-db PATH --tenant TENANT --state-root DIR --eurisko-target USER@HOST --eurisko-repo DIR [--actor ACTOR] [--story-agents true|false] [--ssh-key PATH] [--eurisko-mix PATH] [--eurisko-erlang-bin DIR] [--eurisko-sqlite-bin DIR] [--limit N] [--timeout-seconds N] [--poll-interval-seconds N] [--debounce-seconds N] [--run-id ID] [--eurisko-soup-db PATH] [--active-runner-root DIR] [--active-runner-manifest PATH]

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
ACTIVE_RUNNER_ROOT="${HOME}/.local/libexec/primeradiant/r1"
ACTIVE_RUNNER_MANIFEST=""

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
    --active-runner-root) ACTIVE_RUNNER_ROOT="$2"; shift 2 ;;
    --active-runner-manifest) ACTIVE_RUNNER_MANIFEST="$2"; shift 2 ;;
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
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
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

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

file_mtime_utc() {
  if stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%SZ' "$1" >/dev/null 2>&1; then
    stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%SZ' "$1"
  else
    stat -c '%y' "$1"
  fi
}

repo_commit() {
  git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf "unknown"
}

repo_status_short() {
  git -C "$REPO_ROOT" status --short 2>/dev/null || true
}

build_runner_parity_report() {
  local output_path="$1"
  local source_commit installed_status installed_manifest manifest_json manifest_valid
  local runner_jsonl helper_jsonl status_jsonl

  source_commit="$(repo_commit)"
  runner_jsonl="$STATE_ROOT/$RUN_ID-runner-inventory.jsonl"
  helper_jsonl="$STATE_ROOT/$RUN_ID-helper-parity.jsonl"
  status_jsonl="$STATE_ROOT/$RUN_ID-parity-status.jsonl"
  : > "$runner_jsonl"
  : > "$helper_jsonl"
  : > "$status_jsonl"

  if [[ -z "$ACTIVE_RUNNER_ROOT" ]]; then
    installed_status="not_configured"
  elif [[ -d "$ACTIVE_RUNNER_ROOT" ]]; then
    installed_status="present"
  else
    installed_status="absent"
  fi

  if [[ -z "$ACTIVE_RUNNER_MANIFEST" && -n "$ACTIVE_RUNNER_ROOT" && -r "$ACTIVE_RUNNER_ROOT/install-manifest.json" ]]; then
    installed_manifest="$ACTIVE_RUNNER_ROOT/install-manifest.json"
  else
    installed_manifest="$ACTIVE_RUNNER_MANIFEST"
  fi

  if [[ -n "$installed_manifest" && -r "$installed_manifest" ]]; then
    manifest_json="$(jq -c . "$installed_manifest" 2>/dev/null || jq -n --arg path "$installed_manifest" '{path: $path, parse_error: true}')"
  else
    manifest_json="null"
  fi

  manifest_valid=true
  if [[ "$manifest_json" == "null" ]]; then
    manifest_valid=false
  elif ! jq -e '
    .manifest_version == "primeradiant_r1_install_v1" and
    (.source_repo | type == "string") and
    (.source_commit | type == "string") and
    (.installed_root | type == "string") and
    (.installed_at | type == "string") and
    (.installed_by | type == "string") and
    (.install_session | type == "string") and
    (.helpers | type == "array")
  ' >/dev/null 2>&1 <<<"$manifest_json"; then
    manifest_valid=false
  fi

  jq -n \
    --arg kind "repo_source" \
    --arg path "$REPO_ROOT" \
    --arg commit "$source_commit" \
    --arg status "$(repo_status_short)" \
    '{kind: $kind, path: $path, source_commit: $commit, git_status_short: $status}' \
    >> "$runner_jsonl"

  jq -n \
    --arg kind "script_directory" \
    --arg path "$SCRIPT_DIR" \
    --arg commit "$source_commit" \
    '{kind: $kind, path: $path, source_commit: $commit, role: "repo_r1_helper_artifacts"}' \
    >> "$runner_jsonl"

  jq -n \
    --arg kind "active_installed_runner" \
    --arg path "$ACTIVE_RUNNER_ROOT" \
    --arg status "$installed_status" \
    --arg manifest_path "$installed_manifest" \
    --argjson manifest "$manifest_json" \
    '{
      kind: $kind,
      path: $path,
      status: $status,
      manifest_path: (if $manifest_path == "" then null else $manifest_path end),
      manifest: $manifest
    }' \
    >> "$runner_jsonl"

  if [[ "$installed_status" == "present" ]]; then
    if [[ "$ACTIVE_RUNNER_ROOT" != "$SCRIPT_DIR" && -z "$installed_manifest" ]]; then
      jq -n \
        --arg code "installed_runner_missing_manifest" \
        --arg path "$ACTIVE_RUNNER_ROOT" \
        '{code: $code, path: $path, message: "active installed runner is outside the repo script directory and has no readable install manifest"}' \
        >> "$status_jsonl"
    elif [[ "$ACTIVE_RUNNER_ROOT" != "$SCRIPT_DIR" && "$manifest_valid" != "true" ]]; then
      jq -n \
        --arg code "installed_runner_invalid_manifest" \
        --arg path "$ACTIVE_RUNNER_ROOT" \
        --arg manifest_path "$installed_manifest" \
        '{code: $code, path: $path, manifest_path: $manifest_path, message: "active installed runner manifest is unreadable or missing required provenance fields"}' \
        >> "$status_jsonl"
    else
      if [[ "$ACTIVE_RUNNER_ROOT" != "$SCRIPT_DIR" ]]; then
        manifest_commit="$(jq -r '.source_commit // empty' <<<"$manifest_json")"
        manifest_repo="$(jq -r '.source_repo // empty' <<<"$manifest_json")"
        manifest_root="$(jq -r '.installed_root // empty' <<<"$manifest_json")"

        if [[ "$manifest_commit" != "$source_commit" ]]; then
          jq -n \
            --arg code "installed_runner_manifest_commit_mismatch" \
            --arg manifest_path "$installed_manifest" \
            --arg source_commit "$source_commit" \
            --arg manifest_commit "$manifest_commit" \
            '{
              code: $code,
              manifest_path: $manifest_path,
              source_commit: $source_commit,
              manifest_source_commit: $manifest_commit,
              message: "install manifest source commit does not match the reviewed repo commit"
            }' \
            >> "$status_jsonl"
        fi

        if [[ "$manifest_repo" != "$REPO_ROOT" ]]; then
          jq -n \
            --arg code "installed_runner_manifest_repo_mismatch" \
            --arg manifest_path "$installed_manifest" \
            --arg source_repo "$REPO_ROOT" \
            --arg manifest_repo "$manifest_repo" \
            '{
              code: $code,
              manifest_path: $manifest_path,
              source_repo: $source_repo,
              manifest_source_repo: $manifest_repo,
              message: "install manifest source repo does not match the reviewed repo path"
            }' \
            >> "$status_jsonl"
        fi

        if [[ "$manifest_root" != "$ACTIVE_RUNNER_ROOT" ]]; then
          jq -n \
            --arg code "installed_runner_manifest_root_mismatch" \
            --arg manifest_path "$installed_manifest" \
            --arg active_runner_root "$ACTIVE_RUNNER_ROOT" \
            --arg manifest_root "$manifest_root" \
            '{
              code: $code,
              manifest_path: $manifest_path,
              active_runner_root: $active_runner_root,
              manifest_installed_root: $manifest_root,
              message: "install manifest installed root does not match the active runner root"
            }' \
            >> "$status_jsonl"
        fi
      fi
    fi

    while IFS= read -r source_file; do
      local helper_name installed_file source_sha installed_sha installed_mtime byte_identical exists
      local manifest_helper_path manifest_helper_source_path manifest_helper_source_sha manifest_helper_installed_sha
      helper_name="$(basename "$source_file")"
      installed_file="$ACTIVE_RUNNER_ROOT/$helper_name"
      source_sha="$(sha256_file "$source_file")"
      installed_sha=""
      installed_mtime=""
      byte_identical=false
      exists=false
      manifest_helper_path=""
      manifest_helper_source_path=""
      manifest_helper_source_sha=""
      manifest_helper_installed_sha=""
      if [[ -f "$installed_file" ]]; then
        exists=true
        installed_sha="$(sha256_file "$installed_file")"
        installed_mtime="$(file_mtime_utc "$installed_file")"
        if [[ "$source_sha" == "$installed_sha" ]]; then
          byte_identical=true
        fi
      fi

      if [[ "$ACTIVE_RUNNER_ROOT" != "$SCRIPT_DIR" && "$manifest_valid" == "true" ]]; then
        manifest_helper_path="$(jq -r --arg name "$helper_name" '.helpers[]? | select(.name == $name) | .installed_path // empty' <<<"$manifest_json" | head -n 1)"
        manifest_helper_source_path="$(jq -r --arg name "$helper_name" '.helpers[]? | select(.name == $name) | .source_path // empty' <<<"$manifest_json" | head -n 1)"
        manifest_helper_source_sha="$(jq -r --arg name "$helper_name" '.helpers[]? | select(.name == $name) | .source_sha256 // empty' <<<"$manifest_json" | head -n 1)"
        manifest_helper_installed_sha="$(jq -r --arg name "$helper_name" '.helpers[]? | select(.name == $name) | .installed_sha256 // empty' <<<"$manifest_json" | head -n 1)"
      fi

      if [[ "$exists" != "true" ]]; then
        jq -n \
          --arg code "installed_runner_missing_helper" \
          --arg helper "$helper_name" \
          --arg installed "$installed_file" \
          '{code: $code, helper: $helper, installed_path: $installed, message: "active installed runner is missing a repo R1 helper"}' \
          >> "$status_jsonl"
      elif [[ "$byte_identical" != "true" ]]; then
        jq -n \
          --arg code "installed_runner_checksum_mismatch" \
          --arg helper "$helper_name" \
          --arg source "$source_file" \
          --arg installed "$installed_file" \
          --arg source_sha "$source_sha" \
          --arg installed_sha "$installed_sha" \
          '{
            code: $code,
            helper: $helper,
            source_path: $source,
            installed_path: $installed,
            source_sha256: $source_sha,
            installed_sha256: $installed_sha,
            message: "installed helper is not byte-identical to the reviewed repo artifact"
          }' \
          >> "$status_jsonl"
      fi

      if [[ "$ACTIVE_RUNNER_ROOT" != "$SCRIPT_DIR" && "$manifest_valid" == "true" ]]; then
        if [[ -z "$manifest_helper_path" ]]; then
          jq -n \
            --arg code "installed_runner_manifest_missing_helper" \
            --arg helper "$helper_name" \
            --arg manifest_path "$installed_manifest" \
            '{
              code: $code,
              helper: $helper,
              manifest_path: $manifest_path,
              message: "install manifest is missing a helper entry for a repo R1 script"
            }' \
            >> "$status_jsonl"
        else
          if [[ "$manifest_helper_path" != "$installed_file" ]]; then
            jq -n \
              --arg code "installed_runner_manifest_helper_path_mismatch" \
              --arg helper "$helper_name" \
              --arg manifest_path "$installed_manifest" \
              --arg manifest_installed_path "$manifest_helper_path" \
              --arg installed_path "$installed_file" \
              '{
                code: $code,
                helper: $helper,
                manifest_path: $manifest_path,
                manifest_installed_path: $manifest_installed_path,
                installed_path: $installed_path,
                message: "install manifest helper path does not match the active helper path"
              }' \
              >> "$status_jsonl"
          fi
          if [[ "$manifest_helper_source_path" != "$source_file" ]]; then
            jq -n \
              --arg code "installed_runner_manifest_source_path_mismatch" \
              --arg helper "$helper_name" \
              --arg manifest_path "$installed_manifest" \
              --arg manifest_source_path "$manifest_helper_source_path" \
              --arg source_path "$source_file" \
              '{
                code: $code,
                helper: $helper,
                manifest_path: $manifest_path,
                manifest_source_path: $manifest_source_path,
                source_path: $source_path,
                message: "install manifest source path does not match the reviewed repo helper path"
              }' \
              >> "$status_jsonl"
          fi
          if [[ "$manifest_helper_source_sha" != "$source_sha" ]]; then
            jq -n \
              --arg code "installed_runner_manifest_source_checksum_mismatch" \
              --arg helper "$helper_name" \
              --arg manifest_path "$installed_manifest" \
              --arg manifest_source_sha "$manifest_helper_source_sha" \
              --arg source_sha "$source_sha" \
              '{
                code: $code,
                helper: $helper,
                manifest_path: $manifest_path,
                manifest_source_sha256: $manifest_source_sha,
                source_sha256: $source_sha,
                message: "install manifest source checksum does not match the reviewed repo helper artifact"
              }' \
              >> "$status_jsonl"
          fi
          if [[ "$exists" == "true" && "$manifest_helper_installed_sha" != "$installed_sha" ]]; then
            jq -n \
              --arg code "installed_runner_manifest_installed_checksum_mismatch" \
              --arg helper "$helper_name" \
              --arg manifest_path "$installed_manifest" \
              --arg manifest_installed_sha "$manifest_helper_installed_sha" \
              --arg installed_sha "$installed_sha" \
              '{
                code: $code,
                helper: $helper,
                manifest_path: $manifest_path,
                manifest_installed_sha256: $manifest_installed_sha,
                installed_sha256: $installed_sha,
                message: "install manifest installed checksum does not match the active helper artifact"
              }' \
              >> "$status_jsonl"
          fi
        fi
      fi

      jq -n \
        --arg helper "$helper_name" \
        --arg source "$source_file" \
        --arg installed "$installed_file" \
        --arg source_commit "$source_commit" \
        --arg source_sha "$source_sha" \
        --arg installed_sha "$installed_sha" \
        --arg installed_mtime "$installed_mtime" \
        --arg manifest_installed_path "$manifest_helper_path" \
        --arg manifest_source_path "$manifest_helper_source_path" \
        --arg manifest_source_sha "$manifest_helper_source_sha" \
        --arg manifest_installed_sha "$manifest_helper_installed_sha" \
        --argjson exists "$exists" \
        --argjson byte_identical "$byte_identical" \
        '{
          helper: $helper,
          source_path: $source,
          source_commit: $source_commit,
          source_sha256: $source_sha,
          installed_path: $installed,
          installed_exists: $exists,
          installed_sha256: (if $installed_sha == "" then null else $installed_sha end),
          installed_mtime_utc: (if $installed_mtime == "" then null else $installed_mtime end),
          manifest_installed_path: (if $manifest_installed_path == "" then null else $manifest_installed_path end),
          manifest_source_path: (if $manifest_source_path == "" then null else $manifest_source_path end),
          manifest_source_sha256: (if $manifest_source_sha == "" then null else $manifest_source_sha end),
          manifest_installed_sha256: (if $manifest_installed_sha == "" then null else $manifest_installed_sha end),
          byte_identical: $byte_identical
        }' \
        >> "$helper_jsonl"
    done < <(find "$SCRIPT_DIR" -maxdepth 1 -type f -name '*.sh' | sort)
  fi

  status_count="$(wc -l < "$status_jsonl" | tr -d ' ')"
  if [[ "$status_count" -eq 0 ]]; then
    parity_status="pass"
  else
    parity_status="fail"
  fi

  jq -n \
    --arg run_id "$RUN_ID" \
    --arg source_commit "$source_commit" \
    --arg source_repo "$REPO_ROOT" \
    --arg script_dir "$SCRIPT_DIR" \
    --arg active_runner_root "$ACTIVE_RUNNER_ROOT" \
    --arg active_runner_status "$installed_status" \
    --arg active_runner_manifest "$installed_manifest" \
    --arg parity_status "$parity_status" \
    --argjson manifest "$manifest_json" \
    --slurpfile runtime_entrypoint_inventory "$runner_jsonl" \
    --slurpfile helper_artifact_parity "$helper_jsonl" \
    --slurpfile failures "$status_jsonl" \
    '{
      run_id: $run_id,
      product_rule: "active runner and helper scripts must match repo artifacts or audited installed artifacts generated from the same source commit",
      source_repo: $source_repo,
      source_commit: $source_commit,
      script_dir: $script_dir,
      active_runner_root: (if $active_runner_root == "" then null else $active_runner_root end),
      active_runner_status: $active_runner_status,
      active_runner_manifest_path: (if $active_runner_manifest == "" then null else $active_runner_manifest end),
      active_runner_manifest: $manifest,
      active_runner_manifest_valid: ($manifest != null and ($manifest.parse_error // false | not) and ($manifest.manifest_version // "") == "primeradiant_r1_install_v1"),
      parity_status: $parity_status,
      failure_count: ($failures | length),
      runtime_entrypoint_inventory: $runtime_entrypoint_inventory,
      helper_artifact_parity: $helper_artifact_parity,
      failures: $failures
    }' > "$output_path"
}

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

ACTIVE_RUNNER_PARITY_REPORT="$STATE_ROOT/$RUN_ID-active-runner-parity.json"
build_runner_parity_report "$ACTIVE_RUNNER_PARITY_REPORT"
ACTIVE_RUNNER_PARITY_STATUS="$(jq -r '.parity_status' "$ACTIVE_RUNNER_PARITY_REPORT")"
if [[ "$ACTIVE_RUNNER_PARITY_STATUS" != "pass" ]]; then
  FAILED_REPORT="$STATE_ROOT/$RUN_ID-failed-report.json"
  jq -n \
    --arg run_id "$RUN_ID" \
    --arg source_db "$SOURCE_DB" \
    --arg eurisko_target "$EURISKO_TARGET" \
    --arg eurisko_repo "$EURISKO_REPO" \
    --arg active_runner_parity_report "$ACTIVE_RUNNER_PARITY_REPORT" \
    --slurpfile active_runner_parity "$ACTIVE_RUNNER_PARITY_REPORT" \
    '{
      run_id: $run_id,
      source_db_path: $source_db,
      source_host: "tars",
      consume_host: "eurisko",
      source_mode: "live_subspace_daemon_watcher_once",
      status: "active_runner_artifact_parity_failed",
      failed_stage: "active_runner_artifact_parity",
      eurisko_target: $eurisko_target,
      eurisko_repo: $eurisko_repo,
      active_runner_parity_report_path: $active_runner_parity_report,
      active_runner_parity: $active_runner_parity[0],
      error: {
        command: "active runner artifact parity inventory",
        exit_status: 1,
        message: "active installed runner/helper artifacts differ from the reviewed repo artifact or lack auditable provenance"
      }
    }' > "$FAILED_REPORT"
  printf "active runner artifact parity failed; report: %s\n" "$FAILED_REPORT" >&2
  exit 1
fi

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
  --arg active_runner_parity_report "$ACTIVE_RUNNER_PARITY_REPORT" \
  --argjson catchup_count "$CATCHUP_COUNT" \
  --arg eurisko_target "$EURISKO_TARGET" \
  --arg eurisko_repo "$EURISKO_REPO" \
  --arg eurisko_handoff_root "$EURISKO_HANDOFF_ROOT" \
  --arg eurisko_run_root "$EURISKO_RUN_ROOT" \
  --arg eurisko_soup_db "$EURISKO_SOUP_DB" \
  --slurpfile active_runner_parity "$ACTIVE_RUNNER_PARITY_REPORT" \
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
    ordinary_runner_command: "live_subspace_daemon_watcher_once.sh",
    active_runner_parity_report_path: $active_runner_parity_report,
    active_runner_parity: $active_runner_parity[0],
    cursor_catchup_before_wait_count: $catchup_count,
    local_watch_report: $watch_report,
    consumed: .
  }' "$CONSUMED_JSONL" > "$REPORT"

printf "%s\n" "$REPORT"
