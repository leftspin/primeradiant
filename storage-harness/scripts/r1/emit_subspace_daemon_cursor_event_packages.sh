#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  emit_subspace_daemon_cursor_event_packages.sh --source-db PATH --tenant TENANT --package-root DIR [--after-cursor ACCEPTED_AT|ID] [--limit N] [--run-id ID]

Reads Subspace daemon_event news envelopes after a Primeradiant-owned cursor with
sqlite3 -readonly and emits bounded committed-source-item event packages. The
daemon SQLite DB remains source-owned; this writes only Primeradiant-owned
handoff packages and a manifest under DIR.
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
  RUN_ID="subspace-cursor-$(date -u +%Y%m%dT%H%M%SZ)"
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

AFTER_ACCEPTED=""
AFTER_ID=""
if [[ -n "$AFTER_CURSOR" ]]; then
  AFTER_ACCEPTED="${AFTER_CURSOR%%|*}"
  AFTER_ID="${AFTER_CURSOR#*|}"
  if [[ "$AFTER_ACCEPTED" == "$AFTER_ID" ]]; then
    echo "after cursor must be ACCEPTED_AT|ID" >&2
    exit 2
  fi
fi

WHERE="json_valid(text) AND json_type(text, '$.body') = 'object'"
if [[ -n "$AFTER_CURSOR" ]]; then
  WHERE="$WHERE AND (accepted_at > '$(sql_escape "$AFTER_ACCEPTED")' OR (accepted_at = '$(sql_escape "$AFTER_ACCEPTED")' AND id > $(sql_escape "$AFTER_ID")))"
fi

mkdir -p "$PACKAGE_ROOT/packages"
ROWS_JSON="$PACKAGE_ROOT/cursor-rows.json"
PACKAGES_JSONL="$PACKAGE_ROOT/packages.jsonl"
: > "$PACKAGES_JSONL"

sqlite3 -readonly -cmd ".timeout 10000" -json "$SOURCE_DB" "
  SELECT id, message_id, message_timestamp, inbound_event, author_id, author_name,
         accepted_at, text
    FROM daemon_event
   WHERE $WHERE
   ORDER BY accepted_at ASC, id ASC
   LIMIT $LIMIT;
" > "$ROWS_JSON"

COUNT=0
NEXT_CURSOR="$AFTER_CURSOR"

while IFS= read -r ROW_JSON; do
  [[ -z "$ROW_JSON" ]] && continue
  COUNT=$((COUNT + 1))

  ROW_ID="$(jq -r '.id' <<<"$ROW_JSON")"
  MESSAGE_ID="$(jq -r '.message_id' <<<"$ROW_JSON")"
  ACCEPTED_AT="$(jq -r '.accepted_at' <<<"$ROW_JSON")"
  PACKAGE_DIR="$PACKAGE_ROOT/packages/$(printf '%04d' "$COUNT")-$MESSAGE_ID"
  EVENT_ID="$RUN_ID-$COUNT"
  mkdir -p "$PACKAGE_DIR/raw"

  RAW_OUT="$PACKAGE_DIR/raw/source-envelope.json"
  EVENT_OUT="$PACKAGE_DIR/event.json"
  jq -r '.text' <<<"$ROW_JSON" > "$RAW_OUT"
  RAW_SHA="$(shasum -a 256 "$RAW_OUT" | awk '{print $1}')"
  RAW_LENGTH="$(wc -c < "$RAW_OUT" | tr -d ' ')"

  jq -n \
    --argjson row "$ROW_JSON" \
    --arg event_id "$EVENT_ID" \
    --arg tenant "$TENANT" \
    --arg emitted_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg raw_path "raw/source-envelope.json" \
    --arg raw_sha "$RAW_SHA" \
    --argjson raw_length "$RAW_LENGTH" \
    '{
      event_type: "primeradiant.source.committed_item.v1",
      event_id: $event_id,
      cursor: ("daemon_event:" + ($row.accepted_at|tostring) + "|" + ($row.id|tostring)),
      emitted_at: $emitted_at,
      source: {
        adapter: "daemon-news",
        tenant_id: $tenant,
        item_id: $row.message_id,
        message_type: "swarm.channel.news.report.v0",
        source_space: "subspace.swarm.channel",
        receptor_id: "subspace-daemon",
        sender_id: $row.author_id,
        created_at: $row.message_timestamp,
        committed_at: $row.accepted_at,
        daemon_event_id: $row.id,
        daemon_inbound_event: $row.inbound_event,
        author_name: $row.author_name
      },
      raw_ref: {
        path: $raw_path,
        offset: 0,
        length: $raw_length,
        sha256: $raw_sha
      },
      acl: {
        privacy: "public"
      }
    }' > "$EVENT_OUT"

  NEXT_CURSOR="$ACCEPTED_AT|$ROW_ID"
  jq -n \
    --arg message_id "$MESSAGE_ID" \
    --arg accepted_at "$ACCEPTED_AT" \
    --argjson daemon_event_id "$ROW_ID" \
    --arg event_id "$EVENT_ID" \
    --arg package_dir "$PACKAGE_DIR" \
    --arg cursor "$NEXT_CURSOR" \
    '{message_id: $message_id, accepted_at: $accepted_at, daemon_event_id: $daemon_event_id, event_id: $event_id, package_dir: $package_dir, cursor: $cursor}' \
    >> "$PACKAGES_JSONL"
done < <(jq -c '.[]' "$ROWS_JSON")

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
    source_mode: "subspace_daemon_read_only_db_cursor",
    after_cursor: $after_cursor,
    next_cursor: $next_cursor,
    emitted_count: $count,
    persistent_service_installed: false,
    packages: .
  }' "$PACKAGES_JSONL" > "$PACKAGE_ROOT/manifest.json"

printf "%s\n" "$PACKAGE_ROOT/manifest.json"
