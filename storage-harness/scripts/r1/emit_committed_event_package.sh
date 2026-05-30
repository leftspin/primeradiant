#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  emit_committed_event_package.sh --source-db PATH --message-id ID --tenant TENANT --package-dir DIR [--event-id ID]

Reads one daemon-news message row with sqlite3 -readonly, copies only that row's
referenced raw envelope bytes into DIR/raw/source-envelope.json, and writes a
primeradiant.source.committed_item.v1 event to DIR/event.json. This is an
event-package handoff helper, not a polling loop or persistent service.
USAGE
}

SOURCE_DB=""
MESSAGE_ID=""
TENANT=""
PACKAGE_DIR=""
EVENT_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --message-id) MESSAGE_ID="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --package-dir) PACKAGE_DIR="$2"; shift 2 ;;
    --event-id) EVENT_ID="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$MESSAGE_ID" || -z "$TENANT" || -z "$PACKAGE_DIR" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -r "$SOURCE_DB" ]]; then
  echo "source DB is not readable: $SOURCE_DB" >&2
  exit 1
fi

if [[ -z "$EVENT_ID" ]]; then
  EVENT_ID="evt-$MESSAGE_ID"
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

ROW_JSON="$(
  sqlite3 -readonly "$SOURCE_DB" "
    SELECT json_object(
      'message_id', message_id,
      'source_space', source_space,
      'receptor_id', receptor_id,
      'sender_id', sender_id,
      'message_type', message_type,
      'created_at', created_at,
      'received_at', received_at,
      'raw_archive_path', raw_archive_path,
      'raw_archive_offset', raw_archive_offset,
      'raw_archive_length', raw_archive_length,
      'raw_sha256', raw_sha256
    )
    FROM messages
    WHERE message_id = '$(sql_escape "$MESSAGE_ID")'
      AND message_type = 'swarm.channel.news.report.v0'
    LIMIT 1;
  "
)"

if [[ -z "$ROW_JSON" ]]; then
  echo "source DB has no swarm.channel.news.report.v0 row for message_id: $MESSAGE_ID" >&2
  exit 1
fi

RAW_ARCHIVE_PATH="$(jq -r '.raw_archive_path' <<<"$ROW_JSON")"
RAW_OFFSET="$(jq -r '.raw_archive_offset' <<<"$ROW_JSON")"
RAW_LENGTH="$(jq -r '.raw_archive_length' <<<"$ROW_JSON")"
RAW_SHA="$(jq -r '.raw_sha256' <<<"$ROW_JSON")"

case "$RAW_ARCHIVE_PATH" in
  /*) RESOLVED_RAW_ARCHIVE="$RAW_ARCHIVE_PATH" ;;
  *) RESOLVED_RAW_ARCHIVE="$(cd "$(dirname "$SOURCE_DB")" && pwd)/$RAW_ARCHIVE_PATH" ;;
esac

if [[ ! -r "$RESOLVED_RAW_ARCHIVE" ]]; then
  echo "raw archive is not readable: $RESOLVED_RAW_ARCHIVE" >&2
  exit 1
fi

mkdir -p "$PACKAGE_DIR/raw"
RAW_OUT="$PACKAGE_DIR/raw/source-envelope.json"
EVENT_OUT="$PACKAGE_DIR/event.json"

dd if="$RESOLVED_RAW_ARCHIVE" of="$RAW_OUT" bs=1 skip="$RAW_OFFSET" count="$RAW_LENGTH" status=none

ACTUAL_SHA="$(shasum -a 256 "$RAW_OUT" | awk '{print $1}')"
if [[ "$ACTUAL_SHA" != "$RAW_SHA" ]]; then
  echo "raw digest mismatch for message_id $MESSAGE_ID: expected $RAW_SHA got $ACTUAL_SHA" >&2
  exit 1
fi

jq -n \
  --argjson row "$ROW_JSON" \
  --arg event_id "$EVENT_ID" \
  --arg tenant "$TENANT" \
  --arg emitted_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg raw_path "raw/source-envelope.json" \
  --argjson raw_length "$RAW_LENGTH" \
  '{
    event_type: "primeradiant.source.committed_item.v1",
    event_id: $event_id,
    cursor: ("messages:" + $row.message_id),
    emitted_at: $emitted_at,
    source: {
      adapter: "daemon-news",
      tenant_id: $tenant,
      item_id: $row.message_id,
      message_type: $row.message_type,
      source_space: $row.source_space,
      receptor_id: $row.receptor_id,
      sender_id: $row.sender_id,
      created_at: $row.created_at,
      committed_at: $row.received_at
    },
    raw_ref: {
      path: $raw_path,
      offset: 0,
      length: $raw_length,
      sha256: $row.raw_sha256
    },
    acl: {
      privacy: "public"
    }
  }' > "$EVENT_OUT"

printf "%s\n" "$PACKAGE_DIR"
