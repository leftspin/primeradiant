#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  emit_committed_event.sh --source-db PATH --message-id ID --tenant TENANT [--event-id ID]

Reads one daemon-news message row with sqlite3 -readonly and emits a
primeradiant.source.committed_item.v1 envelope to stdout. This is a no-install
source-side proof helper; it does not register a service, mutate source storage,
or start a persistent publisher.
USAGE
}

SOURCE_DB=""
MESSAGE_ID=""
TENANT=""
EVENT_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --message-id) MESSAGE_ID="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --event-id) EVENT_ID="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$MESSAGE_ID" || -z "$TENANT" ]]; then
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

jq -n \
  --argjson row "$ROW_JSON" \
  --arg event_id "$EVENT_ID" \
  --arg tenant "$TENANT" \
  --arg emitted_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
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
      path: $row.raw_archive_path,
      offset: $row.raw_archive_offset,
      length: $row.raw_archive_length,
      sha256: $row.raw_sha256
    },
    acl: {
      privacy: "public"
    }
  }'
