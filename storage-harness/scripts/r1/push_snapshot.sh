#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  push_snapshot.sh --source-db PATH --handoff-root PATH [--run-id ID] [--limit N]

Creates a manual/test-scoped R1 daemon-news handoff snapshot. The source DB is
opened only with sqlite3 -readonly. The handoff copy is primeradiant-owned and
has raw_archive_path rewritten to local raw/... files for EURISKO consumption.
USAGE
}

SOURCE_DB=""
HANDOFF_ROOT=""
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LIMIT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db) SOURCE_DB="$2"; shift 2 ;;
    --handoff-root) HANDOFF_ROOT="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$SOURCE_DB" || -z "$HANDOFF_ROOT" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -r "$SOURCE_DB" ]]; then
  echo "source DB is not readable: $SOURCE_DB" >&2
  exit 1
fi

ROW_COUNT="$(sqlite3 -readonly "$SOURCE_DB" "select count(*) from messages where message_type='swarm.channel.news.report.v0';")"
if [[ "$ROW_COUNT" == "0" ]]; then
  echo "source DB has no swarm.channel.news.report.v0 rows: $SOURCE_DB" >&2
  exit 1
fi

RUN_DIR="$HANDOFF_ROOT/$RUN_ID"
RAW_DIR="$RUN_DIR/raw"
mkdir -p "$RAW_DIR"

cp -p "$SOURCE_DB" "$RUN_DIR/news.db"

SQL_LIMIT=""
if [[ -n "$LIMIT" ]]; then
  SQL_LIMIT="limit $LIMIT"
fi

sqlite3 -readonly "$SOURCE_DB" \
  "select distinct raw_archive_path from messages where message_type='swarm.channel.news.report.v0' order by raw_archive_path $SQL_LIMIT;" |
while IFS= read -r raw_path; do
  [[ -z "$raw_path" ]] && continue
  if [[ ! -r "$raw_path" ]]; then
    echo "raw archive is not readable: $raw_path" >&2
    exit 1
  fi

  raw_hash="$(printf '%s' "$raw_path" | shasum -a 256 | awk '{print $1}')"
  raw_name="$raw_hash-$(basename "$raw_path")"
  cp -p "$raw_path" "$RAW_DIR/$raw_name"
  sqlite3 "$RUN_DIR/news.db" \
    "update messages set raw_archive_path = 'raw/$raw_name' where raw_archive_path = '$(printf "%s" "$raw_path" | sed "s/'/''/g")';"
done

cat > "$RUN_DIR/manifest.json" <<JSON
{
  "handoff_version": "primeradiant_daemon_news_r1_v1",
  "run_id": "$RUN_ID",
  "source_db_path": "$SOURCE_DB",
  "snapshot_db_path": "$RUN_DIR/news.db",
  "raw_root": "$RUN_DIR",
  "message_type": "swarm.channel.news.report.v0",
  "source_row_count": $ROW_COUNT,
  "source_db_mode": "read_only",
  "persistent_service_installed": false,
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
JSON

echo "$RUN_DIR"
