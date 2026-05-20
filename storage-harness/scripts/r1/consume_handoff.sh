#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  consume_handoff.sh --handoff-dir PATH --run-root PATH --tenant TENANT [--actor ACTOR]

Consumes a manual/test-scoped R1 handoff snapshot and writes primeradiant-owned
soup.sqlite3 plus changed-stories-report.json under RUN_ROOT/<run-id>/.
USAGE
}

HANDOFF_DIR=""
RUN_ROOT=""
TENANT=""
ACTOR="flynn"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --handoff-dir) HANDOFF_DIR="$2"; shift 2 ;;
    --run-root) RUN_ROOT="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$HANDOFF_DIR" || -z "$RUN_ROOT" || -z "$TENANT" ]]; then
  usage >&2
  exit 2
fi

MANIFEST="$HANDOFF_DIR/manifest.json"
SNAPSHOT_DB="$HANDOFF_DIR/news.db"

if [[ ! -r "$MANIFEST" || ! -r "$SNAPSHOT_DB" ]]; then
  echo "handoff manifest/news.db not readable under: $HANDOFF_DIR" >&2
  exit 1
fi

RUN_ID="$(basename "$HANDOFF_DIR")"
OUT_DIR="$RUN_ROOT/$RUN_ID"
mkdir -p "$OUT_DIR"

SOUP_DB="$OUT_DIR/soup.sqlite3"
REPORT="$OUT_DIR/changed-stories-report.json"

/opt/homebrew/bin/mix primeradiant.daemon_news_replay \
  --db "$SNAPSHOT_DB" \
  --raw-root "$HANDOFF_DIR" \
  --soup-db "$SOUP_DB" \
  --tenant "$TENANT" \
  --actor "$ACTOR" > "$REPORT"

jq '. + {r1_handoff: {handoff_dir: $handoff, manifest_path: $manifest, run_output_dir: $out, persistent_service_installed: false}}' \
  --arg handoff "$HANDOFF_DIR" \
  --arg manifest "$MANIFEST" \
  --arg out "$OUT_DIR" \
  "$REPORT" > "$REPORT.tmp"
mv "$REPORT.tmp" "$REPORT"

echo "$OUT_DIR"
