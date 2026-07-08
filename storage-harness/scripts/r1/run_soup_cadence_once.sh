#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
run_soup_cadence_once.sh [--env-file PATH] [--repo DIR] [--actor ACTOR] [--cadence CADENCE] [--limit N]

Runs one bounded Prime Radiant recurring soup cadence pass against the served
soup DB declared by the soup API environment file.
USAGE
}

ENV_FILE="${PRIMERADIANT_SOUP_CADENCE_ENV_FILE:-$HOME/.local/state/primeradiant/soup-api/env}"
REPO="${PRIMERADIANT_SOUP_CADENCE_REPO:-$HOME/src/primeradiant/storage-harness}"
ACTOR="${PRIMERADIANT_SOUP_CADENCE_ACTOR:-prime-radiant-cadence-scheduler}"
CADENCE="${PRIMERADIANT_SOUP_CADENCE_CADENCE:-active_story_transform_detect_link_15m}"
LIMIT="${PRIMERADIANT_SOUP_CADENCE_LIMIT:-8}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --actor) ACTOR="$2"; shift 2 ;;
    --cadence) CADENCE="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

if [[ ! -r "$ENV_FILE" ]]; then
  echo "Prime Radiant cadence env file is not readable: $ENV_FILE" >&2
  exit 1
fi

if [[ ! -d "$REPO" ]]; then
  echo "Prime Radiant storage-harness repo is missing: $REPO" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

: "${PRIMERADIANT_SOUP_API_SOUP_DB:?PRIMERADIANT_SOUP_API_SOUP_DB is required}"
: "${PRIMERADIANT_SOUP_API_TENANT:?PRIMERADIANT_SOUP_API_TENANT is required}"

cd "$REPO"

export PATH="/home/clu/.local/share/mise/installs/elixir/1.19.5-otp-28/bin:/home/clu/.local/share/mise/installs/erlang/28.5/bin:/home/clu/.local/share/mise/installs/sqlite/3.45.1/bin:$PATH"

exec mix primeradiant.soup_cadence_once \
  --soup-db "$PRIMERADIANT_SOUP_API_SOUP_DB" \
  --tenant "$PRIMERADIANT_SOUP_API_TENANT" \
  --actor "$ACTOR" \
  --cadence "$CADENCE" \
  --limit "$LIMIT"
