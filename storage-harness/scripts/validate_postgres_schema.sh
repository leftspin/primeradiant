#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGHOST="${PGHOST:-/tmp}"
PGDATABASE_TEMPLATE="${PGDATABASE_TEMPLATE:-postgres}"
PSQL="${PSQL:-$(command -v psql || true)}"
CREATEDB="${CREATEDB:-$(command -v createdb || true)}"
DROPDB="${DROPDB:-$(command -v dropdb || true)}"
DB_NAME="${PRIMERADIANT_STORAGE_VALIDATION_DB:-primeradiant_storage_validation_$$}"

if [[ -z "$PSQL" && -x /opt/homebrew/Cellar/postgresql@16/16.13/bin/psql ]]; then
  PSQL="/opt/homebrew/Cellar/postgresql@16/16.13/bin/psql"
fi
if [[ -z "$CREATEDB" && -x /opt/homebrew/Cellar/postgresql@16/16.13/bin/createdb ]]; then
  CREATEDB="/opt/homebrew/Cellar/postgresql@16/16.13/bin/createdb"
fi
if [[ -z "$DROPDB" && -x /opt/homebrew/Cellar/postgresql@16/16.13/bin/dropdb ]]; then
  DROPDB="/opt/homebrew/Cellar/postgresql@16/16.13/bin/dropdb"
fi

if [[ -z "$PSQL" || -z "$CREATEDB" || -z "$DROPDB" ]]; then
  echo "psql/createdb/dropdb not found; set PSQL, CREATEDB, and DROPDB" >&2
  exit 127
fi

cleanup() {
  "$DROPDB" -h "$PGHOST" --if-exists "$DB_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$PSQL" -h "$PGHOST" -d "$PGDATABASE_TEMPLATE" -Atc "select version();" >/dev/null
"$CREATEDB" -h "$PGHOST" "$DB_NAME"
"$PSQL" -h "$PGHOST" -v ON_ERROR_STOP=1 -d "$DB_NAME" \
  < "$ROOT/priv/repo/migrations/20260517000000_create_storage_schema.sql" >/dev/null
"$PSQL" -h "$PGHOST" -v ON_ERROR_STOP=1 -d "$DB_NAME" \
  < "$ROOT/priv/repo/validation/postgres_schema_checks.sql"

echo "validated $DB_NAME on host $PGHOST"
