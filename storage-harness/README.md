# Primeradiant Storage Harness

Bounded Elixir/Ecto storage proof for `specs/primeradiant-storage-schema-hardening.html`.

Run from this directory:

```sh
mix test
```

The default test gate uses production-shaped Ecto schemas and a migration-equivalent Postgres SQL file without requiring a live Postgres server. Set `PRIMERADIANT_STORAGE_*` environment variables before using `Primeradiant.StorageHarness.Repo` against a real database.

When local Postgres tooling is already available, run `scripts/validate_postgres_schema.sh` to create an isolated temporary database, apply `priv/repo/migrations/20260517000000_create_storage_schema.sql`, run targeted trigger/constraint checks, and drop the database.

The real-ingestion slice starts at `Primeradiant.StorageHarness.RealIngestion.ingest_items/2`. It admits manual real source items without fixture IDs or trusted story hints, normalizes generic span-backed facts/entities/questions/colors, retrieves visible candidate stories, makes bounded story-identity decisions, and maps accepted decisions through proposal/decision/graph-commit storage rows.
