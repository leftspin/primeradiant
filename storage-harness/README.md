# Primeradiant Storage Harness

Bounded Elixir/Ecto storage proof for `specs/primeradiant-storage-schema-hardening.html`.

Run from this directory:

```sh
mix test
```

The default test gate uses production-shaped Ecto schemas and a migration-equivalent Postgres SQL file without requiring a live Postgres server. Set `PRIMERADIANT_STORAGE_*` environment variables before using `Primeradiant.StorageHarness.Repo` against a real database.

When local Postgres tooling is already available, run `scripts/validate_postgres_schema.sh` to create an isolated temporary database, apply `priv/repo/migrations/20260517000000_create_storage_schema.sql`, run targeted trigger/constraint checks, and drop the database.

The real-ingestion slice starts at `Primeradiant.StorageHarness.RealIngestion.ingest_items/2`. It admits manual real source items without fixture IDs or trusted story hints, normalizes generic span-backed facts/entities/questions/colors, retrieves visible candidate stories, makes bounded story-identity decisions, and maps accepted decisions through proposal/decision/graph-commit storage rows.

Consume a daemon-news committed-source-item event envelope without mutating source
storage:

```sh
scripts/r1/emit_committed_event.sh \
  --source-db /Volumes/BlipsAndChitz/news-storage/news.db \
  --message-id <message-id> \
  --tenant 00000000-0000-0000-0000-000000000001 \
  > /tmp/daemon-news-event.json

mix primeradiant.daemon_news_event \
  --event /tmp/daemon-news-event.json \
  --soup-db /tmp/primeradiant-event-soup.sqlite3 \
  --tenant 00000000-0000-0000-0000-000000000001
```

The event must carry source identity, cursor, raw archive reference, raw digest,
and ACL. Primeradiant resolves the scoped source bytes through the adapter,
admits the item immediately, and writes only primeradiant-owned soup/output rows.
The emitter script is a read-only, one-shot proof helper. It does not install,
register, or enable a persistent source publisher.

Replay already-stored daemon news rows without mutating the source SQLite
database:

```sh
mix primeradiant.daemon_news_replay --db /Volumes/BlipsAndChitz/news-storage/news.db --soup-db /tmp/primeradiant-soup.sqlite3 --tenant 00000000-0000-0000-0000-000000000001
```

The command reads `messages` with SQLite read-only mode, maps supported `swarm.channel.news.report.v0` rows into the same real-ingestion admission path, writes Primeradiant-owned soup tables to `--soup-db`, and prints a changed-stories-with-evidence JSON report generated back from that persisted soup database. This is replay for correctness, not the product trigger.

Manual TARS -> EURISKO transport, still without any persistent service:

```sh
scripts/r1/tars_to_eurisko_handoff.sh \
  --source-db /Volumes/BlipsAndChitz/news-storage/news.db \
  --eurisko-handoff-root /home/clu/primeradiant-r1-handoffs
```

Then consume it on EURISKO from the staged checkout:

```sh
scripts/r1/consume_handoff.sh \
  --handoff-dir /home/clu/primeradiant-r1-handoffs/<run-id> \
  --run-root /home/clu/primeradiant-r1-runs \
  --tenant 00000000-0000-0000-0000-00000000t328
```

The R1 product path is event-driven. Any persistent source-side publisher,
watcher, queue, launchd, systemd, cron, or autostart step remains a separate
Flynn approval gate.
