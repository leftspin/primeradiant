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

Ingest uveal cancer research items from an existing Gibson morning-research
metadata artifact into Prime Radiant soup:

```sh
mix primeradiant.uveal_research_soup_ingest \
  --meta /Users/mike/.openclaw/workspace/www/news/2026-06-14-vr-ai-news-morning.meta.json \
  --soup-db /tmp/primeradiant-uveal-research-soup.sqlite3 \
  --tenant 00000000-0000-0000-0000-00000000t328
```

The adapter reads only `category = "cancer"` items from the morning metadata,
maps each research item into `manual_real_ingest_v1`, and persists through
`Primeradiant.StorageHarness.RealIngestion.ingest_items/2` plus
`DurableSoupDb.persist!/3`. It does not mutate cron, source artifacts, Reporter,
or service persistence; wiring Gibson/cron/runtime activation to this command is
a separate approved deploy step.

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

Watcher-driven importer proof, still without installing a service:

```sh
scripts/r1/watch_sqlite_wakeup.sh \
  --source-db /Volumes/BlipsAndChitz/news-storage/news.db \
  --tenant 00000000-0000-0000-0000-00000000t328 \
  --cursor-file /tmp/primeradiant-t328-cursor.txt \
  --package-root /tmp/primeradiant-t328-packages \
  --run-root /tmp/primeradiant-t328-runs
```

The watcher treats `news.db`, `news.db-wal`, and `news.db-shm` file changes only
as a wakeup bell. It then runs the normal read-only cursor importer; cursoring,
dedupe, row reads, raw digest checks, and story decisions remain Primeradiant
work. The file event is not a story payload and is not trusted as story fact.

Reporter-facing Prime Radiant soup API:

```sh
mix primeradiant.soup_api \
  --soup-db /home/clu/.local/state/primeradiant/soup-api/soup.sqlite3 \
  --tenant 00000000-0000-0000-0000-00000000t328 \
  --token <internal-service-token> \
  --ack-log /home/clu/.local/state/primeradiant/soup-api/acks.jsonl \
  --ip 0.0.0.0 \
  --port 4084
```

The canonical internal HTTP JSON surface is `/api/v1/soup` with Bearer service
auth. It exposes `ready`, `feed`, `delta`, and `ack` for Reporter/News to read
Prime Radiant-owned soup material without binding to run directories, SQLite
internals, proof harness artifacts, daemon-news cursors, or authored projection
framing.

The live Subspace daemon watcher must write to the same stable soup DB path that
the API serves. This is the default EURISKO soup DB target; pass it explicitly
when proving or operating the runtime:

```sh
scripts/r1/live_subspace_daemon_watcher_once.sh \
  --source-db /Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3 \
  --tenant 00000000-0000-0000-0000-00000000t328 \
  --state-root /Users/mike/.local/state/primeradiant/t328-live-watcher \
  --eurisko-target clu@eurisko \
  --eurisko-repo /home/clu/src/primeradiant \
  --eurisko-soup-db /home/clu/.local/state/primeradiant/soup-api/soup.sqlite3
```

Per-run soup DBs remain useful as bounded proof artifacts, but they are not the
Reporter product surface.

The T1430 production deploy path is the source-controlled TARS LaunchAgent in
`deploy/tars/`: its Node-backed watcher reads Microverse on TARS and pushes
bounded packages to EURISKO. The matching recovery and proof procedure is
`../specs/primeradiant-t1430-fresh-news-admission-approval-runbook.md`. Do not
replace it with a EURISKO-local daemon DB watcher.

Any other launchd, systemd, cron, autostart, or permanent service registration
remains a separate deploy step unless a supported product deploy path explicitly
owns that runtime shape.
