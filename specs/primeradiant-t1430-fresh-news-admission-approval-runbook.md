# Prime Radiant T1430 Fresh News Admission Approval Runbook

Status: approval-ready runtime repair plan, not executed.

Scope: restore the existing TARS Prime Radiant daemon-news watcher so fresh
Subspace daemon events can be admitted into the served EURISKO soup DB through
the existing event-package path.

## Root Cause

The intended Prime Radiant admission path exists:

- TARS watcher loop:
  `/Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh`
- TARS watcher label:
  `ai.primeradiant.t328-daemon-db-watcher`
- Source DB:
  `/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3`
- EURISKO consumer repo:
  `/home/clu/src/primeradiant`
- Served soup DB:
  `/home/clu/.local/state/primeradiant/soup-api/soup.sqlite3`

The watcher is not missing or pointed at the wrong soup DB. It is running on
TARS, but the launchd-started process cannot open the Microverse source DB:

```text
Error: unable to open database "/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3": authorization denied
```

The cursor is stuck at:

```text
2026-06-18 18:23:59|7765
```

The TARS daemon DB is fresh beyond that cursor, so the break is before event
package emission, inside source DB cursor read authorization for the watcher
runtime context.

## Product Boundary

This is not source fetch, source re-ingest, or periodic source import. The
approved repair must preserve the existing flow:

1. TARS daemon DB is read with `sqlite3 -readonly`.
2. Prime Radiant emits bounded committed-source-item event packages.
3. EURISKO consumes those packages into the served soup DB.
4. Story agents may run from admitted soup pressure.

Do not mutate the TARS daemon DB. Do not manually edit the EURISKO soup DB.

## Approval Request

Approve exactly one runtime repair of the existing TARS watcher authorization
context so the existing watcher can read:

```text
/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3
```

and then allow one restart of the existing watcher label:

```text
ai.primeradiant.t328-daemon-db-watcher
```

No new service, timer, cron, LaunchAgent, or LaunchDaemon is required by this
runbook.

## Pre-Repair Proof Commands

Read-only proof from TARS:

```sh
ssh tars 'set -eu
DB=/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3
echo "daemon_db=$DB"
sqlite3 -readonly "$DB" "select max(accepted_at), max(id), count(*) from daemon_event where json_valid(text) and json_type(text, '\''$.body'\'') = '\''object'\'';"
cat /Users/mike/.local/state/primeradiant/t328-live-watcher/cursor.txt
tail -n 40 /Users/mike/.local/state/primeradiant/t328-live-watcher/logs/loop.log'
```

Read-only proof from EURISKO:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko 'set -eu
set -a
. /home/clu/.local/state/primeradiant/soup-api/env
set +a
sqlite3 -readonly "$PRIMERADIANT_SOUP_API_SOUP_DB" "select max(observed_at), max(inserted_at), count(*) from inputs;"
systemctl --user show primeradiant-soup-api.service -p ActiveState -p SubState -p MainPID --no-pager'
```

## Repair Action

After explicit approval, grant the existing watcher runtime context read access
to the Microverse daemon DB path, then restart only the existing watcher label.
The exact platform-specific authorization step depends on the operator's chosen
macOS privacy/volume access mechanism; do not substitute a new scheduler.

Allowed restart after authorization:

```sh
ssh tars 'launchctl kickstart -k gui/$(id -u)/ai.primeradiant.t328-daemon-db-watcher'
```

## Post-Repair Verification

The watcher must advance the cursor beyond `2026-06-18 18:23:59|7765` and stop
emitting `authorization denied` for the source DB.

```sh
ssh tars 'set -eu
sleep 10
cat /Users/mike/.local/state/primeradiant/t328-live-watcher/cursor.txt
tail -n 80 /Users/mike/.local/state/primeradiant/t328-live-watcher/logs/loop.log'
```

EURISKO soup freshness must advance through normal package consumption:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko 'set -eu
set -a
. /home/clu/.local/state/primeradiant/soup-api/env
set +a
sqlite3 -readonly "$PRIMERADIANT_SOUP_API_SOUP_DB" "select max(observed_at), max(inserted_at), count(*) from inputs; select max(inserted_at), count(*) from story_events;"'
```

Reporter-facing ready must no longer report the stale June 18 source time after
catch-up completes:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko 'set -eu
set -a
. /home/clu/.local/state/primeradiant/soup-api/env
set +a
curl -fsS -H "Authorization: Bearer $PRIMERADIANT_SOUP_API_TOKEN" "http://127.0.0.1:4084/api/v1/soup/ready?consumer=reporter&projection=news-morning"'
```

## Rollback

If the watcher restart causes unexpected behavior, stop at runtime proof and
restore the previous authorization context. Do not edit either live DB. Do not
reset the Prime Radiant cursor unless Flynn explicitly approves a separate
cursor repair.

