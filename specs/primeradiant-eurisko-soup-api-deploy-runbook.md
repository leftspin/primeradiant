# Prime Radiant EURISKO Soup API Deploy Runbook

Status: supported operator runbook for staged-copy deploys. Do not execute this
runbook during T1329.

Scope: deploy reviewed Prime Radiant source into the real EURISKO Reporter-facing
soup API runtime on port 4084. This runbook covers source copy, build, user
systemd restart, deployed marker update, rollback, health checks, and graph
admission soak boundaries. It does not repair polluted live story data.

## RB1 Source Of Truth And Runtime Paths

Source of truth:

- Repository: `git@github.com:leftspin/primeradiant.git`
- Deploy source commit: an explicit reviewed commit SHA. For the graph admission
  repair gate, this is `06b4dbbe87d2c42662d64e1068acb8267bbf06b7`.
- EURISKO SSH target: `clu@eurisko` using `/Users/mike/.ssh/id_ed25519_clu`.

Active staged runtime on EURISKO:

- Staged source copy: `/home/clu/src/primeradiant`
- Active service working directory:
  `/home/clu/src/primeradiant/storage-harness`
- Deployed marker: `/home/clu/src/primeradiant/DEPLOYED_COMMIT`
- Current pre-runbook observed marker:
  `08b47d9941a472f7adb7cddeb573cebf603bf355`
- Soup DB: `/home/clu/.local/state/primeradiant/soup-api/soup.sqlite3`
- Service environment file:
  `/home/clu/.local/state/primeradiant/soup-api/env`
- Ack log: value from `PRIMERADIANT_SOUP_API_ACK_LOG` in the service
  environment file.
- Tenant: value from `PRIMERADIANT_SOUP_API_TENANT` in the service environment
  file.
- Public internal API base: `http://eurisko:4084/api/v1/soup`

Active service identity:

- User systemd unit: `primeradiant-soup-api.service`
- Unit path:
  `/home/clu/.config/systemd/user/primeradiant-soup-api.service`
- `WorkingDirectory=%h/src/primeradiant/storage-harness`
- `EnvironmentFile=%h/.local/state/primeradiant/soup-api/env`
- `ExecStart=/bin/bash -lc 'export PATH=/home/clu/.local/share/mise/installs/elixir/1.19.5-otp-28/bin:/home/clu/.local/share/mise/installs/erlang/28.5/bin:/home/clu/.local/share/mise/installs/sqlite/3.45.1/bin:$PATH; exec mix primeradiant.soup_api --soup-db "$PRIMERADIANT_SOUP_API_SOUP_DB" --tenant "$PRIMERADIANT_SOUP_API_TENANT" --token "$PRIMERADIANT_SOUP_API_TOKEN" --ack-log "$PRIMERADIANT_SOUP_API_ACK_LOG" --ip 0.0.0.0 --port 4084'`

Before deployment, record:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'cat /home/clu/src/primeradiant/DEPLOYED_COMMIT;
   systemctl --user show primeradiant-soup-api.service \
     -p Id -p FragmentPath -p ExecStart -p WorkingDirectory \
     -p EnvironmentFiles -p ActiveState -p SubState -p MainPID --no-pager'
```

This RB1 inventory is part of the T1275 artifact parity boundary: product proof
must name the active runner, invoked source tree, marker SHA, service identity,
and runtime DB/API target. A source checkout SHA alone is not deploy proof.

## RB1A Recurring Soup Cadence One-Shot

The recurring soup cadence runner is a one-shot command over the already-admitted
Prime Radiant soup DB. It is not source ingest, source fetch, cursor replay, or a
service installer. Do not add or modify systemd timers, cron, launchd, unit
files, or environment files from this section unless a separate reviewed deploy
ticket explicitly authorizes that exact scheduler change.

After a reviewed cadence-capable source deploy, an operator may run one bounded
pass from the active staged source tree:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'set -a;
   . /home/clu/.local/state/primeradiant/soup-api/env;
   set +a;
   cd /home/clu/src/primeradiant/storage-harness &&
   export PATH=/home/clu/.local/share/mise/installs/elixir/1.19.5-otp-28/bin:/home/clu/.local/share/mise/installs/erlang/28.5/bin:/home/clu/.local/share/mise/installs/sqlite/3.45.1/bin:$PATH &&
   mix primeradiant.soup_cadence_once \
     --soup-db "$PRIMERADIANT_SOUP_API_SOUP_DB" \
     --tenant "$PRIMERADIANT_SOUP_API_TENANT" \
     --actor flynn \
     --cadence hourly_story_card_synthesis \
     --limit 8'
```

Expected proof from a conforming pass:

- `source_admission_performed` is `false`.
- `source_behavior` is `recurring_cadence_over_admitted_soup`.
- New story-card rows, if any, carry refresh reason
  `story_card_hourly_synthesis`, agent run provenance, packet hash,
  prompt/config hash, output hash, model, model route, producer kind, and
  decision source.
- Runtime model route remains Gibson/Qwen unless a source-of-truth spec
  explicitly authorizes another product soup-agent route.

## RB2 Copy, Build, And Release Steps

Do not edit live DB rows. Do not modify the user-systemd unit or environment
file as part of a source deploy unless a separate reviewed ticket explicitly
requires it.

1. Confirm the target commit is reachable from `origin/main` on the operator
   machine:

   ```sh
   git ls-remote origin refs/heads/main
   git cat-file -t 06b4dbbe87d2c42662d64e1068acb8267bbf06b7
   ```

2. Create a timestamped backup of the current staged source copy on EURISKO:

   ```sh
   TS="$(date -u +%Y%m%dT%H%M%SZ)"
   ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
     "cp -a /home/clu/src/primeradiant /home/clu/src/primeradiant.backup-t1329-${TS}"
   ```

3. Create a release artifact from the reviewed commit on the operator machine.
   The artifact must contain tracked source from the reviewed commit, not the
   dirty local worktree:

   ```sh
   git archive --format=tar \
     --prefix=primeradiant/ \
     06b4dbbe87d2c42662d64e1068acb8267bbf06b7 \
     > /tmp/primeradiant-06b4dbb.tar
   ```

4. Copy the artifact to EURISKO and expand it into a new staging directory:

   ```sh
   scp -i /Users/mike/.ssh/id_ed25519_clu \
     /tmp/primeradiant-06b4dbb.tar \
     clu@eurisko:/home/clu/primeradiant-06b4dbb.tar

   ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
     'rm -rf /home/clu/src/primeradiant.next &&
      mkdir -p /home/clu/src/primeradiant.next &&
      tar -xf /home/clu/primeradiant-06b4dbb.tar \
        -C /home/clu/src/primeradiant.next --strip-components=1'
   ```

5. Build/check the new staged copy without touching the running service:

   ```sh
   ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
     'cd /home/clu/src/primeradiant.next/storage-harness &&
      export PATH=/home/clu/.local/share/mise/installs/elixir/1.19.5-otp-28/bin:/home/clu/.local/share/mise/installs/erlang/28.5/bin:/home/clu/.local/share/mise/installs/sqlite/3.45.1/bin:$PATH &&
      mix deps.get &&
      mix compile &&
      mix test'
   ```

6. Promote the checked staged copy atomically enough for this single-host source
   layout:

   ```sh
   ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
     'rm -rf /home/clu/src/primeradiant.previous &&
      mv /home/clu/src/primeradiant /home/clu/src/primeradiant.previous &&
      mv /home/clu/src/primeradiant.next /home/clu/src/primeradiant'
   ```

If any copy/build step fails, stop before restart. The runtime remains on the
pre-existing service process and source copy.

## RB3 User-Systemd Restart/Reload And Service Identity

The only service covered by this runbook is the user unit:

```text
primeradiant-soup-api.service
```

Restart is a runtime mutation. It is allowed only when the operator is executing
this runbook for an approved deploy ticket.

Reload the user manager and restart the existing unit:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'systemctl --user daemon-reload &&
   systemctl --user restart primeradiant-soup-api.service &&
   systemctl --user status primeradiant-soup-api.service --no-pager'
```

After restart, confirm the live process still comes from the intended staged
copy and service:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'systemctl --user show primeradiant-soup-api.service \
     -p ActiveState -p SubState -p MainPID -p WorkingDirectory --no-pager;
   p="$(systemctl --user show primeradiant-soup-api.service -p MainPID --value)";
   test -n "$p" && readlink "/proc/$p/cwd"'
```

Do not create, enable, disable, edit, or replace systemd, launchd, cron, or
other persistence in this runbook. This runbook only restarts the already
installed `primeradiant-soup-api.service`.

## RB4 DEPLOYED_COMMIT Update

Update the deployed marker only after:

- the staged copy has been promoted;
- the service restart has completed;
- RB6 health checks pass.

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'printf "%s\n" 06b4dbbe87d2c42662d64e1068acb8267bbf06b7 \
     > /home/clu/src/primeradiant/DEPLOYED_COMMIT &&
   cat /home/clu/src/primeradiant/DEPLOYED_COMMIT'
```

The deploy proof must record both the commit from this file and the running
service `MainPID` after restart. If the marker is updated without a matching
healthy runtime, rollback and do not record deploy proof.

## RB5 Rollback

Rollback target:

- `/home/clu/src/primeradiant.previous` from RB2 step 6, or the timestamped
  `/home/clu/src/primeradiant.backup-t1329-*` directory if promotion failed
  after backup but before `previous` existed.

Rollback command:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'test -d /home/clu/src/primeradiant.previous &&
   rm -rf /home/clu/src/primeradiant.failed &&
   mv /home/clu/src/primeradiant /home/clu/src/primeradiant.failed &&
   mv /home/clu/src/primeradiant.previous /home/clu/src/primeradiant &&
   systemctl --user restart primeradiant-soup-api.service &&
   cat /home/clu/src/primeradiant/DEPLOYED_COMMIT &&
   systemctl --user status primeradiant-soup-api.service --no-pager'
```

Rollback health checks are the same as RB6. Record rollback evidence with the
restored `DEPLOYED_COMMIT`, service status, and `/api/v1/soup/ready` result.

Rollback must not delete or rewrite soup DB rows, story events, graph edges,
acks, or source input records.

## RB6 Health Checks For `/api/v1/soup`

Run health checks from EURISKO localhost and from the operator machine against
the product URL.

Local EURISKO checks:

```sh
ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  '. /home/clu/.local/state/primeradiant/soup-api/env &&
   curl -fsS -m 5 http://127.0.0.1:4084/api/v1/soup/ready &&
   curl -fsS -m 10 \
     -H "Authorization: Bearer $PRIMERADIANT_SOUP_API_TOKEN" \
     "http://127.0.0.1:4084/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=3"'
```

Operator check:

```sh
TOKEN="$(ssh -i /Users/mike/.ssh/id_ed25519_clu clu@eurisko \
  'set -a; . /home/clu/.local/state/primeradiant/soup-api/env; printf "%s" "$PRIMERADIANT_SOUP_API_TOKEN"')"

curl -fsS -m 5 http://eurisko:4084/api/v1/soup/ready
curl -fsS -m 10 \
  -H "Authorization: Bearer ${TOKEN}" \
  "http://eurisko:4084/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=3"
```

Required pass criteria:

- user systemd reports `ActiveState=active` and `SubState=running`;
- `MainPID` is non-empty and its cwd is
  `/home/clu/src/primeradiant/storage-harness`;
- `/api/v1/soup/ready` returns HTTP 2xx;
- authenticated `/api/v1/soup/feed` returns HTTP 2xx JSON;
- `DEPLOYED_COMMIT` matches the reviewed deployed commit after RB4;
- no smoke check mutates the soup DB except normal read/ack behavior explicitly
  requested by the API endpoint being tested.

## RB7 Graph Admission Soak Proof Boundary For `06b4dbb`

This runbook does not itself deploy
`06b4dbbe87d2c42662d64e1068acb8267bbf06b7`. When a later deploy ticket executes
this runbook for that commit, the graph-admission soak must prove only runtime
and write-membrane boundaries, not semantic article truth.

Required source proof before deploy:

- reviewed commit:
  `06b4dbbe87d2c42662d64e1068acb8267bbf06b7`;
- `cd storage-harness && mix test` passed for T1324/T1325/T1326;
- review proof says the repair contains no second semantic judge.

Required post-deploy soak:

- Repeated `/api/v1/soup/ready` checks against the real port 4084 for the soak
  window selected by the deploy ticket.
- Repeated authenticated `/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=3`
  checks against the same service.
- A non-production or copied-DB write-membrane check for the deployed source
  proving:
  - placeholder durable story keys such as `new-story` are rejected;
  - article-story contribution edges missing required structural metadata are
    rejected;
  - `soup_candidate_hint` does not override ecology-agent story identity.

The post-deploy soak must not write malformed probes into the live soup DB.
Use a copied DB, temporary DB, or source test harness for negative write
probes. Live API health checks are read-only except for explicit API ack tests
authorized by the deploy ticket.

Record deploy proof only after source, runtime marker, user-systemd service
identity, health checks, and soak evidence all agree. For Tracker, T1324/T1325/
T1326 may move from `Deployable` to `Verifiable` only after deploy proof exists
for this intended real runtime. They must not move to `Verified` without Flynn
or human confirmation.

## RB8 Live Polluted `new-story` Data Repair Is Separate

The observed polluted live story data, including durable placeholder story keys
such as `new-story`, is not repaired by this deploy runbook.

Live data repair remains behind:

- `specs/primeradiant-graph-admission-live-repair-runbook.md`;
- a reviewed repair runner or migration;
- live soup DB snapshot;
- dry-run plan;
- operator approval for the exact plan;
- provenance capture and replay evidence.

Do not manually update, delete, split, or rewrite live stories, story events,
edges, proposals, graph commits, evidence refs, inputs, source rows, ack logs,
or source DB rows as part of deploying source. The deploy may prevent new bad
form from being admitted, but it does not clean existing polluted graph state.
