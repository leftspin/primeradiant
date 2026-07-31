# Prime Radiant T1430 Fresh News Transport Recovery Runbook

Status: reviewed-path source and runtime recovery plan.

Scope: restore the selected production transport from the canonical TARS
Subspace daemon DB to the served EURISKO soup DB. This runbook owns only the
Prime Radiant watcher and its neutral source/story freshness and health surface.
Authenticated Reporter readiness is downstream integration evidence; Reporter
retains ownership of projection and artifact-readiness policy. T1399/T1765 own
the producer and receiver that already committed qualifying row 20391.

## Selected Product Boundary

T1430's accepted ingest path is:

1. The Node-backed TARS watcher reads the canonical Microverse daemon DB with
   `sqlite3 -readonly`.
2. Prime Radiant emits bounded committed-source-item event packages after the
   durable cursor.
3. TARS pushes each package to the EURISKO consumer over the authenticated SSH
   path.
4. EURISKO consumes the package into the served soup DB and returns the strict
   `primeradiant.consume_ack.v1` acknowledgement.
5. TARS advances its ordered cursor atomically only after that acknowledgement.
6. Story agents run from admitted soup pressure under Prime Radiant semantics.

The production source DB is neither copied to EURISKO nor read from a local
EURISKO receiver replica. The July EURISKO `--local-consume true` systemd unit
is a deployment-topology regression and must not remain installed.

## Invariants

- Preserve cursor bytes `2026-07-22 01:21:48|20350\n` during the ownership move.
- Do not edit either SQLite DB, synthesize or rewrite a cursor, replay/backfill,
  skip a row, or acknowledge an unprocessed row.
- Do not restart or modify the TARS receiver or the RACTER producer.
- Do not weaken the 86,400-second readiness rule.
- A failed package leaves the cursor at the last durably acknowledged package
  and remains eligible for the next normal bounded watcher pass. That cursor is
  20350 only until the first post-recovery package is acknowledged.

## Source-Controlled Runtime

- TARS plist:
  `storage-harness/deploy/tars/ai.primeradiant.t328-daemon-db-watcher.plist`
- TARS Node launcher:
  `storage-harness/deploy/tars/t328-live-watcher-node-launcher.mjs`
- TARS loop:
  `storage-harness/deploy/tars/t328-live-watcher-loop.sh`
- Shared bounded watcher:
  `storage-harness/scripts/r1/live_subspace_daemon_watcher_once.sh`

The Node parent is required because the known-good TARS launchd execution
context can read Microverse and spawn the read-only SQLite child.

## Pre-Deploy Proof

Record, without mutation:

```sh
ssh tars 'set -eu
DB=/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3
/opt/homebrew/bin/node /Users/mike/.local/libexec/primeradiant/t328-live-watcher-node-launcher.mjs --probe
launchctl print gui/$(id -u)/ai.primeradiant.t328-daemon-db-watcher 2>&1 || true
test ! -e /Users/mike/.local/state/primeradiant/t328-live-watcher'

ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko 'set -eu
systemctl --user show primeradiant-subspace-watcher.service \
  -p LoadState -p ActiveState -p SubState -p FragmentPath
test "$(cat ~/.local/state/primeradiant/t328-live-watcher/cursor.txt)" = "2026-07-22 01:21:48|20350"
test "$(wc -c < ~/.local/state/primeradiant/t328-live-watcher/cursor.txt)" -eq 26
sha256sum ~/.local/state/primeradiant/t328-live-watcher/cursor.txt'
```

The EURISKO watcher must be quiesced before copying its cursor-bearing state so
it cannot race the handoff. Its user manager must report
`ActiveState=inactive`, `SubState=dead`, and the source state root must contain
no `live-watcher.lock`. Copy the complete state root to a sibling temporary
directory under `/Users/mike/.local/state/primeradiant` on TARS, compare the
cursor SHA-256 and exact 26 bytes before rename, assert the canonical destination
is absent, and rename the sibling into place. This is a byte-preserving state
ownership transfer, not a cursor edit.

## Deploy

From the reviewed source SHA on eezo:

1. Stop and disable only
   `primeradiant-subspace-watcher.service` on EURISKO. Remove its exact installed
   unit file, reload the user manager, and require `LoadState=not-found`.
2. Install the reviewed watcher script on TARS at
   `~/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh`.
3. Install the reviewed Node launcher and loop at their source-controlled
   absolute paths.
4. Transfer the quiesced EURISKO state root byte-for-byte to the absent TARS
   canonical state-root path and verify the cursor SHA-256 is unchanged.
5. Install the reviewed plist at
   `~/Library/LaunchAgents/ai.primeradiant.t328-daemon-db-watcher.plist`.
6. Bootstrap and kickstart only that existing documented watcher label.

Do not hand-patch either target. All installed executable and plist hashes must
match the reviewed source files.

Before any runtime mutation, bind the operator checkout to the exact accepted
review identity and canonical remote main:

```sh
: "${T1430_REVIEWED_SHA:?set T1430_REVIEWED_SHA to the accepted Janus review SHA}"
git fetch origin main
test "$T1430_REVIEWED_SHA" = "$(git rev-parse HEAD)"
test "$T1430_REVIEWED_SHA" = "$(git rev-parse origin/main)"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
git cat-file -e "${T1430_REVIEWED_SHA}:storage-harness/scripts/r1/live_subspace_daemon_watcher_once.sh"
git cat-file -e "${T1430_REVIEWED_SHA}:storage-harness/deploy/tars/t328-live-watcher-node-launcher.mjs"
git cat-file -e "${T1430_REVIEWED_SHA}:storage-harness/deploy/tars/t328-live-watcher-loop.sh"
git cat-file -e "${T1430_REVIEWED_SHA}:storage-harness/deploy/tars/ai.primeradiant.t328-daemon-db-watcher.plist"
```

Required transfer gates:

```sh
ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko 'set -eu
systemctl --user disable --now primeradiant-subspace-watcher.service
test "$(systemctl --user show primeradiant-subspace-watcher.service -p ActiveState --value)" = inactive
test "$(systemctl --user show primeradiant-subspace-watcher.service -p SubState --value)" = dead
test ! -e ~/.local/state/primeradiant/t328-live-watcher/live-watcher.lock
test "$(cat ~/.local/state/primeradiant/t328-live-watcher/cursor.txt)" = "2026-07-22 01:21:48|20350"
test "$(wc -c < ~/.local/state/primeradiant/t328-live-watcher/cursor.txt)" -eq 26
test "$(sha256sum ~/.local/state/primeradiant/t328-live-watcher/cursor.txt | awk "{print \$1}")" = bc09e90edeaa44eba7b1e1785bab1e9b90d693531e5105ce1469ff90e50dde16
rm ~/.config/systemd/user/primeradiant-subspace-watcher.service
systemctl --user daemon-reload
test "$(systemctl --user show primeradiant-subspace-watcher.service -p LoadState --value)" = not-found'
```

Transfer the quiesced root through the operator host into a uniquely named
TARS sibling on the same filesystem:

```sh
ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  'set -eu
   cursor=/home/clu/.local/state/primeradiant/t328-live-watcher/cursor.txt
   test "$(cat "$cursor")" = "2026-07-22 01:21:48|20350"
   test "$(wc -c < "$cursor")" -eq 26
   test "$(sha256sum "$cursor" | awk "{print \$1}")" = bc09e90edeaa44eba7b1e1785bab1e9b90d693531e5105ce1469ff90e50dde16
   tar -C /home/clu/.local/state/primeradiant -cf - t328-live-watcher' |
ssh tars 'set -eu
parent=/Users/mike/.local/state/primeradiant
sibling=$parent/.t328-live-watcher.transfer-T1430
target=$parent/t328-live-watcher
test ! -e "$sibling"
test ! -e "$target"
mkdir -p "$sibling"
tar -C "$sibling" --strip-components=1 -xf -
test ! -e "$sibling/live-watcher.lock"
test "$(cat "$sibling/cursor.txt")" = "2026-07-22 01:21:48|20350"
test "$(wc -c < "$sibling/cursor.txt")" -eq 26
test "$(shasum -a 256 "$sibling/cursor.txt" | awk "{print \$1}")" = bc09e90edeaa44eba7b1e1785bab1e9b90d693531e5105ce1469ff90e50dde16
mv "$sibling" "$target"'
```

Before the final rename this requires:

- source and sibling cursor SHA-256 equal
  `bc09e90edeaa44eba7b1e1785bab1e9b90d693531e5105ce1469ff90e50dde16`;
- sibling cursor byte count is 26 and command-substituted content equals
  `2026-07-22 01:21:48|20350`;
- `/Users/mike/.local/state/primeradiant/t328-live-watcher` is absent;
- the sibling has no `live-watcher.lock`.

After install, require SHA-256 equality between each reviewed source artifact
and its installed TARS counterpart, and validate the plist with `plutil -lint`,
the launcher with `node --check`, and the shell files with `bash -n` before
bootstrap.

Install only from the reviewed checkout:

```sh
ssh tars 'mkdir -p /Users/mike/.local/libexec/primeradiant/r1'
scp storage-harness/scripts/r1/live_subspace_daemon_watcher_once.sh \
  tars:/Users/mike/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh
scp storage-harness/deploy/tars/t328-live-watcher-node-launcher.mjs \
  tars:/Users/mike/.local/libexec/primeradiant/t328-live-watcher-node-launcher.mjs
scp storage-harness/deploy/tars/t328-live-watcher-loop.sh \
  tars:/Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh
scp storage-harness/deploy/tars/ai.primeradiant.t328-daemon-db-watcher.plist \
  tars:/Users/mike/Library/LaunchAgents/ai.primeradiant.t328-daemon-db-watcher.plist
ssh tars 'set -eu
chmod 755 /Users/mike/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh
chmod 755 /Users/mike/.local/libexec/primeradiant/t328-live-watcher-node-launcher.mjs
chmod 755 /Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh
plutil -lint /Users/mike/Library/LaunchAgents/ai.primeradiant.t328-daemon-db-watcher.plist
/opt/homebrew/bin/node --check /Users/mike/.local/libexec/primeradiant/t328-live-watcher-node-launcher.mjs
/bin/bash -n /Users/mike/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh
/bin/bash -n /Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh'

test "$(shasum -a 256 storage-harness/scripts/r1/live_subspace_daemon_watcher_once.sh | awk '{print $1}')" = \
  "$(ssh tars 'shasum -a 256 /Users/mike/.local/libexec/primeradiant/r1/live_subspace_daemon_watcher_once.sh | awk "{print \$1}"')"
test "$(shasum -a 256 storage-harness/deploy/tars/t328-live-watcher-node-launcher.mjs | awk '{print $1}')" = \
  "$(ssh tars 'shasum -a 256 /Users/mike/.local/libexec/primeradiant/t328-live-watcher-node-launcher.mjs | awk "{print \$1}"')"
test "$(shasum -a 256 storage-harness/deploy/tars/t328-live-watcher-loop.sh | awk '{print $1}')" = \
  "$(ssh tars 'shasum -a 256 /Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh | awk "{print \$1}"')"
test "$(shasum -a 256 storage-harness/deploy/tars/ai.primeradiant.t328-daemon-db-watcher.plist | awk '{print $1}')" = \
  "$(ssh tars 'shasum -a 256 /Users/mike/Library/LaunchAgents/ai.primeradiant.t328-daemon-db-watcher.plist | awk "{print \$1}"')"

ssh tars 'set -eu
launchctl bootstrap gui/$(id -u) /Users/mike/Library/LaunchAgents/ai.primeradiant.t328-daemon-db-watcher.plist
launchctl kickstart -k gui/$(id -u)/ai.primeradiant.t328-daemon-db-watcher'
```

## Ordered Product Proof

Normal bounded passes must begin after cursor 20350 and advance in order through
canonical TARS row 20391. Row 20391 is not required to fit in the first
20-package pass:

- source event: row 20391, `inbound_event=new_message`,
  `accepted_at=2026-07-25 21:00:50 UTC`,
  author `argus-racter-publisher`,
  message `f2665df3-c004-4881-9b82-7ff159be065d`;
- the emitted manifest records the committed source item, an `after_cursor`
  below its package cursor, and ordered package cursors;
- the durable package record maps row 20391 and its exact message ID to one
  generated event ID; EURISKO's acknowledgement returns that event ID with
  schema `primeradiant.consume_ack.v1` and `status=consumed`;
- only then does the TARS cursor advance to or beyond row 20391;
- the served soup DB records the admitted source material;
- story evidence records either a fresh story event or an actual packet-grounded
  agent no-op, refusal, or non-material disposition with agent-run, packet,
  output, and evidence identifiers;
- Prime Radiant's authenticated output exposes the neutral latest source/story
  timestamps, age, and synthesis health separately;
- Reporter-owned R1744-04 integration proof applies Reporter's unchanged
  86,400-second configuration to those fields and records its readiness result.
  Prime Radiant's projection-specific status is downstream compatibility
  evidence, not the authority for Reporter policy.

Pass/fail evidence must include:

1. A read-only TARS query returning row 20391 with the exact accepted time,
   inbound event, author, and message ID above.
2. The manifest path and package object for row 20391, plus proof all preceding
   package cursors were acknowledged in order.
3. The strict acknowledgement object and its durable evidence path.
4. The post-ack TARS cursor and proof its accepted-at/id tuple is at or beyond
   row 20391.
5. A read-only served-soup `inputs` query whose `external_id` is the exact
   row-20391 message ID.
6. The story event, or the packet-grounded agent disposition evidence described
   above.
7. Authenticated Prime Radiant neutral freshness/health fields, followed by the
   separate Reporter-owned R1744-04 configuration and readiness evaluation.

Run these fail-closed assertions from eezo after row 20391 is acknowledged:

```sh
set -euo pipefail

row_json="$(ssh tars 'DB=/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3
/usr/bin/sqlite3 -readonly -json "$DB" "select id,accepted_at,inbound_event,author_name,message_id from daemon_event where id=20391;"')"
printf '%s' "$row_json" | jq -e '
  length == 1 and
  .[0] == {
    id: 20391,
    accepted_at: "2026-07-25 21:00:50",
    inbound_event: "new_message",
    author_name: "argus-racter-publisher",
    message_id: "f2665df3-c004-4881-9b82-7ff159be065d"
  }'

order_json="$(ssh tars /bin/bash -s <<'REMOTE_ORDER_PROOF'
set -eu
root=/Users/mike/.local/state/primeradiant/t328-live-watcher
start="2026-07-22 01:21:48|20350"
target="2026-07-25 21:00:50|20391"
chain_state="$(jq -sc \
  --arg start "$start" \
  --arg target "$target" \
  --slurpfile consumed <(jq -sc "." "$root"/*-consumed.jsonl) '
  ($consumed[0]) as $consumed |
  def strictly_acked($package):
    any($consumed[];
      .event_id == $package.event_id and
      .cursor == $package.cursor and
      .ack.schema == "primeradiant.consume_ack.v1" and
      .ack.status == "consumed" and
      .ack.event_id == $package.event_id);
  def acknowledged_prefix($manifest):
    reduce ($manifest.packages | to_entries[]) as $entry (
      {open: true, packages: []};
      if .open and strictly_acked($entry.value) then
        .packages += [{
          manifest_run_id: $manifest.run_id,
          package_index: ($entry.key + 1),
          event_id: $entry.value.event_id,
          daemon_event_id: $entry.value.daemon_event_id,
          message_id: $entry.value.message_id,
          cursor: $entry.value.cursor,
          after_cursor: $manifest.after_cursor,
          manifest_next_cursor: $manifest.next_cursor
        }] |
        if $entry.value.cursor == $target then .open = false else . end
      else .open = false
      end
    ) | .packages;
  reduce .[] as $manifest (
    {
      complete: false,
      invalid: false,
      expected_after_cursor: $start,
      chain: [],
      nonadvancing_attempts: []
    };
    if .complete then .
    elif ($manifest.after_cursor == .expected_after_cursor) then
      acknowledged_prefix($manifest) as $prefix |
      ([
        $manifest.packages[]?
        | select(.cursor <= $target)
        | select(strictly_acked(.))
      ] | length) as $acked_count |
      if $acked_count != ($prefix | length) then
        .invalid = true
      elif ($prefix | length) == 0 then
        .nonadvancing_attempts += [{
          run_id: $manifest.run_id,
          after_cursor: $manifest.after_cursor,
          manifest_kind: $manifest.manifest_kind
        }]
      else
        .chain += [{manifest: $manifest, acknowledged_packages: $prefix}] |
        .expected_after_cursor = $prefix[-1].cursor |
        .complete = ($prefix[-1].cursor == $target)
      end
    elif any($manifest.packages[]?; strictly_acked(.)) then
      .invalid = true
    else
      .nonadvancing_attempts += [{
        run_id: $manifest.run_id,
        after_cursor: $manifest.after_cursor,
        manifest_kind: $manifest.manifest_kind
      }]
    end
  )
  | select(.complete and (.invalid | not))
' "$root"/*-manifest-records.jsonl)"
expected="$(jq -c "[.chain[].acknowledged_packages[]]" <<<"$chain_state")"
event_ids="$(jq -c "[.[].event_id]" <<<"$expected")"
packages="$(jq -sc --argjson event_ids "$event_ids" '
  [
    .[]
    | select(.event_id as $id | $event_ids | index($id))
    | {
        manifest_run_id,
        package_index,
        event_id,
        daemon_event_id,
        message_id,
        cursor,
        after_cursor,
        manifest_next_cursor
      }
  ]
' "$root"/*-packages.jsonl)"
consumed="$(jq -sc --argjson event_ids "$event_ids" '
  [
    .[]
    | select(.event_id as $id | $event_ids | index($id))
  ]
' "$root"/*-consumed.jsonl)"
jq -cn \
  --arg start "$start" \
  --arg target "$target" \
  --argjson chain_state "$chain_state" \
  --argjson expected "$expected" \
  --argjson packages "$packages" \
  --argjson consumed "$consumed" \
  "{start: \$start, target: \$target, chain_state: \$chain_state,
    expected: \$expected, packages: \$packages, consumed: \$consumed}"
REMOTE_ORDER_PROOF
)"
printf '%s' "$order_json" | jq -e '
  . as $proof |
  ($proof.chain_state.chain | length) > 0 and
  ($proof.chain_state.invalid | not) and
  $proof.chain_state.complete and
  $proof.chain_state.chain[0].manifest.after_cursor == $proof.start and
  $proof.expected[-1].cursor == $proof.target and
  all(range(1; $proof.chain_state.chain | length);
    $proof.chain_state.chain[.].manifest.after_cursor ==
      $proof.chain_state.chain[. - 1].acknowledged_packages[-1].cursor) and
  all($proof.chain_state.chain[];
    . as $segment |
    ($segment.manifest as $manifest |
      $manifest.packages == ($manifest.packages | sort_by(.accepted_at, .daemon_event_id)) and
      (($manifest.packages | length) == 0 and
         $manifest.next_cursor == $manifest.after_cursor or
       ($manifest.packages | length) > 0 and
         $manifest.after_cursor < $manifest.packages[0].cursor and
         $manifest.packages[-1].cursor == $manifest.next_cursor)) and
    all(range(0; $segment.acknowledged_packages | length);
      $segment.acknowledged_packages[.].package_index == (. + 1))) and
  $proof.packages == $proof.expected and
  ($proof.consumed | length) == ($proof.expected | length) and
  all(range(0; $proof.expected | length);
    ($proof.expected[.] as $package |
     $proof.consumed[.] |
       .event_id == $package.event_id and
       .cursor == $package.cursor and
       .ack.schema == "primeradiant.consume_ack.v1" and
       .ack.status == "consumed" and
       .ack.event_id == $package.event_id))'

package_json="$(printf '%s' "$order_json" | jq -ec '.expected[-1]')"
target_segment_json="$(printf '%s' "$order_json" | jq -ec '
  [
    .chain_state.chain[]
    | select(any(.acknowledged_packages[];
        .cursor == "2026-07-25 21:00:50|20391"))
  ]
  | select(length == 1)
  | .[0]')"
manifest_json="$(printf '%s' "$target_segment_json" | jq -c '.manifest')"
printf '%s' "$manifest_json" | jq -e '
  .source_mode == "subspace_daemon_read_only_db_cursor" and
  (.after_cursor | type == "string") and
  .after_cursor < "2026-07-25 21:00:50|20391" and
  (.packages == (.packages | sort_by(.accepted_at, .daemon_event_id))) and
  any(.packages[];
    .daemon_event_id == 20391 and
    .message_id == "f2665df3-c004-4881-9b82-7ff159be065d" and
    .cursor == "2026-07-25 21:00:50|20391")'
printf '%s' "$package_json" | jq -e '
  .daemon_event_id == 20391 and
  .message_id == "f2665df3-c004-4881-9b82-7ff159be065d" and
  (.event_id | test("^[A-Za-z0-9._-]+$")) and
  .cursor == "2026-07-25 21:00:50|20391"'
event_id="$(printf '%s' "$package_json" | jq -r .event_id)"

ack_json="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu \
  clu@eurisko /bin/bash -s -- "$event_id" <<'REMOTE_ACK'
set -euo pipefail
event_id="$1"
ack="$(find /home/clu/primeradiant-runs -type f \
  -path "*/$event_id/consume-ack.json" -print -quit)"
test -n "$ack"
cat "$ack"
REMOTE_ACK
)"
printf '%s' "$ack_json" | jq -e --arg event_id "$event_id" '
  .schema == "primeradiant.consume_ack.v1" and
  .status == "consumed" and
  .event_id == $event_id'

ack_report_path="$(printf '%s' "$ack_json" | jq -er '
  .report_path |
  select(startswith("/home/clu/primeradiant-runs/") and endswith("/changed-stories-report.json"))')"
ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  "test -f '$ack_report_path'"

ssh tars 'cursor="$(cat /Users/mike/.local/state/primeradiant/t328-live-watcher/cursor.txt)"
jq -en --arg cursor "$cursor" '\''
  ($cursor | capture("^(?<accepted>.+)\\|(?<id>[0-9]+)$")) as $c |
  ($c.accepted > "2026-07-25 21:00:50") or
  ($c.accepted == "2026-07-25 21:00:50" and ($c.id | tonumber) >= 20391)'\'''

ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko 'set -eu
set -a
. /home/clu/.local/state/primeradiant/soup-api/env
set +a
sqlite3 -readonly -json "$PRIMERADIANT_SOUP_API_SOUP_DB" \
  "select external_id,observed_at,inserted_at from inputs where external_id='\''f2665df3-c004-4881-9b82-7ff159be065d'\'';" |
  jq -e '\''length == 1 and .[0].external_id == "f2665df3-c004-4881-9b82-7ff159be065d"'\'''

# The strict ack above may be from an idempotent retry after the originating
# attempt durably wrote source material but lost ack delivery. Read only that
# ack's bounded report and the exact durable source identity. Row 20391 did not
# produce a story decision: the packet-grounded agent was unavailable, which is
# distinct from a legitimate unchanged-story classification.
ack_report_json="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu \
  clu@eurisko "cat '$ack_report_path'")"
printf '%s' "$ack_report_json" | jq -e '
  .source.source_item_id == "f2665df3-c004-4881-9b82-7ff159be065d" and
  .story_meaning_proof == false and
  .substrate_proof_only == true and
  .live_story_agent_loop.status == "unavailable" and
  .live_story_agent_loop.reason == "live_gibson_response_shape" and
  .ingestion.inputs == 1 and
  .ingestion.graph_commits == 0 and
  .ingestion.proposals == 0 and
  .ingestion.stories == 0'

story_proof="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  "set -eu
   set -a
   . /home/clu/.local/state/primeradiant/soup-api/env
   set +a
   sqlite3 -readonly \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" '
     select json_object(
       \"input_count\", (
         select count(*) from inputs
          where external_id=\"f2665df3-c004-4881-9b82-7ff159be065d\"
       ),
       \"story_event_count\", (
         select count(*) from story_events se join inputs i on i.id=se.input_id
          where i.external_id=\"f2665df3-c004-4881-9b82-7ff159be065d\"
       ),
       \"agent_run_count\", (
         select count(*) from agent_runs
          where json_extract(scope,\"$.correlation_id\") glob
                \"correlation:news_article:f2665df3-c004-4881-9b82-7ff159be065d:*\"
       ),
       \"card_count\", (
         select count(*) from story_card_versions scv
           join agent_runs ar on ar.id=scv.producing_agent_run_id
          where json_extract(ar.scope,\"$.correlation_id\") glob
                \"correlation:news_article:f2665df3-c004-4881-9b82-7ff159be065d:*\"
       ),
       \"proposal_count\", (
         select count(*) from proposals p
           join story_events se on se.proposal_id=p.id
           join inputs i on i.id=se.input_id
          where i.external_id=\"f2665df3-c004-4881-9b82-7ff159be065d\"
       )
     );'")"
printf '%s' "$story_proof" | jq -e '
  .input_count == 1 and
  .story_event_count == 0 and
  .agent_run_count == 0 and
  .card_count == 0 and
  .proposal_count == 0'

printf '%s\n' \
  'BLOCKER: row 20391 source admission and ordered acknowledgement succeeded, but story synthesis stopped at live_gibson_response_shape.' >&2
exit 1
```

Only after row 20391 has an actual packet-grounded story outcome, run the
supported real-tenant R1744-04 ready/feed/delta/gap/ack proof without altering
its Reporter-owned 86,400-second configuration. Record that configuration
evidence and Reporter's evaluation separately from the neutral Prime Radiant
fields above.

If row 20391 cannot be processed, stop with the exact package/ack failure and
the cursor at the last durably acknowledged predecessor. Do not skip row 20391
or prove a later row first.

## Rollback

For any unexpected restored-watcher behavior, including after partial
acknowledgement, unload only the restored TARS watcher and preserve its current
cursor, state root, and evidence exactly. Do not transfer ownership back,
reactivate the divergent EURISKO local watcher, reset a cursor, or alter either
database.
