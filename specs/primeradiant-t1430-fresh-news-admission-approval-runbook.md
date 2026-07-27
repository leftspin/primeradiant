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
git cat-file -e "$T1430_REVIEWED_SHA:storage-harness/scripts/r1/live_subspace_daemon_watcher_once.sh"
git cat-file -e "$T1430_REVIEWED_SHA:storage-harness/deploy/tars/t328-live-watcher-node-launcher.mjs"
git cat-file -e "$T1430_REVIEWED_SHA:storage-harness/deploy/tars/t328-live-watcher-loop.sh"
git cat-file -e "$T1430_REVIEWED_SHA:storage-harness/deploy/tars/ai.primeradiant.t328-daemon-db-watcher.plist"
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
all_consumed="$(jq -sc "." "$root"/*-consumed.jsonl)"
chain_state="$(jq -sc \
  --arg start "$start" \
  --arg target "$target" \
  --argjson consumed "$all_consumed" '
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
consumed="$(jq -cn --argjson all "$all_consumed" --argjson event_ids "$event_ids" \
  "[
    \$all[]
    | select(.event_id as \$id | \$event_ids | index(\$id))
  ]")"
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

ack_json="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  "ack=\\$(find /home/clu/primeradiant-runs -type f -path '*/$event_id/consume-ack.json' -print -quit)
   test -n \"\\$ack\"
   cat \"\\$ack\"")"
printf '%s' "$ack_json" | jq -e --arg event_id "$event_id" '
  .schema == "primeradiant.consume_ack.v1" and
  .status == "consumed" and
  .event_id == $event_id'

ack_report_path="$(printf '%s' "$ack_json" | jq -er '
  .report_path |
  select(startswith("/home/clu/primeradiant-runs/") and endswith("/changed-stories-report.json"))')"
ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  "test -f '$ack_report_path'"

# The strict ack above may be from an idempotent retry after the originating
# attempt durably wrote soup/story but lost ack delivery. Resolve story meaning
# independently by the exact source item, then join it to durable soup below.
chain_candidates="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu \
  clu@eurisko /bin/bash -s <<'REMOTE_STORY_CANDIDATES'
find /home/clu/primeradiant-runs -type f -name changed-stories-report.json \
  -exec jq -c \
    --arg message_id "f2665df3-c004-4881-9b82-7ff159be065d" '
      select(
        .source.source_item_id == $message_id and
        .story_meaning_proof == true and
        .live_story_agent_loop.story_meaning_proof == true)
      | .live_story_agent_loop.correlation_chains[]
      | select(.source_ref == ("news_article:" + $message_id))
    ' {} + |
  jq -sc 'unique_by(.correlation_id)'
REMOTE_STORY_CANDIDATES
)"

story_proof="$(ssh -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519_clu clu@eurisko \
  "set -eu
   set -a
   . /home/clu/.local/state/primeradiant/soup-api/env
   set +a
   {
     sqlite3 -readonly -json \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" \
       'select id,status,scope from agent_runs;'
     sqlite3 -readonly -json \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" \
       'select id,story_id,story_version,producing_agent_run_id,packet_hash,output_hash,status from story_card_versions;'
     sqlite3 -readonly -json \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" \
       'select id,story_id,input_id,classification,story_version,proposal_id,graph_commit_id from story_events;'
     sqlite3 -readonly -json \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" \
       'select id,subject_type,subject_id,input_id,evidence_label,evidence_hash from evidence_refs;'
     sqlite3 -readonly -json \"\$PRIMERADIANT_SOUP_API_SOUP_DB\" \
       'select id,external_id from inputs;'
   } | jq -cs \
     '{agent_runs: .[0], card_versions: .[1], story_events: .[2], evidence_refs: .[3], inputs: .[4]}'")"
chain_json="$(jq -cn \
  --arg message_id "f2665df3-c004-4881-9b82-7ff159be065d" \
  --argjson candidates "$chain_candidates" \
  --argjson proof "$story_proof" '
  [
    $candidates[] as $chain
    | select(
        ($chain.classification |
          IN("split", "attach", "conflict", "duplicate", "no_op", "color", "stale")) and
        ($chain.agent_run_ids | length > 0) and
        ($chain.packet_ids | length > 0) and
        ($chain.evidence_refs | length > 0) and
        ($chain.story_event_id | type == "string") and
        ($chain.story_card_version_id | type == "string"))
    | select(any($proof.story_events[];
        . as $event |
        $event.id == $chain.story_event_id and
        any($proof.inputs[];
          .id == $event.input_id and .external_id == $message_id)))
    | select(all($chain.agent_run_ids[];
        . as $run_id | any($proof.agent_runs[]; .id == $run_id)))
    | select(any($proof.card_versions[];
        .id == $chain.story_card_version_id))
    | $chain
  ]
  | unique_by(.correlation_id)
  | select(length == 1)
  | .[0]')"
printf '%s' "$chain_json" | jq -e \
  --argjson proof "$story_proof" '
  . as $chain |
  ($proof.story_events | map(select(.id == $chain.story_event_id))) as $events |
  ($proof.card_versions | map(select(.id == $chain.story_card_version_id))) as $cards |
  ($events | length) == 1 and
  ($cards | length) == 1 and
  ($events[0] as $event |
    any($proof.inputs[];
      .id == $event.input_id and
      .external_id == "f2665df3-c004-4881-9b82-7ff159be065d") and
    $event.story_id == $chain.story_id and
    $event.proposal_id == $chain.proposal_id and
    $event.graph_commit_id == $chain.graph_commit_id and
    $event.classification == $chain.classification and
    ($event.story_version | type == "number") and
    all($chain.evidence_refs[];
      . as $evidence_label |
      any($proof.evidence_refs[];
        .subject_type == "story_event" and
        .subject_id == $chain.story_event_id and
        .input_id == $event.input_id and
        .evidence_label == $evidence_label and
        (.evidence_hash | test("^[0-9a-f]{64}$"))))) and
  ($chain.agent_run_ids | unique | length) == ($chain.agent_run_ids | length) and
  all($chain.agent_run_ids[];
    . as $run_id |
    any($proof.agent_runs[];
      .id == $run_id and
      .status == "succeeded" and
      ((.scope | fromjson) as $scope |
        ($chain.packet_ids | index($scope.packet_id)) != null and
        ($scope.packet_hash | test("^[0-9a-f]{64}$")) and
        ($scope.output_hash | test("^[0-9a-f]{64}$")) and
        ($scope.evidence_refs | length > 0) and
        all($scope.evidence_refs[]; . as $ref | ($chain.evidence_refs | index($ref)) != null)))) and
  ($cards[0] as $card |
    ($proof.agent_runs |
      map(select(.id == $card.producing_agent_run_id))) as $producers |
    ($producers | length) == 1 and
    ($producers[0].scope | fromjson) as $producer_scope |
    ($card.producing_agent_run_id as $id | $chain.agent_run_ids | index($id)) != null and
    ($producer_scope.packet_id as $id | $chain.packet_ids | index($id)) != null and
    $card.story_id == $chain.story_id and
    ($card.story_version | type == "number") and
    $card.packet_hash == $producer_scope.packet_hash and
    $card.output_hash == $producer_scope.output_hash and
    ($card.packet_hash | test("^[0-9a-f]{64}$")) and
    ($card.output_hash | test("^[0-9a-f]{64}$")) and
    ($card.status | IN("complete", "incomplete", "refused", "unavailable"))) and
  (
    (($chain.refusal_reason | type) == "string" and ($chain.refusal_reason | length) > 0) or
    ($chain.classification | IN("split", "attach", "conflict")) or
    ($chain.classification | IN("duplicate", "no_op", "color", "stale"))
  )'

disposition="$(printf '%s' "$chain_json" | jq -r '
  if ((.refusal_reason | type) == "string" and (.refusal_reason | length) > 0)
  then "refusal:" + .refusal_reason
  elif (.classification | IN("split", "attach", "conflict"))
  then "material_story:" + .classification
  else "packet_grounded_agent_story_unchanged:" + .classification
  end')"
printf 'row 20391 disposition: %s\n' "$disposition"

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
  jq -e '\''length >= 1 and .[0].external_id == "f2665df3-c004-4881-9b82-7ff159be065d"'\''
curl -fsS -H "Authorization: Bearer $PRIMERADIANT_SOUP_API_TOKEN" \
  "http://127.0.0.1:4084/api/v1/soup/ready?consumer=reporter&projection=news-morning" |
  jq -e '\''
    (.freshness.latest_source_at | type == "string") and
    ((.freshness.latest_story_event_at == null) or (.freshness.latest_story_event_at | type == "string")) and
    (.freshness.max_age_seconds | type == "number") and
    (.freshness.is_stale | type == "boolean") and
    (.synthesis_health.status | type == "string")'\'''
```

Finally run the supported real-tenant R1744-04 ready/feed/delta/gap/ack proof
without altering its Reporter-owned 86,400-second configuration. Record that
configuration evidence and Reporter's evaluation separately from the neutral
Prime Radiant fields above.

If row 20391 cannot be processed, stop with the exact package/ack failure and
the cursor at the last durably acknowledged predecessor. Do not skip row 20391
or prove a later row first.

## Rollback

For any unexpected restored-watcher behavior, including after partial
acknowledgement, unload only the restored TARS watcher and preserve its current
cursor, state root, and evidence exactly. Do not transfer ownership back,
reactivate the divergent EURISKO local watcher, reset a cursor, or alter either
database.
