# Graph admission repair runner

This document is the operator contract for T1649. The runner is finite and
snapshot-first. It does not authorize a live repair by itself.

## Stable-ID requirement and seam matrix

| Requirement | Stable IDs/material preserved | Read seam | Mutation seam | Refusal / proof |
| --- | --- | --- | --- | --- |
| R1 snapshot and deterministic plan | tenant, source DB path, snapshot path/hash, tenant revision, source commit; original story, event, edge, proposal, op, commit, agent-run, evidence, input and source-ref IDs | read-only transactionally complete snapshot queries | none during `--dry-run` | repeated planning of the same snapshot and arguments has the same hash; WAL-bearing copies are refused |
| R2 exact approval and durable run | repair run ID, plan hash, snapshot hash, approval evidence, source commit, actor and timestamps; every new mutation ID | approved plan artifact plus current snapshot | repair-run changeset through the durable soup membrane | absent/mismatched plan hash or snapshot hash refuses before mutation |
| R3 normal writes and immutable history | all historical event/edge/proposal/op/commit/evidence IDs remain unchanged | durable tenant loader | `ChangesetStore` plus `DurableSoupDb.persist_delta!` only | before/after history-ID sets are reported and must match |
| R4 quarantine, replay and rollback | quarantine ID per original story; replay input ID/content hash/source refs/evidence refs; replacement mutation IDs | plan replay groups by source identity/content hash | quarantine changeset, then `LiveStoryAgentLoop.run/4`, then durable delta persistence; rollback restores original visibility, restores prior state for pre-existing stories, and quarantines newly created replacements | agent refusals are recorded; rollback verifies the preserved snapshot and retains every graph/provenance row while rolled-back mutation rows are excluded from feed material |
| R5 T1325-shaped validation | repair run/plan IDs and post-repair story/edge/mutation IDs | read-only post-repair queries | none | placeholder-key count, active quarantined exposure count, replacement placeholder/metadata failures, history preservation and replay refusals |

Prime Radiant owns this neutral soup repair truth. The runner uses the existing
feed contract's `consumer=reporter` and `projection=story_cards` values only as dumb
query inputs to prove quarantine exclusion; it does not accept them as operator
policy or decide artifact readiness. The quarantine record expresses only that a
story is excluded from active soup material. Downstream products remain responsible
for artifact readiness and presentation policy.

## Operator sequence

1. Run `--dry-run` against a snapshot/copy, never the live soup path. Supply the
   source DB path as provenance, the snapshot path, tenant, source commit, and an
   output plan path. Dry-run invokes the current story agents in memory and records
   their exact outputs, proposed replacement memberships/edges, and refusals; it
   performs no database write.
2. Review the machine-readable old-versus-proposed membership comparison and
   refusals. Approval must name the exact `plan_hash`, which also binds the recorded
   replay outputs used by apply.
3. Run `--apply` only with the reviewed plan, matching approved hash, approval
   evidence, actor, and a separate database copy whose bytes and tenant revision
   match the plan snapshot. The preserved snapshot path itself is refused.
4. Preserve the snapshot until post-validation is accepted. A failed apply retains
   a failed repair run without graph mutation. `--rollback` verifies the preserved
   snapshot, restores original story visibility, quarantines replacement stories,
   and marks the run rolled back without deleting or rewriting graph history.

The task prints one JSON object on success. Live model replay requires the normal
Prime Radiant agent configuration. Tests inject bounded fixture adapters and must
not be treated as production approval or trusted repair state.

Dry-run example against a copy:

```sh
mix primeradiant.graph_admission_repair --dry-run \
  --source-db /path/recorded/live-soup.sqlite3 \
  --snapshot /path/to/soup-snapshot.sqlite3 \
  --tenant TENANT_UUID \
  --source-commit COMMIT_SHA \
  --plan /path/to/repair-plan.json
```

Protected apply example after approval of that exact plan hash:

```sh
mix primeradiant.graph_admission_repair --apply \
  --soup-db /path/to/byte-identical-repair-target.sqlite3 \
  --plan /path/to/repair-plan.json \
  --approved-plan-hash SHA256_FROM_PLAN \
  --approval-evidence OPERATOR_APPROVAL_REFERENCE \
  --actor OPERATOR_ID
```

Rollback example for a succeeded repair run:

```sh
mix primeradiant.graph_admission_repair --rollback \
  --soup-db /path/to/repaired-target.sqlite3 \
  --plan /path/to/repair-plan.json \
  --approved-plan-hash SHA256_FROM_PLAN \
  --repair-run-id REPAIR_RUN_UUID \
  --actor OPERATOR_ID
```

`--apply` checks both the canonical plan hash and the database byte hash before
opening the durable write transaction, then uses the tenant revision guard in each
write transaction. Its T1325-shaped `validation` object
reports placeholder replacement failures, quarantine exposure, edge metadata
failures, historical-ID preservation, approved-versus-applied membership equality,
typed replacement story/edge IDs, feed membership, and replay refusals. Durable
mutation IDs include the write-membrane replay audit rows as well as graph and repair
rows.
