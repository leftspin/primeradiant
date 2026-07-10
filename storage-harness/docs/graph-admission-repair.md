# Graph admission repair runner

This document is the operator contract for T1649. The runner is finite and
snapshot-first. It does not authorize a live repair by itself.

## Stable-ID requirement and seam matrix

| Requirement | Stable IDs/material preserved | Read seam | Mutation seam | Refusal / proof |
| --- | --- | --- | --- | --- |
| R1 snapshot and deterministic plan | tenant, source DB path, snapshot path/hash, source commit; original story, event, edge, proposal, op, commit, agent-run, evidence, input and source-ref IDs | read-only snapshot queries | none during `--dry-run` | repeated planning of the same snapshot and arguments has the same hash |
| R2 exact approval and durable run | repair run ID, plan hash, snapshot hash, approval evidence, source commit, actor and timestamps; every new mutation ID | approved plan artifact plus current snapshot | repair-run changeset through the durable soup membrane | absent/mismatched plan hash or snapshot hash refuses before mutation |
| R3 normal writes and immutable history | all historical event/edge/proposal/op/commit/evidence IDs remain unchanged | durable tenant loader | `ChangesetStore` plus `DurableSoupDb.persist_delta!` only | before/after history-ID sets are reported and must match |
| R4 quarantine, replay and rollback | quarantine ID per original story; replay input ID/content hash/source refs/evidence refs; replacement mutation IDs | plan replay groups by source identity/content hash | quarantine changeset, then `LiveStoryAgentLoop.run/4`, then durable delta persistence | agent refusals are recorded; rollback restores the recorded snapshot or marks the run failed without deleting provenance |
| R5 T1325-shaped validation | repair run/plan IDs and post-repair story/edge/mutation IDs | read-only post-repair queries | none | placeholder-key count, active quarantined exposure count, replacement placeholder/metadata failures, history preservation and replay refusals |

Prime Radiant owns this neutral soup repair truth. `consumer` and `projection`
names are not accepted by the runner, and the quarantine record expresses only
that a story is excluded from active soup material. Downstream products remain
responsible for artifact readiness and presentation policy.

## Operator sequence

1. Run `--dry-run` against a snapshot/copy, never the live soup path. Supply the
   source DB path as provenance, the snapshot path, tenant, source commit, and an
   output plan path.
2. Review the machine-readable plan. Approval must name the exact `plan_hash`.
3. Run `--apply` only with the reviewed plan, matching approved hash, approval
   evidence, actor, and the database whose bytes match the plan snapshot hash.
4. Preserve the snapshot until post-validation is accepted. On failure, restore
   it as a whole database copy, or retain the failed repair run and its mutation
   IDs for investigation. Never delete or rewrite historical graph rows.

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
  --soup-db /path/to/approved-snapshot-or-byte-identical-db.sqlite3 \
  --plan /path/to/repair-plan.json \
  --approved-plan-hash SHA256_FROM_PLAN \
  --approval-evidence OPERATOR_APPROVAL_REFERENCE \
  --actor OPERATOR_ID
```

`--apply` checks both the canonical plan hash and the database byte hash before
opening the durable write transaction. Its T1325-shaped `validation` object
reports placeholder replacement failures, quarantine exposure, edge metadata
failures, historical-ID preservation, and replay refusals.
