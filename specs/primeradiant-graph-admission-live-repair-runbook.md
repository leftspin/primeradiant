# Prime Radiant Graph Admission Live Repair Runbook

Status: source-ready repair plan, not authorized live mutation.

Scope: polluted durable story nodes created before the graph admission authority repair, especially durable placeholder story keys such as `new-story`.

## Product Boundary

- The DB/schema/write path validates form, identity, provenance, atomicity, permissions, and referential integrity.
- Ecology/story agents own semantic article-to-story judgment.
- The repair path may quarantine, replay, and re-evaluate agent-authored story membership, but it must not add a second DB-level semantic judge.
- No production DB row may be manually edited, deleted, or rewritten outside an approved repair runner/migration with dry-run output, provenance capture, and replayable evidence.

## Required Source Preconditions

1. The repaired source must be deployed:
   - placeholder story keys are rejected by write path and schema constraints;
   - declared article-story contribution edges require structural audit metadata;
   - `soup_candidate_hint` is packet context only and cannot override durable story identity or event classification.
2. The repair runner must run against a copy or snapshot first.
3. The repair runner must emit a machine-readable plan before any write.
4. A human/operator must approve the exact plan before live mutation.

## Safe Repair Sequence

1. Snapshot the live soup DB and record:
   - source DB path;
   - tenant id;
   - schema version/source commit;
   - snapshot hash and storage path.
2. Dry-run discovery:
   - find stories where `lower(story_key)` is in `new-story`, `new_story`, `newstory`, `story`, or `news-story`;
   - enumerate story events, edges, proposal ops, graph commits, agent runs, evidence refs, and input ids attached to those stories;
   - verify no write is attempted during discovery.
3. Quarantine plan generation:
   - create a proposed quarantine record for each polluted story id;
   - preserve original story id, story key, title history, event ids, edge ids, proposal/op/commit ids, evidence refs, input ids, and source refs;
   - mark the story unavailable for public/magazine projection only through a supported quarantine field/table introduced by reviewed source, not by deleting rows.
4. Replay plan generation:
   - group affected inputs by source identity/content hash;
   - feed each input back through the repaired ecology/story loop with original evidence refs and bounded packets;
   - require current agents/prompts/eval gates to produce replacement story decisions;
   - write only new graph mutations through the normal write membrane.
5. Approval gate:
   - compare old polluted membership with proposed replacement story membership;
   - include rejected/refused inputs and their agent refusal reasons;
   - require operator approval before live writes.
6. Live execution:
   - run the same reviewed repair runner against the live DB;
   - record one repair run id;
   - store dry-run plan hash, approval evidence, snapshot hash, source commit, actor, started/finished timestamps, and every mutation id;
   - do not mutate raw source inputs or rewrite historical proposal/op/commit/evidence rows.
7. Post-repair proof:
   - `new-story` and equivalent placeholder keys cannot be inserted;
   - no active public/magazine projection exposes the quarantined polluted story;
   - replayed replacement stories have non-placeholder keys and required edge metadata;
   - the same feed no longer groups unrelated article sets under one durable story;
   - all new writes pass schema/write membrane tests.

## What Must Not Be Done

- Do not `UPDATE stories SET story_key = ...` on the live DB.
- Do not delete polluted `story_events`, `edges`, `proposals`, `proposal_ops`, `graph_commits`, or `evidence_refs`.
- Do not manually split one story by hand-authored SQL.
- Do not manufacture summaries, source URLs, source domains, or article-to-story explanations.
- Do not use `soup_candidate_hint` as a repair authority.
- Do not deploy a DB semantic validator that decides whether an article really belongs to a story.

## Minimum Repair Runner Acceptance

- `--dry-run` produces the same plan from the same snapshot.
- `--apply` refuses to run without a matching approved dry-run plan hash.
- Every write goes through the same changeset/write membrane as normal story-loop writes.
- The runner records repair provenance in durable soup state.
- A rollback strategy exists before live execution: restore snapshot or mark repair run failed without removing original provenance.

## Current Blocker

This runbook is a plan only. Live EURISKO repair remains blocked until a reviewed repair runner/migration and operator approval exist.
