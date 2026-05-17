**Blocking Findings**

1. Demo-code cheating / spec embellishment: near-collision identity is pre-labeled in fixture body and trusted by the classifier. `story_identity` drives `identity_compatible?/2`, so `public_story_006` attaches and `public_story_007` splits largely because the fixture tells the harness the answer. That undercuts the proof gate requiring decisions “without demo shortcuts or preseeded trusted state.”
Refs: [story_classifier.ex:96](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/projections/story_classifier.ex:96), [public_story_006_near_duplicate_overlap.json:8](/Users/mike/shared-workspace/primeradiant/proof-harness/priv/fixtures/primeradiant_golden/inputs/public_story_006_near_duplicate_overlap.json:8), [public_story_007_adjacent_distinct_terminal_walkout.json:8](/Users/mike/shared-workspace/primeradiant/proof-harness/priv/fixtures/primeradiant_golden/inputs/public_story_007_adjacent_distinct_terminal_walkout.json:8), [proof_harness_test.exs:68](/Users/mike/shared-workspace/primeradiant/proof-harness/test/primeradiant/proof_harness_test.exs:68), source: [primeradiant-mvp-slice.html:128](/Users/mike/shared-workspace/primeradiant/product/primeradiant-mvp-slice.html:128).

2. Named edge contracts are only asserted on proposal ops, not committed graph state. `Builder` emits `edge_type`, but `Engine.apply_op/4` ignores it for `:attach_input` and `:attach_watch`; commits retain only `%{proposal_id, op}`. The test then checks `result.store.proposals |> flat_map(& &1.ops)` instead of committed edges, so it cannot prove “no committed edge is merely related,” `watch_applies_to`, `duplicates`, or `contradicts`.
Refs: [builder.ex:34](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/proposals/builder.ex:34), [builder.ex:59](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/proposals/builder.ex:59), [engine.ex:37](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/arbitration/engine.ex:37), [engine.ex:121](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/arbitration/engine.ex:121), [proof_harness_test.exs:47](/Users/mike/shared-workspace/primeradiant/proof-harness/test/primeradiant/proof_harness_test.exs:47), source: [primeradiant-elixir-contracts.html:60](/Users/mike/shared-workspace/primeradiant/architecture/primeradiant-elixir-contracts.html:60).

3. Proposal-backed writes are not actually arbitration-backed or evidence-validated. `Engine.commit/3` appends every proposal and applies every op unconditionally; `Proposal` has no `status`, `agent_run_id`, typed `proposed_ops`, or validation path, and ops carry no per-op `evidence_refs`. That misses the contract that graph changes pass through typed operations, evidence refs, validation gates, and arbitration before commit.
Refs: [proposal.ex:4](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/proposals/proposal.ex:4), [builder.ex:14](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/proposals/builder.ex:14), [engine.ex:6](/Users/mike/shared-workspace/primeradiant/proof-harness/lib/primeradiant/arbitration/engine.ex:6), source: [primeradiant-elixir-contracts.html:40](/Users/mike/shared-workspace/primeradiant/architecture/primeradiant-elixir-contracts.html:40), [primeradiant-elixir-contracts.html:141](/Users/mike/shared-workspace/primeradiant/architecture/primeradiant-elixir-contracts.html:141).

**Checks**

No demo-code cheating: failed, due `story_identity` shortcut.

Proposal-backed graph writes: partially present as structs, but failed as arbitration/validation-backed writes.

Evidence/provenance preservation: failed; evidence is proposal-level fixture IDs only, not op/edge/sentence provenance.

Named edge contracts: failed at committed graph layer.

Spec embellishment: failed; `story_identity` is an unspecced semantic gate.

Verification: `elixir -pa _build/test/lib/jason/ebin -pa _build/test/lib/primeradiant_proof_harness/ebin -r test/test_helper.exs -r test/primeradiant/proof_harness_test.exs` passed 7 tests. `mix test` is blocked by sandbox TCP denial in Mix PubSub. Independent Codex review could not complete because network access is restricted. Engram queries returned no provenance records.
