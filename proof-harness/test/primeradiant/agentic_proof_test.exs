defmodule Primeradiant.AgenticProofTest do
  use ExUnit.Case, async: true

  alias Primeradiant.Agentic.Proof

  test "T1137 proof runs configured agent-style workers over ACL-scoped packets" do
    result = Proof.run()
    audit = result.proof_audit

    assert audit.corpus.real_message_style_evidence
    assert audit.corpus.no_trusted_story_ids
    assert audit.corpus.repeated_pressure
    assert audit.corpus.public_evidence
    assert audit.corpus.private_evidence

    assert audit.agents.actual_invocations >= 3
    assert audit.agents.explicit_configs
    assert audit.agents.bounded_packets
    assert audit.agents.overlapping_regions
    assert audit.agents.abstention_or_refusal
    assert audit.agents.mutation_triggered_follow_on
    assert audit.agents.required_roles_present

    assert Enum.any?(result.invocations, &(&1.role == :story_identity and &1.outcome == :write))

    assert Enum.any?(
             result.invocations,
             &(&1.role == :advancement_contradiction and &1.outcome == :write)
           )

    assert Enum.any?(
             result.invocations,
             &(&1.role == :advancement_contradiction and &1.outcome == :refused)
           )

    assert Enum.any?(
             result.invocations,
             &(&1.operation_family == :follow_on_review and &1.outcome == :abstained)
           )
  end

  test "T1137 proof writes evidence-backed mutations with agent packet provenance" do
    result = Proof.run()
    audit = result.proof_audit

    assert audit.writes.atomic_mutations
    assert audit.writes.append_only_logs
    assert audit.writes.actor_attribution
    assert audit.writes.evidence_refs
    assert audit.writes.confidence_and_uncertainty
    assert audit.writes.typed_edges
    assert audit.writes.no_silent_replacement

    assert Enum.all?(result.store.proposals, &is_atom(&1.agent_role))
    assert Enum.all?(result.store.proposals, &is_binary(&1.agent_config_version))
    assert Enum.all?(result.store.proposals, &is_binary(&1.prompt_version_hash))
    assert Enum.all?(result.store.proposals, &is_binary(&1.input_packet_hash))
    assert Enum.all?(result.store.proposals, &(&1.uncertainty_class != nil))
    refute Enum.any?(result.store.proposals, &(&1.agent_role == :fixture_story_seeker))

    refute Enum.any?(
             result.store.proposals,
             &String.starts_with?(&1.input_packet_hash, "legacy-packet:")
           )

    assert Enum.all?(result.store.commits, &is_binary(&1.input_packet_hash))
    assert Enum.all?(result.store.proposal_decisions, &is_binary(&1.prompt_version_hash))

    decisions = Map.new(result.decisions, &{&1.fixture_id, &1})
    stale = decisions["stale_case_001_related_no_material_change"]
    assert stale.classification == :stale_background_state
    assert stale.outcome == :committed
    assert result.store.stories[stale.story_key].state == :background

    assert Enum.any?(
             result.store.edges,
             &(&1.from == "stale_case_001_related_no_material_change" and
                 &1.to == stale.story_key and &1.edge_type == :adds_color)
           )
  end

  test "T1137 proof preserves public/private membrane and Flynn-relative seen state" do
    result = Proof.run()
    audit = result.proof_audit

    assert audit.membrane.source_corpus_read_only
    assert audit.membrane.public_writes_public_evidence_only
    assert audit.membrane.private_writes_remain_private
    assert audit.membrane.no_private_to_public_leak

    assert audit.output.flynn_relative_delta
    assert audit.output.cites_allowed_evidence
    assert audit.output.distinguishes_new_to_flynn
    assert audit.output.records_uncertainty
    assert audit.output.mutates_seen_state_or_private_brain
    assert map_size(result.seen_state.stories) > 0

    assert result.second_briefing.text =~ "what changed for Flynn since last seen"
    assert result.first_briefing.output_id != result.second_briefing.output_id
    assert result.seed_seen_state.stories != %{}
  end

  test "T1137 proof maps architecture correction items and excludes harness-only claims" do
    result = Proof.run()
    audit = result.proof_audit

    assert audit.anti_cheat.no_fixture_oracle_output
    assert audit.anti_cheat.no_demo_product_claim
    assert audit.anti_cheat.harness_labels_excluded_from_product_claims

    assert Enum.all?(Map.values(audit.correction_matrix))
  end
end
