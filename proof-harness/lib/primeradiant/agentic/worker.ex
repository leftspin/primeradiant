defmodule Primeradiant.Agentic.Worker do
  @moduledoc false

  alias Primeradiant.Agentic.Packet
  alias Primeradiant.Agentic.Transcript
  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Proposals.Builder

  @role_configs %{
    source_admission: %{
      role: :source_admission,
      config_version: "source-admission.v1.t1137",
      prompt_version_hash: "prompt:source-admission:2026-05-24"
    },
    story_identity: %{
      role: :story_identity,
      config_version: "story-identity.v1.t1137",
      prompt_version_hash: "prompt:story-identity:2026-05-24"
    },
    advancement_contradiction: %{
      role: :advancement_contradiction,
      config_version: "advancement-contradiction.v1.t1137",
      prompt_version_hash: "prompt:advancement-contradiction:2026-05-24"
    },
    flynn_relative_authoring: %{
      role: :flynn_relative_authoring,
      config_version: "flynn-relative-authoring.v1.t1137",
      prompt_version_hash: "prompt:flynn-relative-authoring:2026-05-24"
    }
  }

  def configs, do: @role_configs

  def source_admission(raw) do
    config = @role_configs.source_admission
    packet = Packet.for_source(raw, config)
    normalized = Normalizer.normalize(raw)

    %{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      outcome: :write,
      operation_family: :source_admission,
      normalized: normalized,
      evidence_refs: [normalized.fixture_id],
      confidence: 0.9,
      uncertainty_class: uncertainty_for(normalized),
      decision_source: :deterministic_substrate,
      deterministic_product_logic: true,
      agent_output_hash: Packet.hash(%{source: normalized.fixture_id, role: config.role}),
      reason: "Admitted bounded source packet into soup-visible normalized evidence."
    }
  end

  def story_identity(store, normalized, user_id) do
    config = @role_configs.story_identity
    packet = Packet.for_story(store, normalized, config, user_id)
    decision = Transcript.story_decision(normalized.fixture_id, config.role)

    %{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      outcome: :write,
      operation_family: :story_identity,
      decision: decision,
      evidence_refs: [normalized.fixture_id],
      confidence: decision.confidence,
      uncertainty_class: uncertainty_for(normalized),
      reason: decision.rationale,
      decision_source: :recorded_agent_transcript,
      deterministic_product_logic: false,
      agent_output_hash: decision.agent_output_hash
    }
    |> Map.merge(Transcript.producer())
    |> Map.put(:role_provenance, role_provenance(decision))
  end

  def advancement_contradiction(store, normalized, decision, user_id) do
    config = @role_configs.advancement_contradiction
    packet = Packet.for_story(store, normalized, config, user_id)
    advancement_decision = Transcript.story_decision(normalized.fixture_id, config.role)

    cond do
      low_authority_private_public?(store, normalized, decision) ->
        refusal(
          config,
          packet,
          normalized,
          :role_authority,
          "Private Flynn evidence cannot be used for a public story mutation."
        )

      true ->
        proposal =
          normalized
          |> Builder.build(decision)
          |> annotate_proposal(config, packet, :advancement_or_story_transformation, normalized)

        %{
          role: config.role,
          config: config,
          packet: packet,
          packet_hash: Packet.hash(packet),
          outcome: :write,
          operation_family: :advancement_or_story_transformation,
          proposal: proposal,
          evidence_refs: proposal.evidence_refs,
          confidence: proposal.confidence,
          uncertainty_class: proposal.uncertainty_class,
          reason: proposal.rationale,
          decision_source: :recorded_agent_transcript,
          deterministic_product_logic: false,
          agent_output_hash: advancement_decision.agent_output_hash
        }
        |> Map.merge(Transcript.producer())
        |> Map.put(:role_provenance, role_provenance(advancement_decision))
    end
  end

  def flynn_relative_authoring(store, seen_state, user_id, stage) do
    config = @role_configs.flynn_relative_authoring
    packet = Packet.for_authoring(store, seen_state, config, user_id)
    output = Transcript.authoring_output(stage, store, user_id)

    %{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      outcome: :write,
      operation_family: :flynn_relative_delta,
      output: output,
      evidence_refs: output.evidence_refs,
      confidence: if(output.verified, do: 0.91, else: 0.0),
      uncertainty_class: if(output.verified, do: :bounded_evidence, else: :ungrounded_output),
      reason:
        "Authored a Flynn-relative delta from committed public/private soup and seen-state.",
      decision_source: :recorded_agent_transcript,
      deterministic_product_logic: false,
      agent_output_hash: Packet.hash(%{stage: stage, output: output})
    }
    |> Map.merge(Transcript.producer())
  end

  def follow_on_review(store, activation, user_id) do
    config = @role_configs.advancement_contradiction
    packet = Packet.for_follow_on(store, activation, config, user_id)

    if review = Transcript.follow_on_review(activation.caused_by_fixture_id) do
      %{
        role: config.role,
        config: config,
        packet: packet,
        packet_hash: Packet.hash(packet),
        outcome: review.outcome,
        operation_family: :follow_on_review,
        caused_by_fixture_id: activation.caused_by_fixture_id,
        evidence_refs: activation.evidence_refs,
        confidence: review.confidence,
        uncertainty_class: review.uncertainty_class,
        decision_source: :recorded_agent_transcript,
        deterministic_product_logic: false,
        agent_output_hash: review.agent_output_hash,
        reason: review.rationale
      }
      |> Map.merge(Transcript.producer())
    end
  end

  def annotate_proposal(proposal, config, packet, operation_family, normalized) do
    %{
      proposal
      | agent_run_id: "agent-run:#{config.config_version}:#{normalized.fixture_id}",
        agent_role: config.role,
        agent_config_version: config.config_version,
        prompt_version_hash: config.prompt_version_hash,
        input_packet_hash: Packet.hash(packet),
        visibility: packet.output_visibility,
        uncertainty_class: uncertainty_for(normalized),
        ops:
          Enum.map(proposal.ops, fn op ->
            op
            |> Map.put(:agent_role, config.role)
            |> Map.put(:agent_config_version, config.config_version)
            |> Map.put(:prompt_version_hash, config.prompt_version_hash)
            |> Map.put(:input_packet_hash, Packet.hash(packet))
            |> Map.put(:operation_family, operation_family)
            |> Map.put(:visibility, packet.output_visibility)
            |> Map.put(:uncertainty_class, uncertainty_for(normalized))
          end)
    }
  end

  defp role_provenance(decision) do
    %{
      role: decision.role,
      config_version: decision.config_version,
      prompt_version_hash: decision.prompt_version_hash,
      recorded_choice: decision.recorded_choice
    }
  end

  defp refusal(config, packet, normalized, uncertainty_class, reason) do
    %{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      outcome: :refused,
      operation_family: :advancement_or_story_transformation,
      fixture_id: normalized.fixture_id,
      evidence_refs: [normalized.fixture_id],
      confidence: 0.0,
      uncertainty_class: uncertainty_class,
      decision_source: :recorded_agent_transcript,
      deterministic_product_logic: false,
      agent_output_hash: Packet.hash(%{fixture_id: normalized.fixture_id, reason: reason}),
      reason: reason
    }
    |> Map.merge(Transcript.producer())
  end

  defp low_authority_private_public?(store, normalized, decision) do
    normalized.acl["privacy"] == "private" and public_story_target?(store, decision.story_key)
  end

  defp public_story_target?(store, story_key) do
    case store.stories[story_key] do
      nil ->
        false

      story ->
        Enum.all?(story.inputs, fn input_id ->
          get_in(store.nodes, [input_id, :attrs, :acl, "privacy"]) == "public"
        end)
    end
  end

  defp uncertainty_for(normalized) do
    cond do
      normalized.questions != %{} -> :open_question
      normalized.facts["correction_scope"] != nil -> :contradictory_source
      normalized.colors != [] and normalized.facts == %{} -> :framing_only
      true -> :bounded_evidence
    end
  end
end
