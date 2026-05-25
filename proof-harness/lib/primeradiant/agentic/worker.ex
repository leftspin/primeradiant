defmodule Primeradiant.Agentic.Worker do
  @moduledoc false

  alias Primeradiant.Agentic.Packet
  alias Primeradiant.Authoring.Briefing
  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Projections.StoryClassifier
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
      reason: "Admitted bounded source packet into soup-visible normalized evidence."
    }
  end

  def story_identity(store, normalized, user_id) do
    config = @role_configs.story_identity
    packet = Packet.for_story(store, normalized, config, user_id)
    decision = StoryClassifier.decide(store, normalized)

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
      reason: decision.rationale
    }
  end

  def advancement_contradiction(store, normalized, decision, user_id) do
    config = @role_configs.advancement_contradiction
    packet = Packet.for_story(store, normalized, config, user_id)

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
          reason: proposal.rationale
        }
    end
  end

  def flynn_relative_authoring(store, seen_state, user_id) do
    config = @role_configs.flynn_relative_authoring
    packet = Packet.for_authoring(store, seen_state, config, user_id)
    output = Briefing.render(store, seen_state, user_id)

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
      reason: "Authored a Flynn-relative delta from committed public/private soup and seen-state."
    }
  end

  def follow_on_review(store, activation, user_id) do
    config = @role_configs.advancement_contradiction
    packet = Packet.for_follow_on(store, activation, config, user_id)

    %{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      outcome: :abstained,
      operation_family: :follow_on_review,
      caused_by_fixture_id: activation.caused_by_fixture_id,
      evidence_refs: activation.evidence_refs,
      confidence: 0.76,
      uncertainty_class: :bounded_evidence,
      reason:
        "Read the mutation-triggered story packet and left no additional mutation because the triggering write already represented the bounded delta."
    }
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
      reason: reason
    }
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
