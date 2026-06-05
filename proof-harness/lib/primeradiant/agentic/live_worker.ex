defmodule Primeradiant.Agentic.LiveWorker do
  @moduledoc false

  alias Primeradiant.Agentic.LiveGibson
  alias Primeradiant.Agentic.Packet
  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Proposals.Builder

  @system_prompt """
  You are a Prime Radiant soup agent. You inspect only the bounded soup packet supplied in the user message.
  You may author candidate mutations, abstentions, refusals, questions, and explicitly permitted mediated writes.
  You are not a central arbiter and you do not judge final truth. Return only one JSON object matching the requested schema.
  """

  @story_identity_prompt """
  Decide whether the input belongs to an existing visible story or starts a new story.
  Use evidence from the packet snippets, input fields, and visible stories. Choose a stable story_key from the subject.
  For material new facts about an existing story, use classification substantive_update.
  For a new story, use classification new_story. Include changed_facts as a JSON object, conflicts as an array, and a short evidence-backed rationale.
  """

  @advancement_prompt """
  Author a situated candidate mutation for the story state if the packet supports one.
  Use one of these classifications: new_story, substantive_update, repeated_noop_input, conflict_correction, color_spin_without_structural_change, stale_background_state.
  Use the visible story when the source is about the same continuing situation; otherwise use a new stable story_key.
  Include changed_facts, conflicts, watch_ids, and rationale from the bounded packet only.
  """

  @authoring_prompt """
  Inspect accumulated soup and Flynn seen-state. Produce a private Flynn-relative delta only from visible committed soup.
  Return operation_family create_authored_delta with concise bullets and evidence_refs when there is a grounded delta.
  Return operation_family mark_no_op, verified false, and an empty bullets list when no grounded delta is supported.
  """

  @follow_on_prompt """
  Inspect the committed soup-change packet and prior story state. If the bounded packet adds no new grounded mutation, abstain with operation_family mark_no_op.
  If it exposes a specific open question, return abstained with operation_family create_question and evidence_refs.
  """

  def configs do
    %{
      source_admission:
        config(
          :source_admission,
          "source-admission.v1.t1137",
          "Admit bounded source packets.",
          %{}
        ),
      story_identity:
        config(:story_identity, "story-identity.v2.t1178.live-gibson", @story_identity_prompt, %{
          classification: "new_story | substantive_update",
          story_key: "stable story key",
          confidence: "0.0-1.0",
          changed_facts: "object",
          conflicts: "array",
          watch_ids: "array",
          parent_story_key: "string or null",
          rationale: "string"
        }),
      advancement_contradiction:
        config(
          :advancement_contradiction,
          "advancement-contradiction.v2.t1178.live-gibson",
          @advancement_prompt,
          %{
            classification:
              "new_story | substantive_update | repeated_noop_input | conflict_correction | color_spin_without_structural_change | stale_background_state",
            story_key: "stable story key",
            confidence: "0.0-1.0",
            changed_facts: "object",
            conflicts: "array",
            watch_ids: "array",
            parent_story_key: "string or null",
            rationale: "string"
          }
        ),
      flynn_relative_authoring:
        config(
          :flynn_relative_authoring,
          "flynn-relative-authoring.v2.t1178.live-gibson",
          @authoring_prompt,
          %{
            operation_family: "create_authored_delta | mark_no_op",
            verified: "boolean",
            bullets: "array of strings",
            evidence_refs: "array",
            touched_story_keys: "array",
            confidence: "0.0-1.0",
            rationale: "string"
          }
        ),
      follow_on_review:
        config(
          :advancement_contradiction,
          "advancement-contradiction.v2.t1178.live-gibson",
          @follow_on_prompt,
          %{
            outcome: "abstained | refused | write",
            operation_family: "mark_no_op | create_question",
            confidence: "0.0-1.0",
            uncertainty_class: "bounded_evidence | open_question | source_gap",
            rationale: "string"
          }
        )
    }
  end

  def source_admission(raw) do
    config = configs().source_admission
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
    config = configs().story_identity
    packet = Packet.for_story(store, normalized, config, user_id)
    live = LiveGibson.invoke(config.role, config, packet)
    decision = decision_from(live.output, normalized, config)

    live_invocation(live, config, packet, %{
      outcome: :write,
      operation_family: :story_identity,
      decision: decision,
      evidence_refs: [normalized.fixture_id],
      confidence: decision.confidence,
      uncertainty_class: uncertainty_for(normalized),
      reason: decision.rationale,
      role_provenance: role_provenance(config, live)
    })
  end

  def advancement_contradiction(store, normalized, identity_decision, user_id) do
    config = configs().advancement_contradiction
    packet = Packet.for_story(store, normalized, config, user_id)

    if low_authority_private_public?(store, normalized, identity_decision) do
      refusal(
        config,
        packet,
        normalized,
        :role_authority,
        "Private Flynn evidence cannot be used for a public story mutation."
      )
    else
      live = LiveGibson.invoke(config.role, config, packet)
      decision = decision_from(live.output, normalized, config)

      proposal =
        normalized
        |> Builder.build(decision)
        |> annotate_proposal(config, packet, :advancement_or_story_transformation, normalized)

      live_invocation(live, config, packet, %{
        outcome: :write,
        operation_family: :advancement_or_story_transformation,
        proposal: proposal,
        evidence_refs: proposal.evidence_refs,
        confidence: proposal.confidence,
        uncertainty_class: proposal.uncertainty_class,
        reason: proposal.rationale,
        role_provenance: role_provenance(config, live)
      })
    end
  end

  def flynn_relative_authoring(store, seen_state, user_id, stage) do
    config = configs().flynn_relative_authoring
    packet = Packet.for_authoring(store, seen_state, config, user_id)
    live = LiveGibson.invoke(config.role, config, packet)
    output = authoring_output(live.output, store, user_id, stage)

    live_invocation(live, config, packet, %{
      outcome:
        if(output.operation_family == :create_authored_delta, do: :write, else: :abstained),
      operation_family: output.operation_family,
      output: output,
      evidence_refs: output.evidence_refs,
      confidence: output.confidence,
      uncertainty_class: if(output.verified, do: :bounded_evidence, else: :ungrounded_output),
      reason: output.rationale
    })
  end

  def follow_on_review(store, activation, user_id) do
    config = configs().follow_on_review
    packet = Packet.for_follow_on(store, activation, config, user_id)
    live = LiveGibson.invoke(config.role, config, packet)

    live_invocation(live, config, packet, %{
      outcome: required_atom(live.output, :outcome, [:abstained, :refused, :write]),
      operation_family:
        required_atom(live.output, :operation_family, [:mark_no_op, :create_question]),
      caused_by_fixture_id: activation.caused_by_fixture_id,
      evidence_refs: activation.evidence_refs,
      confidence: required_confidence(live.output, :confidence),
      uncertainty_class:
        required_atom(live.output, :uncertainty_class, [
          :bounded_evidence,
          :open_question,
          :source_gap
        ]),
      reason: required_string(live.output, :rationale)
    })
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

  defp config(role, version, task_prompt, output_schema) do
    prompt_body = @system_prompt <> "\n" <> task_prompt <> "\n" <> Jason.encode!(output_schema)

    %{
      role: role,
      config_version: version,
      system_prompt: @system_prompt,
      task_prompt: task_prompt,
      output_schema: output_schema,
      prompt_body: prompt_body,
      prompt_version_hash: LiveGibson.hash(prompt_body)
    }
  end

  defp decision_from(output, normalized, config) do
    %{
      role: config.role,
      config_version: config.config_version,
      prompt_version_hash: config.prompt_version_hash,
      classification:
        required_atom(output, :classification, [
          :new_story,
          :substantive_update,
          :repeated_noop_input,
          :conflict_correction,
          :color_spin_without_structural_change,
          :stale_background_state
        ]),
      story_key: required_story_key(output, :story_key),
      confidence: required_confidence(output, :confidence),
      changed_facts: required_map(output, :changed_facts),
      watch_ids: required_string_list(output, :watch_ids),
      rationale: required_string(output, :rationale),
      parent_story_key: required_optional_string(output, :parent_story_key),
      conflicts: required_conflict_list(output, :conflicts),
      fixture_id: normalized.fixture_id
    }
  end

  defp live_invocation(live, config, packet, attrs) do
    attrs
    |> Map.merge(%{
      role: config.role,
      config: config,
      packet: packet,
      packet_hash: Packet.hash(packet),
      decision_source: live.decision_source,
      deterministic_product_logic: false,
      agent_output_hash: live.agent_output_hash,
      producer_kind: live.producer_kind,
      producer_id: live.producer_id,
      artifact_ref: nil,
      model_family: live.model_family,
      model_route: live.model_route,
      model: live.model,
      runtime_target: live.runtime_target,
      invocation_transport_id: live.response_id,
      prompt_body_hash: live.prompt_body_hash,
      system_prompt_hash: live.system_prompt_hash,
      prompt_body: live.prompt_body,
      raw_model_content: live.raw_model_content,
      duration_ms: live.duration_ms
    })
  end

  defp authoring_output(output, store, user_id, stage) do
    output_id = "authored-output:" <> Integer.to_string(length(store.commits))
    raw_evidence_refs = required_string_list(output, :evidence_refs)
    evidence_refs = Enum.filter(raw_evidence_refs, &Map.has_key?(store.nodes, &1))
    touched_story_keys = required_string_list(output, :touched_story_keys)
    bullets = required_string_list(output, :bullets)

    operation_family =
      required_atom(output, :operation_family, [:create_authored_delta, :mark_no_op])

    rationale = required_string(output, :rationale)
    confidence = required_confidence(output, :confidence)
    verified = required_boolean(output, :verified)

    %{
      output_id: output_id,
      user_id: user_id,
      operation_family: operation_family,
      bullets: bullets,
      text: "what changed for Flynn since last seen, and why\n" <> Enum.join(bullets, "\n"),
      evidence_packet: %{user_id: user_id, evidence_refs: evidence_refs},
      sentence_evidence:
        Enum.map(bullets, fn bullet ->
          %{
            text: bullet,
            evidence_refs: evidence_refs,
            claim_refs: claim_refs_for(touched_story_keys, store)
          }
        end),
      evidence_refs: evidence_refs,
      touched_story_keys: touched_story_keys,
      verified: verified and evidence_refs != [],
      confidence: confidence,
      rationale: rationale,
      stage: stage
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
      decision_source: :mechanical_permission_refusal,
      deterministic_product_logic: false,
      agent_output_hash: Packet.hash(%{fixture_id: normalized.fixture_id, reason: reason}),
      reason: reason,
      producer_kind: :mechanical_write_mediator,
      producer_id: "prime-radiant-write-gate",
      artifact_ref: nil
    }
  end

  defp role_provenance(config, live) do
    %{
      role: config.role,
      config_version: config.config_version,
      prompt_version_hash: config.prompt_version_hash,
      prompt_body_hash: live.prompt_body_hash,
      model_family: live.model_family,
      producer_id: live.producer_id
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
        Enum.all?(story.inputs, &(get_in(store.nodes, [&1, :attrs, :acl, "privacy"]) == "public"))
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

  defp required_atom(output, key, allowed) do
    value = output |> required_value(key) |> atom()

    if value in allowed do
      value
    else
      raise "live Gibson output #{key} was outside the allowed schema"
    end
  end

  defp required_story_key(output, key) do
    output
    |> required_string(key)
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
    |> case do
      "" -> raise "live Gibson output #{key} was blank"
      story_key -> story_key
    end
  end

  defp confidence(value, _default) when is_number(value), do: max(0.0, min(value * 1.0, 1.0))

  defp confidence(value, default) when is_binary(value),
    do: value |> Float.parse() |> elem_or(default)

  defp confidence(_value, default), do: default

  defp elem_or({number, _}, _default), do: max(0.0, min(number, 1.0))
  defp elem_or(:error, default), do: default

  defp required_confidence(output, key) do
    case required_value(output, key) do
      value when is_number(value) ->
        confidence(value, nil)

      value when is_binary(value) ->
        case Float.parse(value) do
          {number, _} -> confidence(number, nil)
          :error -> raise "live Gibson output #{key} was not numeric"
        end

      _ ->
        raise "live Gibson output #{key} was not numeric"
    end
  end

  defp required_string(output, key) do
    case required_value(output, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise "live Gibson output #{key} was not a non-empty string"
    end
  end

  defp required_map(output, key) do
    case required_value(output, key) do
      value when is_map(value) -> value
      _ -> raise "live Gibson output #{key} was not an object"
    end
  end

  defp required_string_list(output, key) do
    case required_value(output, key) do
      value when is_list(value) -> Enum.map(value, &to_string/1)
      _ -> raise "live Gibson output #{key} was not a list"
    end
  end

  defp required_boolean(output, key) do
    case required_value(output, key) do
      value when is_boolean(value) -> value
      _ -> raise "live Gibson output #{key} was not a boolean"
    end
  end

  defp required_conflict_list(output, key) do
    case required_value(output, key) do
      value when is_list(value) ->
        Enum.map(value, fn
          item when is_map(item) -> item
          item -> %{source_ref: to_string(item), relation: "conflicts_with_prior_soup"}
        end)

      _ ->
        raise "live Gibson output #{key} was not a list"
    end
  end

  defp required_optional_string(output, key) do
    case Map.fetch(output, key) do
      {:ok, value} when value in [nil, ""] -> nil
      {:ok, value} when is_binary(value) -> value
      {:ok, _} -> raise "live Gibson output #{key} was not null or a string"
      :error -> raise "live Gibson output omitted #{key}"
    end
  end

  defp required_value(output, key) do
    case Map.fetch(output, key) do
      {:ok, value} -> value
      :error -> raise "live Gibson output omitted #{key}"
    end
  end

  defp atom(value) when is_atom(value), do: value

  defp atom(value),
    do: value |> to_string() |> String.downcase() |> String.replace("-", "_") |> String.to_atom()

  defp claim_refs_for(story_keys, store) do
    story_keys
    |> Enum.flat_map(fn story_key ->
      case store.stories[story_key] do
        nil -> []
        story -> story.structural_facts |> Map.keys() |> Enum.map(&"claim:#{story_key}:#{&1}")
      end
    end)
    |> Enum.filter(&Map.has_key?(store.nodes, &1))
    |> Enum.uniq()
  end
end
