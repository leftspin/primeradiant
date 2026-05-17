defmodule Primeradiant.StoryIdentity.Decider do
  @moduledoc false

  alias Primeradiant.Ingestion.RealNormalizer
  @identity_anchor_fact_keys ~w(venue location actor affected_group group date)

  def decide(_state, envelope, candidates) do
    facts = RealNormalizer.facts(envelope)
    colors = envelope.extracted.colors
    questions = envelope.extracted.questions
    best = choose_candidate(candidates, facts)
    conflict = best && first_conflict(best, facts, envelope)

    cond do
      low_confidence?(envelope, candidates) ->
        decision(envelope, :abstain, nil, %{
          rationale: "Identity evidence below threshold.",
          confidence: 0.42,
          status: :needs_more_evidence
        })

      stale_input?(envelope) and best ->
        decision(envelope, :stale, best, %{
          changed_facts: %{},
          rationale: "Background check-in with no new activity."
        })

      conflict ->
        decision(envelope, :conflict, best, %{
          changed_facts: facts,
          conflicting_facts: [conflict],
          rationale: "Incoming fact value conflicts with current story fact on compatible scope."
        })

      colors != [] and facts == %{} and best ->
        decision(envelope, :color, best, %{
          colors: Enum.map(colors, & &1.text),
          rationale: "Framing item discusses an existing story without structural facts."
        })

      best && best.signals.exact_content_duplicate? ->
        decision(envelope, :no_op, best, %{
          rationale: "Exact content already attached to candidate story."
        })

      best && facts != %{} && facts_subset?(facts, best, envelope) && questions == [] ->
        decision(envelope, :no_op, best, %{
          rationale: "Input repeats current story facts without material delta."
        })

      best && part_of_input?(envelope) && best.signals.topic_overlap? && facts != %{} ->
        decision(envelope, :part_of, best, %{
          changed_facts: facts,
          rationale:
            "Shared topic with distinct scoped situation; keep separate story and attach part_of relation."
        })

      best && identity_anchor_mismatch?(facts, best.story.structural_facts) ->
        decision(envelope, :split, best, %{
          changed_facts: facts,
          rationale:
            "Shared topic with incompatible identity anchors; create distinct story identity."
        })

      best && best.signals.structural_compatible? ->
        decision(envelope, :attach, best, %{
          changed_facts: new_facts(facts, best, envelope),
          rationale: "Shared structural evidence supports existing story identity."
        })

      best && best.signals.topic_overlap? && facts != %{} ->
        decision(envelope, :split, best, %{
          changed_facts: facts,
          rationale:
            "Topic overlap without structural compatibility; create distinct story identity."
        })

      facts != %{} ->
        decision(envelope, :split, nil, %{
          changed_facts: facts,
          rationale: "No compatible candidate; create new story identity."
        })

      true ->
        decision(envelope, :abstain, nil, %{
          rationale: "No structural facts or compatible candidate.",
          confidence: 0.4,
          status: :needs_more_evidence
        })
    end
  end

  defp decision(envelope, type, candidate, opts) do
    facts = Map.get(opts, :changed_facts, RealNormalizer.facts(envelope))
    story_key = candidate && candidate.story.story_key
    proposed_key = if type in [:split, :part_of], do: proposed_story_key(envelope), else: nil

    %{
      input_id: envelope.input_id,
      input_ref: envelope.input_ref,
      decision_type: type,
      candidate_story_id: candidate && candidate.story.id,
      candidate_story_key: story_key,
      proposed_story_key: proposed_key,
      parent_story_id: if(type == :part_of, do: candidate.story.id),
      parent_story_key: if(type == :part_of, do: story_key),
      changed_facts: facts,
      conflicting_facts: Map.get(opts, :conflicting_facts, []),
      colors: Map.get(opts, :colors, []),
      questions: Map.new(envelope.extracted.questions, &{&1.question_key, &1.text}),
      rationale: opts.rationale,
      confidence: Map.get(opts, :confidence, 0.86),
      status: Map.get(opts, :status, :pending),
      evidence_refs: evidence_refs_for(envelope, type, facts),
      abstained_candidates: abstained(candidate_list(candidate), story_key),
      identity_anchors: identity_anchors(envelope)
    }
  end

  defp candidate_list(nil), do: []
  defp candidate_list(candidate), do: Map.get(candidate, :all_candidates, [candidate])

  defp choose_candidate([], _facts), do: nil

  defp choose_candidate(candidates, facts) do
    candidates
    |> Enum.max_by(&candidate_score(&1, facts))
    |> Map.put(:all_candidates, candidates)
  end

  defp candidate_score(candidate, facts) do
    mismatch_penalty =
      if identity_anchor_mismatch?(facts, candidate.story.structural_facts), do: -100, else: 0

    mismatch_penalty +
      if(candidate.signals.exact_content_duplicate?, do: 100, else: 0) +
      if(candidate.signals.structural_compatible?, do: 50, else: 0) +
      length(candidate.fact_value_overlap) * 10 +
      length(candidate.fact_key_overlap) * 5 +
      length(candidate.token_overlap)
  end

  defp first_conflict(candidate, incoming_facts, envelope) do
    incoming_facts
    |> Enum.find_value(fn {key, incoming_value} ->
      prior_fact = prior_fact_version(candidate, key)
      prior_value = prior_fact && prior_fact.fact_value
      incoming_scope = incoming_scope_for_key(envelope, key)
      prior_scope = prior_fact && prior_fact.time_scope
      claim = incoming_claim_for_key(envelope, key)

      if not is_nil(prior_value) and prior_value != incoming_value and conflictable_fact?(key) and
           scopes_compatible?(incoming_scope, prior_scope) and conflict_comparable_claim?(claim) do
        %{
          fact_key: key,
          prior_fact_version_id: prior_fact.id,
          prior_value: prior_value,
          incoming_value: incoming_value,
          comparison_type: "mutually_exclusive_value",
          scope: %{"story_id" => candidate.story.id, "time_scope" => incoming_scope},
          proposed_status: "open",
          evidence_refs: candidate.evidence_refs,
          confidence: 0.88
        }
      end
    end)
  end

  defp prior_fact_version(candidate, key) do
    candidate.visible_facts
    |> Enum.find(&(&1.fact_key == key))
  end

  defp incoming_scope_for_key(envelope, key) do
    incoming_claim_for_key(envelope, key)
    |> case do
      nil -> "current"
      claim -> claim.time_scope
    end
  end

  defp incoming_claim_for_key(envelope, key),
    do: Enum.find(envelope.extracted.claims, &(&1.fact_key == key))

  defp conflict_comparable_claim?(nil), do: false

  defp conflict_comparable_claim?(claim) do
    asserted_by = get_in(claim, [:attribution, "asserted_by"])

    claim.polarity == "positive" and
      claim.modality in ["asserted", "reported", "corrected"] and
      claim.downstream_use in ["identity_candidate", "conflict_candidate"] and
      asserted_by not in [nil, "unknown"]
  end

  defp scopes_compatible?(nil, _prior_scope), do: true
  defp scopes_compatible?(_incoming_scope, nil), do: true
  defp scopes_compatible?(scope, scope), do: true
  defp scopes_compatible?("current", _prior_scope), do: true
  defp scopes_compatible?(_incoming_scope, "current"), do: true
  defp scopes_compatible?(_incoming_scope, _prior_scope), do: false

  defp low_confidence?(envelope, _candidates) do
    status = get_in(envelope, [:extracted, :quality, "status"])

    cond do
      status in ["low_confidence", "refused"] ->
        true

      status in ["usable", "partial"] ->
        false

      true ->
        RealNormalizer.facts(envelope) == %{} and envelope.extracted.colors == [] and
          envelope.extracted.questions == []
    end
  end

  defp identity_anchor_mismatch?(incoming, existing) do
    Enum.any?(@identity_anchor_fact_keys, fn key ->
      Map.has_key?(incoming, key) and Map.has_key?(existing, key) and
        incoming[key] != existing[key]
    end)
  end

  defp conflictable_fact?(key), do: key not in @identity_anchor_fact_keys

  defp stale_input?(envelope),
    do:
      String.match?(
        String.downcase(envelope.body_text),
        ~r/\b(no new activity|no change|background check)\b/
      )

  defp part_of_input?(envelope),
    do: String.match?(String.downcase(envelope.body_text), ~r/\bpart of|substory|facet\b/)

  defp facts_subset?(incoming, candidate, envelope) do
    Enum.all?(incoming, fn {key, value} ->
      Enum.any?(candidate.visible_facts, fn fact ->
        fact.fact_key == key and fact.fact_value == value and
          scopes_compatible?(incoming_scope_for_key(envelope, key), fact.time_scope)
      end)
    end)
  end

  defp new_facts(incoming, candidate, envelope) do
    incoming
    |> Enum.reject(fn {key, value} ->
      Enum.any?(candidate.visible_facts, fn fact ->
        fact.fact_key == key and fact.fact_value == value and
          scopes_compatible?(incoming_scope_for_key(envelope, key), fact.time_scope)
      end)
    end)
    |> Map.new()
  end

  defp proposed_story_key(envelope) do
    entity = envelope.extracted.entities |> List.first() |> then(&(&1 && &1.entity_key))
    claim = List.first(envelope.extracted.claims)

    [entity, claim && claim.fact_key, claim && claim.fact_value]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("-")
    |> String.slice(0, 72)
    |> String.trim("-")
  end

  defp identity_anchors(envelope) do
    entity_anchors = Enum.map(envelope.extracted.entities, & &1.entity_key)
    fact_anchors = envelope.extracted.claims |> Enum.flat_map(&[&1.fact_key, &1.fact_value])

    (entity_anchors ++ fact_anchors ++ envelope.extracted.topic_tokens)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp evidence_refs_for(envelope, type, facts) do
    fact_keys = Map.keys(facts || %{})

    claim_refs =
      envelope.extracted.claims
      |> Enum.filter(fn claim ->
        fact_keys == [] or claim.fact_key in fact_keys or claim.structural_candidate == true
      end)
      |> Enum.flat_map(&(&1.evidence_refs || []))

    color_refs =
      if type == :color,
        do: Enum.flat_map(envelope.extracted.colors, &(&1.evidence_refs || [])),
        else: []

    question_refs =
      if type in [:attach, :split, :part_of],
        do: Enum.flat_map(envelope.extracted.questions, &(&1.evidence_refs || [])),
        else: []

    event_refs = Enum.flat_map(envelope.extracted.events || [], &(&1.evidence_refs || []))

    stance_refs =
      Enum.flat_map(envelope.extracted.source_stance || [], &(&1["evidence_refs"] || []))

    (claim_refs ++ color_refs ++ question_refs ++ event_refs ++ stance_refs)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> case do
      [] -> [%{"input_ref" => envelope.input_ref}]
      refs -> refs
    end
  end

  defp abstained([], _story_key), do: []

  defp abstained(candidates, story_key),
    do:
      Enum.reject(candidates, &(&1.story.story_key == story_key))
      |> Enum.map(&%{story_id: &1.story.id, reason: "weaker identity evidence"})
end
