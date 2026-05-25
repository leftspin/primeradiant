defmodule Primeradiant.Proposals.Builder do
  @moduledoc false

  alias Primeradiant.Proposals.Proposal

  def build(normalized, decision) do
    %Proposal{
      id: "proposal:" <> normalized.fixture_id,
      agent_run_id: "agent-run:fixture-story-seeker",
      actor_id: "flynn",
      fixture_id: normalized.fixture_id,
      story_key: decision.story_key,
      classification: decision.classification,
      confidence: decision.confidence,
      rationale: decision.rationale,
      evidence_refs: [normalized.fixture_id],
      status: :pending,
      agent_role: :fixture_story_seeker,
      agent_config_version: "fixture-story-seeker.legacy",
      prompt_version_hash: "prompt:fixture-story-seeker:legacy",
      input_packet_hash: "legacy-packet:" <> normalized.fixture_id,
      visibility: normalized.acl["privacy"] || "public",
      uncertainty_class: :bounded_evidence,
      ops:
        ops_for(normalized, decision)
        |> add_op_provenance([normalized.fixture_id], decision.confidence)
    }
  end

  defp ops_for(normalized, %{classification: :new_story, story_key: story_key} = decision) do
    [
      create_input_op(normalized),
      %{
        op: :create_story,
        story_key: story_key,
        title: normalized.title,
        observed_at: normalized.observed_at
      },
      %{
        op: :attach_input,
        story_key: story_key,
        input_id: normalized.fixture_id,
        edge_type: :supports
      },
      %{op: :merge_facts, story_key: story_key, facts: normalized.facts},
      %{op: :merge_background, story_key: story_key, background: normalized.background},
      %{op: :append_colors, story_key: story_key, colors: normalized.colors},
      %{op: :add_questions, story_key: story_key, questions: normalized.questions},
      %{
        op: :record_event,
        story_key: story_key,
        classification: :new_story,
        input_id: normalized.fixture_id,
        changed_facts: Map.get(decision, :changed_facts, %{})
      }
    ]
    |> maybe_add(:attach_story_part_of, Map.get(decision, :parent_story_key) != nil, %{
      child_story_key: story_key,
      parent_story_key: Map.get(decision, :parent_story_key),
      edge_type: :part_of
    })
    |> add_watch_edges(story_key, Map.get(decision, :watch_ids, []))
  end

  defp ops_for(
         normalized,
         %{classification: classification, story_key: story_key, changed_facts: changed_facts} =
           decision
       ) do
    base = [
      create_input_op(normalized),
      %{
        op: :attach_input,
        story_key: story_key,
        input_id: normalized.fixture_id,
        edge_type: edge_type(classification)
      },
      %{
        op: :record_event,
        story_key: story_key,
        classification: classification,
        input_id: normalized.fixture_id,
        changed_facts: changed_facts
      },
      %{op: :mark_last_seen_input, story_key: story_key, observed_at: normalized.observed_at}
    ]

    base
    |> maybe_add(:merge_facts, changed_facts != %{}, %{story_key: story_key, facts: changed_facts})
    |> maybe_add(:merge_background, normalized.background != %{}, %{
      story_key: story_key,
      background: normalized.background
    })
    |> maybe_add(:append_colors, normalized.colors != [], %{
      story_key: story_key,
      colors: normalized.colors
    })
    |> maybe_add(:add_questions, normalized.questions != %{}, %{
      story_key: story_key,
      questions: normalized.questions
    })
    |> maybe_add(:record_conflicts, Map.get(decision, :conflicts, []) != [], %{
      story_key: story_key,
      conflicts: Map.get(decision, :conflicts, [])
    })
    |> maybe_add(:mark_state, classification == :stale_background_state, %{
      story_key: story_key,
      state: :background
    })
  end

  defp maybe_add(ops, op, true, payload), do: ops ++ [Map.put(payload, :op, op)]
  defp maybe_add(ops, _op, false, _payload), do: ops

  defp create_input_op(normalized) do
    %{
      op: :create_input,
      input_id: normalized.fixture_id,
      title: normalized.title,
      source_type: normalized.source_type,
      observed_at: normalized.observed_at,
      acl: normalized.acl
    }
  end

  defp edge_type(:attach_to_existing_story), do: :supports
  defp edge_type(:substantive_update), do: :updates
  defp edge_type(:repeated_noop_input), do: :duplicates
  defp edge_type(:color_spin_without_structural_change), do: :adds_color
  defp edge_type(:stale_background_state), do: :adds_color
  defp edge_type(:conflict_correction), do: :contradicts

  defp add_watch_edges(ops, story_key, watch_ids) do
    watch_ops =
      Enum.flat_map(watch_ids, fn watch_id ->
        [
          %{
            op: :attach_watch,
            story_key: story_key,
            watch_id: watch_id,
            edge_type: :watch_applies_to
          }
        ]
      end)

    ops ++ watch_ops
  end

  defp add_op_provenance(ops, evidence_refs, confidence) do
    Enum.map(ops, fn op ->
      op
      |> Map.put(:evidence_refs, evidence_refs)
      |> Map.put(:confidence, confidence)
    end)
  end
end
