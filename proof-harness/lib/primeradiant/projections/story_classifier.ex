defmodule Primeradiant.Projections.StoryClassifier do
  @moduledoc false

  def decide(store, normalized) do
    case best_story_key(store, normalized) do
      nil -> new_story_decision(store, normalized)
      story_key -> classify_against_story(store.stories[story_key], normalized)
    end
  end

  def stale_story_keys(store, reference_iso) do
    reference = DateTime.from_iso8601(reference_iso) |> elem(1)

    store.stories
    |> Enum.flat_map(fn {story_key, story} ->
      story_time = DateTime.from_iso8601(story.last_material_at || story.updated_at) |> elem(1)
      days = DateTime.diff(reference, story_time, :day)

      if days >= 7, do: [story_key], else: []
    end)
  end

  defp new_story_decision(store, normalized) do
    story_key = story_key(normalized)

    %{
      classification: :new_story,
      story_key: story_key,
      confidence: 0.93,
      rationale:
        "No existing story shared enough topic overlap; created fresh story from structural facts.",
      changed_facts: normalized.facts,
      parent_story_key: structurally_adjacent_story_key(store, normalized),
      watch_ids: attached_watch_ids(store, story_key)
    }
  end

  defp classify_against_story(story, normalized) do
    conflicts = conflicts(story, normalized)
    changed_facts = changed_facts(story, normalized)

    cond do
      MapSet.member?(story.input_fingerprints, normalized.fingerprint) ->
        base_decision(
          story,
          :repeated_noop_input,
          normalized,
          0.99,
          %{},
          "Exact fingerprint already ingested; adds no new story state."
        )

      conflicts != [] ->
        base_decision(
          story,
          :conflict_correction,
          normalized,
          0.95,
          normalized.facts,
          "Incoming structural fact conflicts with committed story state."
        )
        |> Map.put(:conflicts, conflicts)

      normalized.facts != %{} and changed_facts == normalized.facts and
          map_size(story.structural_facts) == 0 ->
        base_decision(
          story,
          :attach_to_existing_story,
          normalized,
          0.88,
          %{},
          "Input belongs to the same story but only establishes the first structured facts for it."
        )

      changed_facts != %{} ->
        base_decision(
          story,
          :substantive_update,
          normalized,
          0.91,
          changed_facts,
          "Input adds or revises structural story facts."
        )

      normalized.colors != [] ->
        base_decision(
          story,
          :color_spin_without_structural_change,
          normalized,
          0.82,
          %{},
          "Input changes framing/context but not structural story facts."
        )

      normalized.background != %{} or normalized.questions != %{} ->
        base_decision(
          story,
          :stale_background_state,
          normalized,
          0.8,
          %{},
          "Input is related background/check-in state without a material story development."
        )

      normalized.facts == %{} and normalized.colors == [] and normalized.background == %{} and
          normalized.questions == %{} ->
        base_decision(
          story,
          :attach_to_existing_story,
          normalized,
          0.77,
          %{},
          "Input clearly belongs to the story but contributes no structured change yet."
        )

      true ->
        base_decision(
          story,
          :repeated_noop_input,
          normalized,
          0.78,
          %{},
          "Related input produced no new structural delta."
        )
    end
  end

  defp base_decision(story, classification, _normalized, confidence, changed_facts, rationale) do
    %{
      classification: classification,
      story_key: story.story_key,
      confidence: confidence,
      changed_facts: changed_facts,
      watch_ids: story.watch_ids,
      rationale: rationale
    }
  end

  defp best_story_key(store, normalized) do
    store.stories
    |> Enum.map(fn {story_key, story} ->
      overlap = MapSet.intersection(story.topic_tokens, normalized.topic_tokens) |> MapSet.size()
      {story_key, overlap}
    end)
    |> Enum.filter(fn {story_key, overlap} ->
      overlap >= 2 and structurally_compatible?(store.stories[story_key], normalized)
    end)
    |> Enum.sort_by(fn {_story_key, overlap} -> -overlap end)
    |> List.first()
    |> case do
      nil -> nil
      {story_key, _overlap} -> story_key
    end
  end

  defp structurally_adjacent_story_key(store, normalized) do
    store.stories
    |> Enum.map(fn {story_key, story} ->
      overlap = MapSet.intersection(story.topic_tokens, normalized.topic_tokens) |> MapSet.size()
      {story_key, overlap}
    end)
    |> Enum.filter(fn {story_key, overlap} ->
      overlap >= 2 and not structurally_compatible?(store.stories[story_key], normalized)
    end)
    |> Enum.sort_by(fn {_story_key, overlap} -> -overlap end)
    |> List.first()
    |> case do
      nil -> nil
      {story_key, _overlap} -> story_key
    end
  end

  defp structurally_compatible?(story, normalized) do
    if story.structural_facts == %{} or normalized.facts == %{} do
      true
    else
      exact_shared_facts(story, normalized) != []
    end
  end

  defp exact_shared_facts(story, normalized) do
    Enum.filter(normalized.facts, fn {key, value} ->
      Map.get(story.structural_facts, key) == value
    end)
  end

  defp attached_watch_ids(store, story_key) do
    store.watch_index
    |> Enum.filter(fn {_id, watch} -> story_match?(watch["match_any"], story_key) end)
    |> Enum.map(fn {id, _watch} -> id end)
  end

  defp story_match?(tokens, story_key) do
    Enum.any?(tokens || [], &String.contains?(story_key, &1))
  end

  defp story_key(normalized) do
    normalized.title
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9\s_-]/u, " ")
    |> String.split(~r/\s+/, trim: true)
    |> Enum.reject(
      &(&1 in ~w(a an and are as at be before but by for from has have in into is it its of on or that the their this to was were will with re))
    )
    |> Enum.uniq()
    |> Enum.take(3)
    |> Enum.join("-")
    |> case do
      "" -> normalized.fixture_id
      key -> key
    end
  end

  defp changed_facts(story, normalized) do
    Enum.reduce(normalized.facts, %{}, fn {key, value}, acc ->
      if Map.get(story.structural_facts, key) == value do
        acc
      else
        Map.put(acc, key, value)
      end
    end)
  end

  defp conflicts(story, normalized) do
    if correction_input?(normalized) do
      Enum.flat_map(normalized.facts, fn {key, value} ->
        case Map.get(story.structural_facts, key) do
          nil -> []
          ^value -> []
          old_value -> [%{fact: key, prior: old_value, incoming: value}]
        end
      end)
    else
      []
    end
  end

  defp correction_input?(normalized) do
    Map.has_key?(normalized.facts, "correction_scope") or
      String.contains?(String.downcase(normalized.title), "correct") or
      String.contains?(String.downcase(normalized.title), "contradiction")
  end
end
