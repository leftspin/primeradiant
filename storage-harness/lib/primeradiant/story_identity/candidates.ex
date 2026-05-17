defmodule Primeradiant.StoryIdentity.Candidates do
  @moduledoc false

  alias Primeradiant.Ingestion.RealNormalizer
  alias Primeradiant.StorageHarness.State

  def retrieve(%State{} = state, envelope, actor_id) do
    incoming_tokens = MapSet.new(envelope.extracted.topic_tokens)
    incoming_facts = RealNormalizer.facts(envelope)

    state.stories
    |> Enum.filter(&visible_story?(state, &1, actor_id))
    |> Enum.map(fn story ->
      visible_facts = visible_facts(state, story, actor_id)
      visible_events = visible_events(state, story, actor_id)
      visible_structural_facts = Map.new(visible_facts, &{&1.fact_key, &1.fact_value})
      story_tokens = visible_story_tokens(state, story, visible_facts, actor_id)
      token_overlap = MapSet.intersection(incoming_tokens, story_tokens) |> MapSet.to_list()

      fact_key_overlap =
        Map.keys(incoming_facts) --
          (Map.keys(incoming_facts) -- Map.keys(visible_structural_facts))

      fact_value_overlap = fact_value_overlap(incoming_facts, visible_structural_facts)
      evidence_refs = visible_input_refs(state, story, actor_id)
      same_source_thread? = same_source_thread?(visible_events, state, envelope, actor_id)
      scoped_fact_continuity? = scoped_fact_continuity?(incoming_facts, visible_facts, envelope)

      visible_story = %{
        story
        | title: visible_story_title(story, visible_events, state),
          first_observed_at: visible_story_first_observed_at(story, visible_events),
          updated_at_story: visible_story_updated_at(story, visible_events),
          last_material_at: nil,
          background_facts: %{},
          colors: [],
          questions: %{},
          attrs: %{},
          version: length(visible_events),
          state: visible_story_state(story, visible_events),
          structural_facts: visible_structural_facts,
          topic_tokens: MapSet.to_list(story_tokens)
      }

      %{
        story: visible_story,
        token_overlap: token_overlap,
        fact_key_overlap: fact_key_overlap,
        fact_value_overlap: fact_value_overlap,
        evidence_refs: evidence_refs,
        visible_facts: visible_facts,
        visible_events: visible_events,
        signals: %{
          exact_content_duplicate?:
            exact_content_duplicate?(visible_events, state, envelope, actor_id),
          structural_compatible?:
            fact_value_overlap != [] or scoped_fact_continuity? or same_source_thread?,
          topic_overlap?: token_overlap != [],
          same_source_thread?: same_source_thread?
        }
      }
    end)
    |> Enum.filter(fn candidate ->
      candidate.signals.exact_content_duplicate? or candidate.signals.topic_overlap? or
        candidate.fact_key_overlap != [] or candidate.fact_value_overlap != []
    end)
  end

  defp fact_value_overlap(incoming, existing) do
    incoming
    |> Enum.filter(fn {key, value} -> Map.get(existing, key) == value end)
    |> Enum.map(fn {key, _value} -> key end)
  end

  defp visible_story?(state, story, actor_id) do
    visible_input_refs(state, story, actor_id) != []
  end

  defp visible_input_refs(state, story, actor_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.map(& &1.input_id)
    |> Enum.uniq()
    |> Enum.map(fn input_id -> Enum.find(state.inputs, &(&1.id == input_id)) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(&input_visible?(&1, actor_id))
    |> Enum.map(&"#{&1.source_type}:#{&1.external_id}")
  end

  defp visible_facts(state, story, actor_id) do
    state.story_fact_versions
    |> Enum.filter(&(&1.story_id == story.id and &1.status == "current"))
    |> Enum.filter(fn fact ->
      state.inputs
      |> Enum.find(&(&1.id == fact.input_id))
      |> input_visible?(actor_id)
    end)
  end

  defp visible_events(state, story, actor_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.filter(fn event ->
      state.inputs
      |> Enum.find(&(&1.id == event.input_id))
      |> input_visible?(actor_id)
    end)
  end

  defp exact_content_duplicate?(visible_events, state, envelope, actor_id) do
    visible_events
    |> Enum.any?(fn event ->
      input = Enum.find(state.inputs, &(&1.id == event.input_id))

      input && input_visible?(input, actor_id) &&
        input.content_sha256 == envelope.provenance.content_sha256
    end)
  end

  defp same_source_thread?(visible_events, state, envelope, actor_id) do
    source_actor = get_in(envelope.provenance, [:source_actor, "stable_id"])

    private_scope?(envelope) &&
      source_actor &&
      visible_events
      |> Enum.any?(fn event ->
        input = Enum.find(state.inputs, &(&1.id == event.input_id))

        input && input_visible?(input, actor_id) &&
          get_in(input.normalized, ["source_actor", "stable_id"]) == source_actor
      end)
  end

  defp scoped_fact_continuity?(incoming_facts, visible_facts, envelope) do
    scopes = RealNormalizer.fact_scopes(envelope)

    Enum.any?(incoming_facts, fn {key, _value} ->
      Enum.any?(visible_facts, fn fact ->
        fact.fact_key == key and
          not scopes_compatible?(Map.get(scopes, key, "current"), fact.time_scope)
      end)
    end)
  end

  defp private_scope?(envelope), do: get_in(envelope.acl, ["privacy"]) == "private"

  defp scopes_compatible?(nil, _prior_scope), do: true
  defp scopes_compatible?(_incoming_scope, nil), do: true
  defp scopes_compatible?(scope, scope), do: true
  defp scopes_compatible?("current", _prior_scope), do: true
  defp scopes_compatible?(_incoming_scope, "current"), do: true
  defp scopes_compatible?(_incoming_scope, _prior_scope), do: false

  defp visible_story_tokens(state, story, visible_facts, actor_id) do
    fact_tokens = Enum.flat_map(visible_facts, &[&1.fact_key, &1.fact_value])

    input_tokens =
      state.story_events
      |> Enum.filter(&(&1.story_id == story.id))
      |> Enum.map(fn event -> Enum.find(state.inputs, &(&1.id == event.input_id)) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&input_visible?(&1, actor_id))
      |> Enum.flat_map(&(&1.topic_tokens || []))

    MapSet.new(fact_tokens ++ input_tokens)
  end

  defp visible_story_title(_story, [event | _events], state) do
    input = Enum.find(state.inputs, &(&1.id == event.input_id))
    input.title || "Visible story"
  end

  defp visible_story_title(story, _events, _state), do: story.story_key

  defp visible_story_first_observed_at(story, []), do: story.first_observed_at

  defp visible_story_first_observed_at(_story, events),
    do: events |> Enum.map(& &1.observed_at) |> Enum.min(DateTime)

  defp visible_story_updated_at(story, []), do: story.updated_at_story

  defp visible_story_updated_at(_story, events),
    do: events |> Enum.map(& &1.observed_at) |> Enum.max(DateTime)

  defp visible_story_state(story, []), do: story.state
  defp visible_story_state(_story, _events), do: "active"

  defp input_visible?(nil, _actor_id), do: false

  defp input_visible?(input, actor_id) do
    acl = input.acl || %{"privacy" => "public"}
    acl["privacy"] == "public" or actor_id in (acl["participants"] || [])
  end
end
