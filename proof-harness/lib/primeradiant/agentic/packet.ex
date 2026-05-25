defmodule Primeradiant.Agentic.Packet do
  @moduledoc false

  def for_source(raw, role_config) do
    acl = raw["acl"] || %{"privacy" => "public"}

    packet(%{
      role: role_config.role,
      output_visibility: acl["privacy"] || "public",
      source_scope: raw["source_type"],
      evidence_refs: [raw["fixture_id"]],
      snippets: [snippet(raw["body_text"] || "")],
      input: raw,
      visible_story_refs: [],
      private_refs_allowed: private_allowed?(acl)
    })
  end

  def for_story(store, normalized, role_config, user_id) do
    output_visibility = normalized.acl["privacy"] || "public"

    visible_stories =
      store.stories
      |> Enum.filter(fn {_key, story} ->
        story_visible?(story, store, user_id) and
          (output_visibility == "private" or public_story?(story, store))
      end)
      |> Enum.map(fn {key, story} ->
        %{
          story_key: key,
          title: story.title,
          structural_facts: story.structural_facts,
          background_facts: story.background_facts,
          colors: story.colors,
          questions: story.questions,
          version: story.version,
          state: story.state,
          input_refs: Enum.filter(story.inputs, &input_visible?(store.nodes[&1], user_id))
        }
      end)

    packet(%{
      role: role_config.role,
      output_visibility: output_visibility,
      source_scope: normalized.source_type,
      evidence_refs: [normalized.fixture_id],
      snippets: [snippet(normalized.body_text)],
      input: normalized,
      visible_story_refs: Enum.map(visible_stories, & &1.story_key),
      stories: visible_stories,
      private_refs_allowed: private_allowed?(normalized.acl)
    })
  end

  def for_authoring(store, seen_state, role_config, user_id) do
    public_refs =
      store.nodes
      |> Map.values()
      |> Enum.filter(&input_visible?(&1, user_id))
      |> Enum.map(& &1.id)
      |> Enum.sort()

    packet(%{
      role: role_config.role,
      output_visibility: "private",
      source_scope: "flynn-relative-private-brain",
      evidence_refs: public_refs,
      snippets: [],
      seen_story_keys: Map.keys(seen_state.stories),
      private_refs_allowed: true
    })
  end

  def for_follow_on(store, activation, role_config, user_id) do
    story_key = activation.packet.story_key
    story = store.stories[story_key]

    packet(%{
      role: role_config.role,
      output_visibility: activation.packet.output_visibility,
      source_scope: "committed-soup-mutation",
      evidence_refs: activation.evidence_refs,
      snippets: [],
      story_key: story_key,
      story_state: story && story.state,
      visible_story_refs:
        if(story && story_visible?(story, store, user_id), do: [story_key], else: []),
      trigger_packet_hash: activation.packet_hash,
      private_refs_allowed: activation.packet.output_visibility == "private"
    })
  end

  def hash(packet) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(packet))
    |> Base.encode16(case: :lower)
  end

  defp packet(attrs) do
    attrs
    |> Map.put(:packet_contract, :acl_scoped_soup_packet)
    |> Map.put(:traversal_depth, 1)
    |> Map.put(:raw_database_access, false)
  end

  defp snippet(text), do: text |> String.slice(0, 280)

  defp private_allowed?(acl), do: acl["privacy"] == "private"

  defp story_visible?(story, store, user_id),
    do: Enum.any?(story.inputs, &input_visible?(store.nodes[&1], user_id))

  defp public_story?(story, store) do
    Enum.all?(story.inputs, fn input_id ->
      get_in(store.nodes, [input_id, :attrs, :acl, "privacy"]) == "public"
    end)
  end

  defp input_visible?(nil, _user_id), do: false

  defp input_visible?(node, user_id) do
    acl = get_in(node, [:attrs, :acl]) || %{"privacy" => "public"}
    acl["privacy"] == "public" or user_id in (acl["participants"] || [])
  end
end
