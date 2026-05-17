defmodule Primeradiant.Authoring.Briefing do
  @moduledoc false

  alias Primeradiant.UserContext.SeenState

  def render(store, seen_state, user_id) do
    stories =
      store.stories
      |> Map.values()
      |> Enum.filter(&story_visible_to_user?(&1, store, user_id))
      |> Enum.filter(&story_changed_for_user?(&1, seen_state, store, user_id))
      |> Enum.sort_by(&story_rank/1, :desc)

    packet = evidence_packet(store, stories, user_id)
    bullet_records = Enum.map(stories, &story_bullet(&1, seen_state, packet, store, user_id))
    bullet_points = Enum.map(bullet_records, & &1.text)
    output_id = "authored-output:" <> Integer.to_string(length(store.commits))

    text =
      case bullet_points do
        [] -> ""
        _ -> "what changed for Flynn since last seen, and why\n" <> Enum.join(bullet_points, "\n")
      end

    %{
      output_id: output_id,
      user_id: user_id,
      bullets: bullet_points,
      text: text,
      evidence_packet: packet,
      sentence_evidence: bullet_records,
      evidence_refs: packet.evidence_refs,
      touched_story_keys: Enum.map(stories, & &1.story_key),
      verified: verify_output(bullet_records, packet)
    }
  end

  def record_output(store, authored_output) do
    if authored_output.verified != true,
      do: raise(ArgumentError, "cannot record unverified output")

    put_in(store, [:nodes, authored_output.output_id], %{
      id: authored_output.output_id,
      node_type: :authored_output,
      title: authored_output.output_id,
      state: :active,
      attrs: %{user_id: authored_output.user_id, text: authored_output.text},
      evidence_refs: authored_output.evidence_refs
    })
  end

  def mark_seen(seen_state, store, authored_output) do
    if authored_output.verified != true,
      do: raise(ArgumentError, "cannot mark seen for unverified output")

    Enum.reduce(authored_output.touched_story_keys, seen_state, fn story_key, acc ->
      SeenState.mark_seen(acc, store.stories[story_key], authored_output)
    end)
  end

  defp story_changed_for_user?(story, seen_state, store, user_id) do
    seen_inputs = SeenState.seen_input_refs(seen_state, story.story_key) |> MapSet.new()
    seen_version = SeenState.last_seen_version(seen_state, story.story_key)

    story.history
    |> Enum.filter(&input_visible_to_user?(store.nodes[&1.input_id], user_id))
    |> Enum.any?(fn event ->
      not MapSet.member?(seen_inputs, event.input_id) and event.story_version > seen_version and
        (seen_version == 0 or material_visible_event?(event))
    end)
  end

  defp material_visible_event?(event) do
    case event do
      %{classification: classification, changed_facts: changed_facts}
      when classification in [:substantive_update, :conflict_correction] and
             map_size(changed_facts) > 0 ->
        true

      _ ->
        false
    end
  end

  defp story_rank(story) do
    base = story.version * 10 + map_size(story.structural_facts) * 5
    if story.watch_ids != [], do: base + 100, else: base
  end

  defp story_bullet(story, seen_state, packet, store, user_id) do
    last_seen = SeenState.last_seen_version(seen_state, story.story_key)
    latest_event = latest_visible_event(story, store, user_id) || %{classification: :new_story}
    why = why_text(story, latest_event.classification, last_seen)

    fact_pairs =
      story
      |> visible_fact_pairs(store, user_id, latest_event, last_seen)

    facts =
      fact_pairs
      |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
      |> Enum.join(", ")

    evidence_refs = packet.story_evidence[story.story_key] || []
    evidence = evidence_refs |> Enum.take(3) |> Enum.join(", ")
    prefix = if story.watch_ids != [], do: "[watch] ", else: ""

    %{
      text:
        "#{prefix}#{story.title}: #{why}. Evidence: #{evidence}#{if facts != "", do: " (#{facts})", else: ""}",
      evidence_refs: evidence_refs,
      claim_refs: claim_refs_for_fact_pairs(story, fact_pairs, store)
    }
  end

  defp evidence_packet(store, stories, user_id) do
    story_evidence =
      Map.new(stories, fn story ->
        refs =
          story.inputs
          |> Enum.reverse()
          |> Enum.filter(&input_visible_to_user?(store.nodes[&1], user_id))
          |> Enum.take(3)

        {story.story_key, refs}
      end)

    %{
      user_id: user_id,
      story_evidence: story_evidence,
      evidence_refs: story_evidence |> Map.values() |> List.flatten() |> Enum.uniq()
    }
  end

  defp verify_output(bullet_records, packet) do
    packet_refs = MapSet.new(packet.evidence_refs)

    Enum.all?(bullet_records, fn bullet ->
      bullet.evidence_refs != [] and bullet.claim_refs != [] and
        Enum.all?(bullet.evidence_refs, &MapSet.member?(packet_refs, &1))
    end)
  end

  defp visible_structural_facts(story, store, user_id) do
    story.structural_facts
    |> Enum.filter(fn {key, _value} ->
      refs = get_in(story.fact_provenance, [key, :evidence_refs]) || []
      refs != [] and Enum.any?(refs, &input_visible_to_user?(store.nodes[&1], user_id))
    end)
    |> Map.new()
  end

  defp visible_fact_pairs(story, store, user_id, latest_event, last_seen) do
    visible_facts = visible_structural_facts(story, store, user_id)

    if last_seen > 0 and map_size(Map.get(latest_event, :changed_facts, %{})) > 0 do
      latest_event.changed_facts
      |> Map.take(Map.keys(visible_facts))
    else
      visible_facts
    end
    |> Enum.sort()
    |> Enum.take(3)
  end

  defp claim_refs_for_fact_pairs(story, fact_pairs, store) do
    fact_pairs
    |> Enum.map(fn {key, _value} -> key end)
    |> Enum.sort()
    |> Enum.map(&"claim:#{story.story_key}:#{&1}")
    |> Enum.filter(&Map.has_key?(store.nodes, &1))
  end

  defp latest_visible_event(story, store, user_id) do
    story.history
    |> Enum.reverse()
    |> Enum.find(fn event -> input_visible_to_user?(store.nodes[event.input_id], user_id) end)
  end

  defp story_visible_to_user?(story, store, user_id),
    do: Enum.any?(story.inputs, &input_visible_to_user?(store.nodes[&1], user_id))

  defp input_visible_to_user?(nil, _user_id), do: false

  defp input_visible_to_user?(node, user_id) do
    acl = get_in(node, [:attrs, :acl]) || %{"privacy" => "public"}
    acl["privacy"] == "public" or user_id in (acl["participants"] || [])
  end

  defp why_text(_story, :new_story, _last_seen), do: "new to Flynn since last seen"

  defp why_text(_story, :substantive_update, _last_seen),
    do: "changed for Flynn since last seen because structural facts moved"

  defp why_text(_story, :conflict_correction, _last_seen),
    do: "changed for Flynn since last seen because prior state was corrected"

  defp why_text(_story, :color_spin_without_structural_change, 0),
    do: "new to Flynn and notable mainly for framing"

  defp why_text(_story, :color_spin_without_structural_change, _last_seen),
    do: "did not structurally change, but the framing shifted"

  defp why_text(_story, :stale_background_state, 0), do: "new to Flynn only as background context"

  defp why_text(_story, :stale_background_state, _last_seen),
    do: "mostly background since last seen"

  defp why_text(_story, :attach_to_existing_story, 0),
    do: "new to Flynn as another connected input"

  defp why_text(_story, :attach_to_existing_story, _last_seen),
    do: "same story, with another connected input"

  defp why_text(_story, _classification, 0), do: "new to Flynn since last seen"
  defp why_text(_story, _classification, _last_seen), do: "still worth noting for Flynn"
end
