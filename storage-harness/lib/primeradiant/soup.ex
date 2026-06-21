defmodule Primeradiant.Soup do
  @moduledoc false

  @contract_version "soup.v1"
  @retained_event_window 100

  alias Primeradiant.StorageHarness.State

  def ready(%State{} = state, params \\ %{}) do
    generated_at = now()
    blockers = blockers(state, params)
    freshness = freshness(state, generated_at)
    status = if blockers == [], do: freshness_status(freshness), else: "blocked"

    %{
      contract_version: @contract_version,
      status: status,
      generated_at: generated_at,
      substrate_cursor: cursor_for(state),
      substrate_epoch: epoch(state),
      freshness: freshness,
      blockers: blockers
    }
  end

  def feed(%State{} = state, params \\ %{}) do
    blockers = blockers(state, params)
    limit = parse_limit(params["limit"] || params[:limit], 50)

    items =
      if blockers == [] do
        state |> visible_stories() |> Enum.map(&item(state, &1)) |> Enum.take(limit)
      else
        []
      end

    %{
      contract_version: @contract_version,
      substrate_cursor: cursor_for(state),
      substrate_epoch: epoch(state),
      items: items,
      blockers: blockers
    }
  end

  def delta(%State{} = state, params) do
    after_cursor = params["after"] || params[:after]
    blockers = blockers(state, params)

    with [] <- blockers,
         {:ok, requested} <- decode_cursor(after_cursor),
         :ok <- cursor_epoch_ok(state, requested, after_cursor),
         :ok <- cursor_known(state, requested, after_cursor) do
      limit = parse_limit(params["limit"] || params[:limit], 100)
      delta_projection(state, requested, limit)
    else
      [_ | _] ->
        %{
          contract_version: @contract_version,
          items: [],
          next_cursor: cursor_for(state),
          gap: nil,
          blockers: blockers
        }

      {:gap, gap} ->
        %{
          contract_version: @contract_version,
          items: [],
          next_cursor: cursor_for(state),
          gap: gap
        }
    end
  end

  def ack(%State{}, body, opts \\ []) when is_map(body) do
    response = %{
      contract_version: @contract_version,
      ack: %{
        consumer: body["consumer"] || body[:consumer],
        projection: body["projection"] || body[:projection],
        substrate_cursor: body["substrate_cursor"] || body[:substrate_cursor],
        substrate_epoch: body["substrate_epoch"] || body[:substrate_epoch],
        rendered_at: body["rendered_at"] || body[:rendered_at],
        projection_id: body["projection_id"] || body[:projection_id],
        status: body["status"] || body[:status],
        reason: body["reason"] || body[:reason],
        recorded_at: now()
      }
    }

    record_ack(response.ack, opts[:ack_log_path])
    response
  end

  def cursor_for(%State{} = state, event_index \\ nil) do
    index = event_index || length(ordered_card_changes(state))
    payload = %{"v" => 1, "epoch" => epoch(state), "event_index" => index}

    payload
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  def epoch(%State{} = state) do
    :crypto.hash(:sha256, "#{state.tenant_id}:soup:v1")
    |> Base.url_encode64(padding: false)
  end

  defp blockers(state, params) do
    cond do
      (params["consumer"] || params[:consumer]) != "reporter" ->
        [%{code: "unsupported_consumer", message: "consumer must be reporter"}]

      not story_card_projection?(params["projection"] || params[:projection]) ->
        [
          %{
            code: "unsupported_projection",
            message: "projection must be story_cards or news-morning"
          }
        ]

      state.stories == [] ->
        [
          %{
            code: "no_admitted_story_material",
            message: "no admitted story material is available"
          }
        ]

      true ->
        []
    end
  end

  defp story_card_projection?(projection), do: projection in ["story_cards", "news-morning"]

  defp freshness(state, generated_at) do
    latest_source =
      state.inputs |> Enum.map(& &1.observed_at) |> Enum.max(DateTime, fn -> nil end)

    latest_event =
      state.story_events |> Enum.map(& &1.observed_at) |> Enum.max(DateTime, fn -> nil end)

    latest = latest_event || latest_source
    age = if latest, do: DateTime.diff(parse_time!(generated_at), latest), else: nil

    %{
      latest_source_at: iso(latest_source),
      latest_story_event_at: iso(latest_event),
      max_age_seconds: age,
      is_stale: is_integer(age) and age > 86_400
    }
  end

  defp freshness_status(%{is_stale: true}), do: "degraded"
  defp freshness_status(_), do: "ready"

  defp delta_projection(state, requested, limit) do
    changes = ordered_card_changes(state)
    next_changes = changes |> Enum.drop(requested["event_index"]) |> Enum.take(limit)
    story_ids = next_changes |> Enum.map(& &1.story_id) |> MapSet.new()

    items =
      state
      |> visible_stories()
      |> Enum.filter(&MapSet.member?(story_ids, &1.id))
      |> Enum.map(&item(state, &1))

    %{
      contract_version: @contract_version,
      items: items,
      next_cursor: cursor_for(state, requested["event_index"] + length(next_changes)),
      gap: nil
    }
  end

  defp item(state, story) do
    case current_story_card_version(state, story.id) do
      nil -> incomplete_story_card_item(state, story)
      card -> story_card_item(state, story, card)
    end
  end

  defp story_card_item(state, story, card) do
    coverage = source_coverage_for(state, card.id)
    claims = key_claims_for(state, card.id)
    change_set = change_set_for(state, card.id)
    reader_delta = reader_delta_for(state, card.id)

    %{
      story_id: story.id,
      story_version: story.version,
      story_card_version_id: card.id,
      card_version: card.card_version,
      change_kind: if(change_set, do: change_set.refresh_reason, else: card.refresh_reason),
      section: "story_card",
      title: card.title,
      deck: card.deck,
      summary: card.summary,
      key_claims: Enum.map(claims, &claim_projection/1),
      source_coverage: Enum.map(coverage, &coverage_projection/1),
      source_links: Enum.map(coverage, &source_link_projection/1),
      provenance:
        Map.merge(card.provenance || %{}, %{
          "projection_id" => projection_id(state),
          "soup_cursor" => cursor_for(state),
          "story_card_version_id" => card.id,
          "producing_agent_run_ids" => [card.producing_agent_run_id]
        }),
      freshness: card.freshness,
      status: card.status,
      refresh_reason: card.refresh_reason,
      timestamps: %{
        story_first_seen_at: iso(story.first_observed_at),
        story_updated_at: iso(story.updated_at_story),
        card_created_at: iso(card.inserted_at)
      },
      field_completeness: card.field_completeness,
      changed_since_seen: changed_since_seen_projection(reader_delta),
      topic_salience: card.topic_salience,
      ranking: %{
        score: ranking_score(story, state.story_events),
        reasons: ["story_card_version", "source_breadth", "material_recency"]
      },
      change_set:
        change_set &&
          %{
            changed_field_keys: change_set.changed_field_keys,
            added_claim_refs: change_set.added_claim_refs,
            removed_claim_refs: change_set.removed_claim_refs,
            changed_claim_refs: change_set.changed_claim_refs,
            changed_source_coverage_refs: change_set.changed_source_coverage_refs,
            refresh_reason: change_set.refresh_reason,
            change_summary: change_set.change_summary
          }
    }
  end

  defp incomplete_story_card_item(state, story) do
    provenance_refs = ["story-card:unavailable:#{story.id}"]

    %{
      story_id: story.id,
      story_version: story.version,
      story_card_version_id: nil,
      card_version: nil,
      change_kind: "story_card_unavailable",
      section: "story_card",
      title: unavailable_text_field("story_card_not_synthesized", provenance_refs),
      deck: unavailable_text_field("story_card_not_synthesized", provenance_refs),
      summary: unavailable_text_field("story_card_not_synthesized", provenance_refs),
      key_claims: [],
      source_coverage: [],
      source_links: [],
      provenance: %{
        "projection_id" => projection_id(state),
        "soup_cursor" => cursor_for(state),
        "story_card_version_id" => nil,
        "producing_agent_run_ids" => [],
        "state" => "incomplete",
        "reason" => "story_card_not_synthesized"
      },
      freshness: freshness_for(story),
      status: "incomplete",
      refresh_reason: "story_card_not_synthesized",
      timestamps: %{
        story_first_seen_at: iso(story.first_observed_at),
        story_updated_at: iso(story.updated_at_story),
        card_created_at: nil
      },
      field_completeness: %{
        "title" => "unavailable",
        "deck" => "unavailable",
        "summary" => "unavailable",
        "key_claims" => "unavailable",
        "overall" => "incomplete"
      },
      changed_since_seen: changed_since_seen_projection(nil),
      topic_salience: %{
        "state" => "unavailable",
        "reason" => "story_card_not_synthesized"
      },
      ranking: %{
        score: ranking_score(story, state.story_events),
        reasons: ["story_card_unavailable"]
      },
      change_set: nil
    }
  end

  defp unavailable_text_field(reason, provenance_refs) do
    %{
      "text" => nil,
      "state" => "unavailable",
      "reason" => reason,
      "provenance_refs" => provenance_refs
    }
  end

  defp current_story_card_version(state, story_id) do
    state.story_card_versions
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.sort_by(& &1.card_version, :desc)
    |> List.first()
  end

  defp source_coverage_for(state, card_id),
    do: Enum.filter(state.story_source_coverage, &(&1.story_card_version_id == card_id))

  defp key_claims_for(state, card_id),
    do: Enum.filter(state.story_key_claims, &(&1.story_card_version_id == card_id))

  defp change_set_for(state, card_id),
    do: Enum.find(state.story_card_change_sets, &(&1.new_card_version_id == card_id))

  defp reader_delta_for(state, card_id),
    do: Enum.find(state.story_reader_deltas, &(&1.current_card_version_id == card_id))

  defp claim_projection(claim) do
    %{
      claim_ref: claim.claim_ref,
      text: claim.text,
      status: claim.status,
      materiality: claim.materiality,
      evidence_refs: claim.evidence_refs,
      conflict_refs: claim.conflict_refs,
      uncertainty: claim.uncertainty,
      appears_in_current_card: claim.appears_in_current_card
    }
  end

  defp coverage_projection(row) do
    %{
      source_ref: row.source_ref,
      article_ref: row.article_ref,
      canonical_public_url: row.canonical_public_url,
      source_domain: row.source_domain,
      source_label: row.source_label,
      publication: row.publication,
      source_posture: row.source_posture,
      contribution_reason: row.contribution_reason,
      materiality: row.materiality,
      source_weight: row.source_weight,
      first_observed_at: iso(row.first_observed_at),
      last_observed_at: iso(row.last_observed_at),
      evidence_refs: row.evidence_refs,
      provenance_refs: row.provenance_refs
    }
  end

  defp source_link_projection(row) do
    %{
      source_ref: row.source_ref,
      article_ref: row.article_ref,
      canonical_public_url: row.canonical_public_url,
      source_domain: row.source_domain,
      source_label: row.source_label,
      publication: row.publication,
      contribution_reason: row.contribution_reason,
      evidence_refs: row.evidence_refs
    }
  end

  defp changed_since_seen_projection(nil) do
    %{
      state: "unavailable",
      reason: "reader_delta_not_requested",
      material_unseen_deltas: [],
      nonmaterial_exclusions: []
    }
  end

  defp changed_since_seen_projection(delta) do
    %{
      state: "complete",
      user_id: delta.user_id,
      seen_state_id: delta.seen_state_id,
      prior_seen_story_version: delta.prior_seen_story_version,
      prior_seen_card_version_id: delta.prior_seen_card_version_id,
      current_story_version: delta.current_story_version,
      current_card_version_id: delta.current_card_version_id,
      material_unseen_deltas: delta.material_unseen_deltas,
      nonmaterial_exclusions: delta.nonmaterial_exclusions,
      producing_agent_run_id: delta.producing_agent_run_id,
      evidence_refs: delta.evidence_refs,
      provenance_refs: delta.provenance_refs
    }
  end

  defp projection_id(state), do: "story-cards:#{state.tenant_id}:#{epoch(state)}"

  defp visible_stories(state),
    do: Enum.sort_by(state.stories, & &1.updated_at_story, {:desc, DateTime})

  defp freshness_for(story) do
    age = DateTime.diff(parse_time!(now()), story.updated_at_story)
    %{state: story.state, age_seconds: age}
  end

  defp ranking_score(story, events), do: story.version + length(events)

  defp ordered_card_changes(state),
    do: Enum.sort_by(state.story_card_change_sets, &{iso(&1.inserted_at) || "", &1.id})

  defp decode_cursor(nil), do: {:gap, cursor_gap("cursor_unknown", nil)}

  defp decode_cursor(cursor) do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false),
         {:ok, %{"v" => 1, "epoch" => epoch, "event_index" => index} = decoded} <-
           Jason.decode(json),
         true <- is_binary(epoch),
         true <- is_integer(index) do
      {:ok, decoded}
    else
      _ -> {:gap, cursor_gap("cursor_unknown", cursor)}
    end
  end

  defp cursor_epoch_ok(state, %{"epoch" => cursor_epoch, "event_index" => _index}, raw_cursor) do
    if cursor_epoch == epoch(state),
      do: :ok,
      else: {:gap, cursor_gap("epoch_rotated", raw_cursor)}
  end

  defp cursor_known(state, %{"event_index" => index}, raw_cursor) do
    event_count = length(ordered_card_changes(state))
    retained_floor = max(event_count - @retained_event_window, 0)

    cond do
      index < retained_floor -> {:gap, cursor_gap("cursor_expired", raw_cursor)}
      index > event_count -> {:gap, cursor_gap("cursor_unknown", raw_cursor)}
      true -> :ok
    end
  end

  defp cursor_gap(code, requested) do
    %{
      code: code,
      requested_cursor: requested,
      recovery: "full_feed_required",
      message: "requested cursor cannot be served without a full feed recovery"
    }
  end

  defp parse_limit(nil, default), do: default
  defp parse_limit(limit, _default) when is_integer(limit), do: limit
  defp parse_limit(limit, default), do: String.to_integer(to_string(limit || default))

  defp record_ack(ack, path) do
    path |> Path.dirname() |> File.mkdir_p!()
    File.write!(path, Jason.encode!(ack) <> "\n", [:append])
  end

  defp iso(nil), do: nil
  defp iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  defp parse_time!(value), do: value |> DateTime.from_iso8601() |> elem(1)
end
