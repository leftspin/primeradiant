defmodule Primeradiant.StorySubstrate do
  @moduledoc false

  @contract_version "story_substrate.v1"
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
      events = ordered_events(state)
      limit = parse_limit(params["limit"] || params[:limit], 100)
      next_events = events |> Enum.drop(requested["event_index"]) |> Enum.take(limit)
      story_ids = next_events |> Enum.map(& &1.story_id) |> MapSet.new()

      items =
        state
        |> visible_stories()
        |> Enum.filter(&MapSet.member?(story_ids, &1.id))
        |> Enum.map(&item(state, &1))

      %{
        contract_version: @contract_version,
        items: items,
        next_cursor: cursor_for(state, requested["event_index"] + length(next_events)),
        gap: nil
      }
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
    index = event_index || length(ordered_events(state))
    payload = %{"v" => 1, "epoch" => epoch(state), "event_index" => index}

    payload
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  def epoch(%State{} = state) do
    :crypto.hash(:sha256, "#{state.tenant_id}:story-substrate:v1")
    |> Base.url_encode64(padding: false)
  end

  defp blockers(state, params) do
    cond do
      (params["consumer"] || params[:consumer]) != "reporter" ->
        [%{code: "unsupported_consumer", message: "consumer must be reporter"}]

      (params["projection"] || params[:projection]) != "news-morning" ->
        [%{code: "unsupported_projection", message: "projection must be news-morning"}]

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

  defp item(state, story) do
    events = state.story_events |> Enum.filter(&(&1.story_id == story.id))
    inputs = story_inputs(state, story.id)
    evidence_refs = evidence_refs(state, story.id)
    latest_event = List.last(Enum.sort_by(events, &event_sort_key/1))
    score = confidence_score(state, story.id)

    %{
      story_id: story.id,
      admitted_item_ids: Enum.map(inputs, & &1.id),
      title: %{text: story.title, source: "primeradiant_story", evidence_refs: evidence_refs},
      summary: %{
        text: summary_text(story),
        source: "primeradiant_facts",
        evidence_refs: evidence_refs
      },
      timestamps: %{
        first_seen_at: iso(story.first_observed_at),
        last_source_at: iso(max_time(Enum.map(inputs, & &1.observed_at))),
        last_material_change_at: iso(story.last_material_at),
        last_agent_review_at: iso(max_time(Enum.map(state.agent_runs, & &1.ended_at)))
      },
      provenance: Enum.map(inputs, &provenance_for(state, &1, evidence_refs)),
      confidence: %{
        score: score,
        label: confidence_label(score),
        reasons: ["accepted_pr_proposal_confidence"]
      },
      freshness: freshness_for(story),
      change: %{
        kind: if(latest_event, do: latest_event.classification, else: "new_story"),
        signals: change_signals(latest_event),
        evidence_refs: evidence_refs
      },
      ranking: %{
        score: ranking_score(story, events),
        reasons: ["story_version", "material_recency"]
      }
    }
  end

  defp visible_stories(state),
    do: Enum.sort_by(state.stories, & &1.updated_at_story, {:desc, DateTime})

  defp story_inputs(state, story_id) do
    input_ids =
      state.story_events
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(& &1.input_id)
      |> MapSet.new()

    Enum.filter(state.inputs, &MapSet.member?(input_ids, &1.id))
  end

  defp evidence_refs(state, story_id) do
    subject_ids =
      (state.story_events ++ state.story_fact_versions)
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    state.evidence_refs
    |> Enum.filter(&MapSet.member?(subject_ids, &1.subject_id))
    |> Enum.map(& &1.evidence_label)
    |> Enum.uniq()
  end

  defp provenance_for(_state, input, refs) do
    uri = input.object_uri || ""
    host = if uri == "", do: input.source_type, else: URI.parse(uri).host

    %{
      source_domain: host,
      url: input.object_uri,
      published_at: iso(input.observed_at),
      admitted_at: iso(input.inserted_at),
      evidence_refs: refs
    }
  end

  defp summary_text(story) do
    story.structural_facts
    |> Enum.sort()
    |> Enum.map_join(", ", fn {key, value} -> "#{key}=#{value}" end)
  end

  defp confidence_score(state, story_id) do
    scores =
      state.proposals
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(&Decimal.to_float(&1.confidence))

    if scores == [], do: 0.0, else: Enum.sum(scores) / length(scores)
  end

  defp confidence_label(score) when score >= 0.8, do: "high"
  defp confidence_label(score) when score >= 0.5, do: "medium"
  defp confidence_label(_), do: "low"

  defp freshness_for(story) do
    age = DateTime.diff(parse_time!(now()), story.updated_at_story)
    %{state: story.state, age_seconds: age}
  end

  defp change_signals(nil), do: []
  defp change_signals(event), do: event.changed_facts |> Map.keys() |> Enum.sort()

  defp ranking_score(story, events), do: story.version + length(events)

  defp ordered_events(state), do: Enum.sort_by(state.story_events, &event_sort_key/1)

  defp event_sort_key(event), do: {DateTime.to_iso8601(event.observed_at), event.id}

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
    event_count = length(ordered_events(state))
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

  defp max_time([]), do: nil
  defp max_time(times), do: times |> Enum.reject(&is_nil/1) |> Enum.max(DateTime, fn -> nil end)

  defp iso(nil), do: nil
  defp iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  defp parse_time!(value), do: value |> DateTime.from_iso8601() |> elem(1)
end
