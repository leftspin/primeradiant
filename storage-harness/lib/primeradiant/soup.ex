defmodule Primeradiant.Soup do
  @moduledoc false

  @contract_version "soup.v1"
  @retained_event_window 100

  alias Primeradiant.StorageHarness.State

  def ready(%State{} = state, params \\ %{}) do
    state
    |> ready_facts()
    |> ready_from_facts(params)
  end

  def ready_from_facts(facts, params \\ %{}) when is_map(facts) do
    generated_at = now()
    blockers = readiness_blockers(facts.story_count, params)
    freshness = freshness(facts, generated_at)
    synthesis_health = synthesis_health(facts)
    health_blockers = synthesis_health_blockers(synthesis_health)
    all_blockers = if blockers == [], do: health_blockers, else: blockers
    status = if all_blockers == [], do: freshness_status(freshness), else: "blocked"

    %{
      contract_version: @contract_version,
      status: status,
      generated_at: generated_at,
      substrate_cursor: cursor_for_count(facts.tenant_id, facts.change_count),
      substrate_epoch: epoch_for_tenant(facts.tenant_id),
      freshness: freshness,
      synthesis_health: synthesis_health,
      blockers: all_blockers
    }
  end

  def feed(%State{} = state, params \\ %{}) do
    blockers = blockers(state, params)
    limit = parse_limit(params["limit"] || params[:limit], 50)

    items =
      if blockers == [] do
        state |> feed_visible_stories() |> Enum.map(&item(state, &1)) |> Enum.take(limit)
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
    epoch_for_tenant(state.tenant_id)
  end

  defp epoch_for_tenant(tenant_id) do
    :crypto.hash(:sha256, "#{tenant_id}:soup:v1")
    |> Base.url_encode64(padding: false)
  end

  defp cursor_for_count(tenant_id, event_count) do
    %{"v" => 1, "epoch" => epoch_for_tenant(tenant_id), "event_index" => event_count}
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp blockers(state, params) do
    readiness_blockers(length(state.stories), params)
  end

  defp readiness_blockers(story_count, params) do
    cond do
      (params["consumer"] || params[:consumer]) != "reporter" ->
        [%{code: "unsupported_consumer", message: "consumer must be reporter"}]

      (params["projection"] || params[:projection]) not in ["story_cards", "news-morning"] ->
        [
          %{
            code: "unsupported_projection",
            message: "projection must be story_cards or news-morning"
          }
        ]

      story_count == 0 ->
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

  defp freshness(
         %{latest_source_at: latest_source, latest_story_event_at: latest_event},
         generated_at
       ) do
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

  defp synthesis_health(facts) do
    synthesis_health(
      facts.visible_story_count,
      facts.complete_current_synopsis_count,
      facts.failure_streak_count,
      facts.failure_reasons
    )
  end

  defp synthesis_health(
         story_count,
         complete_current_count,
         failure_streak_count,
         failure_reasons
       ) do
    failures =
      []
      |> maybe_add_zero_complete_synopsis_failure(story_count, complete_current_count)
      |> maybe_add_synthesis_failure_streak(failure_streak_count, failure_reasons)

    %{
      status: if(failures == [], do: "healthy", else: "failed"),
      checked_story_count: story_count,
      complete_current_synopsis_count: complete_current_count,
      current_synopsis_artifact: %{
        required: true,
        status: if(complete_current_count > 0, do: "present", else: "missing")
      },
      refused_or_invalid_streak_count: failure_streak_count,
      refused_or_invalid_streak_threshold: 3,
      failures: failures
    }
  end

  defp maybe_add_zero_complete_synopsis_failure(failures, 0, _count), do: failures

  defp maybe_add_zero_complete_synopsis_failure(failures, _story_count, count) when count > 0,
    do: failures

  defp maybe_add_zero_complete_synopsis_failure(failures, story_count, 0) do
    [
      %{
        code: "zero_complete_current_synopsis_artifacts",
        message:
          "Prime Radiant has admitted current story material but no complete current grounded synopsis artifacts",
        story_count: story_count
      }
      | failures
    ]
  end

  defp maybe_add_synthesis_failure_streak(failures, failure_streak_count, _failure_reasons)
       when failure_streak_count < 3,
       do: failures

  defp maybe_add_synthesis_failure_streak(failures, failure_streak_count, failure_reasons) do
    [
      %{
        code: "story_synthesis_refused_or_invalid_streak",
        message:
          "Prime Radiant story synthesis has repeatedly refused or quarantined invalid current synopsis output",
        streak_count: failure_streak_count,
        latest_reasons: failure_reasons
      }
      | failures
    ]
  end

  defp synthesis_health_blockers(%{failures: []}), do: []

  defp synthesis_health_blockers(%{failures: failures}) do
    Enum.map(failures, fn failure ->
      %{
        code: failure.code,
        message: failure.message,
        health_surface: "synthesis_health"
      }
    end)
  end

  defp current_synthesis_failure_streak(state) do
    state.story_card_versions
    |> Enum.filter(&(&1.refresh_reason == "story_card_hourly_synthesis"))
    |> Enum.sort_by(&{time_sort_value(&1.inserted_at), &1.card_version}, :desc)
    |> Enum.reduce_while([], fn card, streak ->
      if synthesis_failure_card?(card) do
        {:cont,
         [%{story_card_version_id: card.id, reason: synthesis_failure_reason(card)} | streak]}
      else
        {:halt, streak}
      end
    end)
  end

  def ready_facts(%State{} = state) do
    visible = visible_stories(state)
    active_cards = active_repair_rows(state, state.story_card_versions)

    current_cards_by_story =
      active_cards
      |> Enum.group_by(& &1.story_id)
      |> Map.new(fn {story_id, cards} ->
        {story_id, Enum.max_by(cards, & &1.card_version)}
      end)

    failure_streak = current_synthesis_failure_streak(state)

    %{
      tenant_id: state.tenant_id,
      story_count: length(state.stories),
      visible_story_count: length(visible),
      complete_current_synopsis_count:
        Enum.count(visible, fn story ->
          complete_story_card?(Map.get(current_cards_by_story, story.id))
        end),
      failure_streak_count: length(failure_streak),
      failure_reasons: failure_streak |> Enum.map(& &1.reason) |> Enum.uniq(),
      latest_source_at:
        state.inputs |> Enum.map(& &1.observed_at) |> Enum.max(DateTime, fn -> nil end),
      latest_story_event_at:
        state.story_events |> Enum.map(& &1.observed_at) |> Enum.max(DateTime, fn -> nil end),
      change_count: length(ordered_card_changes(state))
    }
  end

  defp synthesis_failure_card?(%{status: "refused"}), do: true

  defp synthesis_failure_card?(card) when is_map(card) do
    synthesis_failure_reason(card) in [
      "story_synthesis_invalid_model_output",
      "story_synthesis_malformed_model_output"
    ]
  end

  defp synthesis_failure_card?(_), do: false

  defp synthesis_failure_reason(card) do
    card.provenance["reason"] ||
      card.provenance[:reason] ||
      card.deck["reason"] ||
      card.deck[:reason] ||
      card.summary["reason"] ||
      card.summary[:reason] ||
      card.field_completeness["overall"] ||
      card.field_completeness[:overall] ||
      card.status
  end

  defp time_sort_value(nil), do: 0
  defp time_sort_value(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)
  defp time_sort_value(value) when is_binary(value), do: value
  defp time_sort_value(_), do: 0

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
      nil ->
        incomplete_story_card_item(state, story)

      card ->
        if complete_story_card?(card) do
          story_card_item(state, story, card)
        else
          incomplete_story_card_item(state, story, card)
        end
    end
  end

  defp story_card_item(state, story, card) do
    coverage = source_coverage_for(state, card.id)
    events = state.story_events |> Enum.filter(&(&1.story_id == story.id))
    inputs = story_inputs(state, story.id)
    evidence_refs = evidence_refs(state, story.id)
    magazine_sources = source_coverage(state, story, inputs, evidence_refs)
    complete_card? = complete_story_card?(card)
    claims = if complete_card?, do: key_claims_for(state, card.id), else: []
    coverage_projection_rows = if complete_card?, do: coverage, else: []

    source_links =
      if complete_card? do
        coverage
        |> Enum.filter(&reader_visible_source_link?/1)
        |> Enum.map(&source_link_projection/1)
      else
        []
      end

    change_set = change_set_for(state, card.id)
    reader_delta = reader_delta_for(state, card.id)

    %{
      story_id: story.id,
      story_version: story.version,
      story_card_version_id: card.id,
      card_version: card.card_version,
      change_kind: if(change_set, do: change_set.refresh_reason, else: card.refresh_reason),
      admitted_item_ids: Enum.map(inputs, & &1.id),
      section: "story_card",
      title: card.title,
      deck: card.deck,
      summary: card.summary,
      key_claims: Enum.map(claims, &claim_projection/1),
      source_coverage: Enum.map(coverage_projection_rows, &coverage_projection/1),
      source_links: source_links,
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
      projection_provenance:
        magazine_projection_provenance(state, story, inputs, card.id, magazine_sources),
      ranking: %{
        score: ranking_score(story, state.story_events),
        reasons: ["story_card_version", "source_breadth", "material_recency"],
        hints: ranking_hints(story, events, magazine_sources)
      },
      magazine_contract: magazine_contract(card.summary, card.deck, inputs, magazine_sources),
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

  defp incomplete_story_card_item(state, story, card \\ nil) do
    provenance_refs = ["story-card:unavailable:#{story.id}"]
    events = state.story_events |> Enum.filter(&(&1.story_id == story.id))
    inputs = story_inputs(state, story.id)
    evidence_refs = evidence_refs(state, story.id)
    magazine_sources = source_coverage(state, story, inputs, evidence_refs)
    reason = if card, do: "story_card_not_complete", else: "story_card_not_synthesized"
    summary = unavailable_text_field(reason, provenance_refs)
    deck = unavailable_text_field(reason, provenance_refs)

    %{
      story_id: story.id,
      story_version: story.version,
      story_card_version_id: card && card.id,
      card_version: card && card.card_version,
      change_kind: if(card, do: card.refresh_reason, else: "story_card_unavailable"),
      section: "story_card",
      title: unavailable_text_field(reason, provenance_refs),
      deck: deck,
      summary: summary,
      admitted_item_ids: Enum.map(inputs, & &1.id),
      key_claims: [],
      source_coverage: [],
      source_links: [],
      provenance:
        %{
          "projection_id" => projection_id(state),
          "soup_cursor" => cursor_for(state),
          "story_card_version_id" => card && card.id,
          "producing_agent_run_ids" => if(card, do: [card.producing_agent_run_id], else: []),
          "state" => "incomplete",
          "reason" => reason,
          "suppressed_story_card_status" => card && card.status
        }
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new(),
      projection_provenance:
        magazine_projection_provenance(state, story, inputs, card && card.id, magazine_sources),
      freshness: freshness_for(story),
      status: "incomplete",
      refresh_reason: reason,
      timestamps: %{
        story_first_seen_at: iso(story.first_observed_at),
        story_updated_at: iso(story.updated_at_story),
        card_created_at: card && iso(card.inserted_at)
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
        "reason" => reason
      },
      ranking: %{
        score: ranking_score(story, state.story_events),
        reasons: ["story_card_unavailable"],
        hints: ranking_hints(story, events, magazine_sources)
      },
      magazine_contract: magazine_contract(summary, deck, inputs, magazine_sources),
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
    state
    |> active_repair_rows(state.story_card_versions)
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

  defp reader_visible_source_link?(row),
    do: reader_visible_contribution_reason?(row.contribution_reason)

  defp complete_story_card?(%{status: "complete"}), do: true
  defp complete_story_card?(_), do: false

  defp reader_visible_contribution_reason?(%{"state" => "complete", "text" => text})
       when is_binary(text) and text != "",
       do: true

  defp reader_visible_contribution_reason?(%{state: "complete", text: text})
       when is_binary(text) and text != "",
       do: true

  defp reader_visible_contribution_reason?(%{"state" => state, "reason" => reason})
       when state in ["unavailable", "refused"] and is_binary(reason) and reason != "" and
              reason != "story_synthesis_agent_did_not_supply_field",
       do: not internal_synthesis_failure_reason?(reason)

  defp reader_visible_contribution_reason?(%{state: state, reason: reason})
       when state in ["unavailable", "refused"] and is_binary(reason) and reason != "" and
              reason != "story_synthesis_agent_did_not_supply_field",
       do: not internal_synthesis_failure_reason?(reason)

  defp reader_visible_contribution_reason?(_), do: false

  defp internal_synthesis_failure_reason?(
         "story_synthesis_agent_omitted_required_source_coverage_after_retry"
       ),
       do: true

  defp internal_synthesis_failure_reason?(_reason), do: false

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

  defp feed_visible_stories(state) do
    state.stories
    |> Enum.reject(&t1421_quarantined?/1)
    |> Enum.reject(&graph_admission_quarantined?(state, &1))
    |> Enum.sort_by(
      &{story_card_serving_rank(state, &1), &1.updated_at_story},
      fn {left_rank, left_updated_at}, {right_rank, right_updated_at} ->
        left_rank < right_rank or
          (left_rank == right_rank and DateTime.compare(left_updated_at, right_updated_at) != :lt)
      end
    )
  end

  defp visible_stories(state) do
    state.stories
    |> Enum.reject(&t1421_quarantined?/1)
    |> Enum.reject(&graph_admission_quarantined?(state, &1))
    |> Enum.sort_by(& &1.updated_at_story, {:desc, DateTime})
  end

  defp story_card_serving_rank(state, story) do
    case current_story_card_version(state, story.id) do
      %{status: "complete"} -> 0
      _ -> 1
    end
  end

  defp t1421_quarantined?(story) do
    case story.attrs["t1421_quarantine"] || story.attrs[:t1421_quarantine] do
      %{"active_product_truth" => false} -> true
      %{active_product_truth: false} -> true
      _ -> false
    end
  end

  defp graph_admission_quarantined?(state, story),
    do:
      Enum.any?(
        state.story_quarantines,
        &(&1.story_id == story.id and &1.rollback_status != "restored")
      )

  defp story_inputs(state, story_id) do
    input_ids =
      state
      |> active_repair_rows(state.story_events)
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(& &1.input_id)
      |> MapSet.new()

    Enum.filter(state.inputs, &MapSet.member?(input_ids, &1.id))
  end

  defp evidence_refs(state, story_id) do
    subject_ids =
      active_repair_rows(state, state.story_events ++ state.story_fact_versions)
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    state
    |> active_repair_rows(state.evidence_refs)
    |> Enum.filter(&MapSet.member?(subject_ids, &1.subject_id))
    |> Enum.map(& &1.evidence_label)
    |> Enum.uniq()
  end

  defp source_contract_for(state, story, input, refs) do
    canonical_uri = blank_to_nil(input.normalized["canonical_uri"])
    raw_object_uri = blank_to_nil(input.normalized["raw_object_uri"] || input.object_uri)
    public_uri = public_uri(canonical_uri)
    source_label = source_label(input)
    domain = public_uri && URI.parse(public_uri).host
    article_title = blank_to_nil(input.title)
    edge = article_story_edge(state, story, input)
    contribution = contribution_contract(edge)
    media = media_contract(input)

    %{
      input_id: input.id,
      admitted_item_id: input.id,
      source_ref: input_ref(input),
      article_ref: input.id,
      article_external_id: input.external_id,
      article_title: article_title,
      article_name: article_title,
      no_article_title_reason: if(article_title, do: nil, else: "no_article_title_committed"),
      source_domain: normalize_domain(domain),
      no_source_domain_reason: if(domain, do: nil, else: no_source_domain_reason(canonical_uri)),
      source_label: source_label,
      no_source_label_reason: if(source_label, do: nil, else: "no_source_label_committed"),
      publication: source_label,
      no_publication_reason: if(source_label, do: nil, else: "no_publication_committed"),
      canonical_public_url: public_uri,
      raw_object_uri: raw_object_uri,
      url: public_uri,
      url_kind: if(public_uri, do: "canonical_public_url", else: nil),
      link_status: if(public_uri, do: "public", else: "unavailable"),
      unavailable_reason:
        if(public_uri, do: nil, else: no_public_url_reason(canonical_uri, raw_object_uri)),
      summary_text: nil,
      deck_text: nil,
      no_summary_reason: "source_summary_not_committed_separately",
      favicon_url: media.favicon_url,
      no_favicon_reason: media.no_favicon_reason,
      image_url: media.image_url,
      no_image_reason: media.no_image_reason,
      contribution_summary: contribution.summary,
      contribution_type: contribution.type,
      contribution_reason: contribution.summary,
      contribution_link_basis: contribution.link_basis,
      contribution_salience: contribution.salience,
      no_contribution_summary_reason: contribution.no_summary_reason,
      edge_provenance: contribution.edge_provenance,
      evidence_refs: refs,
      provenance: %{
        source_type: input.source_type,
        external_id: input.external_id,
        content_sha256: input.content_sha256,
        raw_object_uri: raw_object_uri
      }
    }
  end

  defp source_coverage(state, story, inputs, refs) do
    inputs
    |> Enum.map(&source_contract_for(state, story, &1, refs))
    |> Enum.uniq_by(&{&1.source_ref, &1.article_ref, &1.canonical_public_url})
  end

  defp magazine_projection_provenance(
         state,
         story,
         inputs,
         story_card_version_id,
         source_coverage
       ) do
    edges =
      inputs
      |> Enum.map(&article_story_edge(state, story, &1))
      |> Enum.reject(&is_nil/1)

    evidence_refs = evidence_refs(state, story.id)

    %{
      projection_id: "news-morning",
      soup_cursor: cursor_for(state),
      story_card_version_id: story_card_version_id,
      story_id: story.id,
      story_version: story.version,
      agent_run_ids:
        edges |> Enum.map(& &1.attrs["agent_run_id"]) |> Enum.reject(&is_nil/1) |> Enum.uniq(),
      packet_hashes:
        edges |> Enum.map(& &1.attrs["packet_hash"]) |> Enum.reject(&is_nil/1) |> Enum.uniq(),
      prompt_config_hashes: [],
      prompt_config_hash_unavailable_reason:
        "prompt_config_hash_not_committed_on_article_story_edges",
      prompt_versions:
        edges
        |> Enum.map(& &1.attrs["agent_prompt_version"])
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq(),
      source_refs: source_coverage |> Enum.map(& &1.source_ref) |> Enum.reject(&is_nil/1),
      claim_refs: story_claim_refs(state, story.id),
      prior_story_card_version_id: nil,
      prior_story_card_version_reason: "no_prior_story_card_version_committed",
      evidence_refs: evidence_refs,
      field_provenance: %{
        title: %{evidence_refs: evidence_refs},
        summary: %{
          evidence_refs: evidence_refs,
          no_summary_reason: story_summary(story).no_summary_reason
        },
        deck: %{evidence_refs: evidence_refs, no_deck_reason: story_deck(story).no_deck_reason},
        key_claims: %{claim_refs: story_claim_refs(state, story.id)},
        source_coverage: %{
          source_refs: source_coverage |> Enum.map(& &1.source_ref) |> Enum.reject(&is_nil/1)
        },
        changed_since_seen: %{status: "projected_by_story_card"},
        topic_salience: %{status: "projected_by_story_card"}
      }
    }
  end

  defp magazine_contract(summary, deck, inputs, source_coverage) do
    %{
      summary_text: summary["text"] || summary[:text],
      deck_text: deck["text"] || deck[:text],
      no_summary_reason: text_unavailable_reason(summary),
      no_deck_reason: text_unavailable_reason(deck),
      source_domains: source_domains(inputs),
      no_source_domains_reason: no_source_domains_reason(inputs),
      sources: source_coverage
    }
  end

  defp text_unavailable_reason(field) do
    field["no_summary_reason"] || field[:no_summary_reason] || field["no_deck_reason"] ||
      field[:no_deck_reason] || field["reason"] || field[:reason]
  end

  defp story_claim_refs(state, story_id) do
    state
    |> active_repair_rows(state.story_fact_versions)
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.map(& &1.claim_node_id)
    |> Enum.reject(&is_nil/1)
  end

  defp article_story_edge(state, story, input) do
    input_node_ids =
      state.soup_nodes
      |> Enum.filter(&(&1.input_id == input.id and &1.state == "active"))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    story_node_ids =
      state.soup_nodes
      |> Enum.filter(&(&1.story_id == story.id and &1.state == "active"))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    state
    |> active_repair_rows(state.edges)
    |> Enum.filter(fn edge ->
      edge.status == "committed" and
        edge.attrs["edge_contract"] == "article_story_contribution" and
        MapSet.member?(input_node_ids, edge.from_node_id) and
        MapSet.member?(story_node_ids, edge.to_node_id)
    end)
    |> List.first()
  end

  defp active_repair_rows(state, rows) do
    rolled_back_ids =
      state.repair_runs
      |> Enum.filter(&(&1.status == "rolled_back"))
      |> Enum.flat_map(& &1.mutation_ids)
      |> MapSet.new()

    Enum.reject(rows, &MapSet.member?(rolled_back_ids, &1.id))
  end

  defp contribution_contract(nil) do
    %{
      summary: nil,
      type: nil,
      link_basis: nil,
      salience: nil,
      no_summary_reason: "no_article_story_contribution_committed",
      edge_provenance: nil
    }
  end

  defp contribution_contract(edge) do
    link_basis = blank_to_nil(edge.attrs["link_basis"])
    contribution_type = blank_to_nil(edge.attrs["contribution_type"])
    summary = contribution_summary(contribution_type, link_basis)

    %{
      summary: summary,
      type: contribution_type,
      link_basis: link_basis,
      salience: %{
        source_weight: edge.attrs["source_weight"],
        source_posture: edge.attrs["source_posture"]
      },
      no_summary_reason:
        if(summary, do: nil, else: "article_story_edge_has_no_link_basis_or_type"),
      edge_provenance: %{
        edge_id: edge.id,
        edge_type: edge.edge_type,
        source_ref: edge.attrs["source_ref"],
        agent_run_id: edge.attrs["agent_run_id"],
        agent_prompt_version: edge.attrs["agent_prompt_version"],
        agent_output_hash: edge.attrs["agent_output_hash"],
        packet_hash: edge.attrs["packet_hash"],
        correlation_id: edge.attrs["correlation_id"],
        evidence_refs: edge.attrs["evidence_refs"] || []
      }
    }
  end

  defp contribution_summary(nil, nil), do: nil
  defp contribution_summary(type, nil), do: "Article contribution type: #{type}."
  defp contribution_summary(nil, link_basis), do: link_basis
  defp contribution_summary(type, link_basis), do: "#{type}: #{link_basis}"

  defp media_contract(input) do
    favicon_url =
      first_present([
        input.normalized["favicon_url"],
        get_in(input.normalized, ["metadata", "favicon_url"]),
        get_in(input.normalized, ["metadata", "favicon"]),
        get_in(input.normalized, ["metadata", "provenance", "favicon_url"])
      ])
      |> public_uri()

    image_url =
      first_present([
        input.normalized["image_url"],
        input.normalized["thumbnail_url"],
        get_in(input.normalized, ["metadata", "image_url"]),
        get_in(input.normalized, ["metadata", "thumbnail_url"]),
        get_in(input.normalized, ["metadata", "og_image"]),
        get_in(input.normalized, ["metadata", "provenance", "image_url"])
      ])
      |> public_uri()

    %{
      favicon_url: favicon_url,
      no_favicon_reason: if(favicon_url, do: nil, else: "no_favicon_metadata_committed"),
      image_url: image_url,
      no_image_reason: if(image_url, do: nil, else: "no_image_metadata_committed")
    }
  end

  defp story_summary(story) do
    text = blank_to_nil(story.attrs["summary_text"])

    %{
      text: text,
      no_summary_reason: if(text, do: nil, else: "no_committed_story_summary")
    }
  end

  defp story_deck(story) do
    text = blank_to_nil(story.attrs["deck_text"])

    %{
      text: text,
      no_deck_reason: if(text, do: nil, else: "no_committed_story_deck")
    }
  end

  defp source_domains(inputs) do
    inputs
    |> Enum.map(fn input ->
      input.normalized["canonical_uri"]
      |> public_uri()
      |> then(fn
        nil -> nil
        uri -> uri |> URI.parse() |> Map.fetch!(:host) |> normalize_domain()
      end)
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp no_source_domains_reason(inputs) do
    if source_domains(inputs) == [] do
      "no_public_canonical_source_domains_committed"
    end
  end

  defp source_label(input) do
    blank_to_nil(input.normalized["source_name"]) ||
      blank_to_nil(get_in(input.normalized, ["source_actor", "name"]))
  end

  defp first_present(values), do: Enum.find_value(values, &blank_to_nil/1)

  defp public_uri(nil), do: nil

  defp public_uri(uri) do
    parsed = URI.parse(uri)

    if parsed.scheme in ["http", "https"] and is_binary(parsed.host) and
         not private_reader_uri?(uri) do
      uri
    end
  end

  defp private_reader_uri?(uri) do
    normalized = String.downcase(uri)

    Enum.any?(
      [
        "source-envelope",
        "#offset=",
        "&offset=",
        "?offset=",
        "/raw/",
        "/private/",
        "/provenance/",
        "/error"
      ],
      &String.contains?(normalized, &1)
    )
  end

  defp input_ref(input), do: "#{input.source_type}:#{input.external_id}"

  defp normalize_domain(nil), do: nil
  defp normalize_domain(domain), do: String.downcase(domain)

  defp no_source_domain_reason(canonical_uri) do
    cond do
      is_binary(canonical_uri) && private_reader_uri?(canonical_uri) ->
        "canonical_uri_is_raw_archive_reference"

      is_binary(canonical_uri) ->
        "canonical_uri_is_not_public_http_url"

      true ->
        "no_canonical_uri_committed"
    end
  end

  defp no_public_url_reason(canonical_uri, raw_object_uri) do
    cond do
      is_binary(canonical_uri) && private_reader_uri?(canonical_uri) ->
        "canonical_uri_is_raw_archive_reference"

      is_binary(canonical_uri) ->
        "canonical_uri_is_not_public_http_url"

      is_binary(raw_object_uri) ->
        "only_raw_archive_reference_available"

      true ->
        "no_source_url_committed"
    end
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  defp freshness_for(story) do
    age = DateTime.diff(parse_time!(now()), story.updated_at_story)
    %{state: story.state, age_seconds: age}
  end

  defp ranking_score(story, events), do: story.version + length(events)

  defp ranking_hints(_story, events, source_coverage) do
    domains =
      source_coverage
      |> Enum.map(& &1.source_domain)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    %{
      source_count: length(source_coverage),
      distinct_source_domain_count: length(domains),
      material_update_count:
        Enum.count(events, &(&1.classification in ["attach", "split", "conflict"])),
      topic_affinity: %{
        status: "unavailable",
        unavailable_reason: "no_story_topic_affinity_projection_committed"
      },
      seen_state: %{
        status: "unavailable",
        unavailable_reason: "no_reader_seen_state_projection_committed"
      },
      meaningful_new_info: %{
        status: "unavailable",
        unavailable_reason: "no_reader_delta_projection_committed"
      }
    }
  end

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
