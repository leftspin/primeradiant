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
    :crypto.hash(:sha256, "#{state.tenant_id}:soup:v1")
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
    story_summary = story_summary(story)
    story_deck = story_deck(story)

    story_card_version_id = "story_card:#{story.id}:v#{story.version}"
    source_coverage = source_coverage(state, story, inputs, evidence_refs)

    %{
      story_id: story.id,
      story_version: story.version,
      story_card_version_id: story_card_version_id,
      change_kind: if(latest_event, do: latest_event.classification, else: "new_story"),
      admitted_item_ids: Enum.map(inputs, & &1.id),
      title: %{text: story.title, source: "primeradiant_story", evidence_refs: evidence_refs},
      summary: %{
        text: story_summary.text,
        no_summary_reason: story_summary.no_summary_reason,
        source: "primeradiant_facts",
        evidence_refs: evidence_refs
      },
      deck: %{
        text: story_deck.text,
        no_deck_reason: story_deck.no_deck_reason,
        source: "primeradiant_facts",
        evidence_refs: evidence_refs
      },
      key_claims: key_claims(state, story.id),
      source_coverage: source_coverage,
      source_links: source_coverage,
      refresh_reason: refresh_reason(latest_event),
      changed_since_seen: changed_since_seen_state(),
      topic_salience: topic_salience_state(),
      field_completeness: field_completeness(story_summary, story_deck, inputs),
      projection_provenance:
        projection_provenance(state, story, inputs, story_card_version_id, source_coverage),
      timestamps: %{
        first_seen_at: iso(story.first_observed_at),
        last_source_at: iso(max_time(Enum.map(inputs, & &1.observed_at))),
        last_material_change_at: iso(story.last_material_at),
        last_agent_review_at: iso(max_time(Enum.map(state.agent_runs, & &1.ended_at)))
      },
      provenance: Enum.map(inputs, &provenance_for(state, story, &1, evidence_refs)),
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
        reasons: ["story_version", "material_recency"],
        hints: ranking_hints(story, events, source_coverage)
      },
      magazine_contract: %{
        summary_text: story_summary.text,
        deck_text: story_deck.text,
        no_summary_reason: story_summary.no_summary_reason,
        no_deck_reason: story_deck.no_deck_reason,
        source_domains: source_domains(inputs),
        no_source_domains_reason: no_source_domains_reason(inputs),
        sources: source_coverage
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

  defp provenance_for(state, story, input, refs) do
    source = source_contract_for(state, story, input, refs)

    %{
      article_ref: source.article_ref,
      article_title: source.article_title,
      article_name: source.article_name,
      source_ref: source.source_ref,
      source_domain: source.source_domain,
      no_source_domain_reason: source.no_source_domain_reason,
      source_label: source.source_label,
      no_source_label_reason: source.no_source_label_reason,
      publication: source.publication,
      no_publication_reason: source.no_publication_reason,
      url: source.url,
      canonical_public_url: source.canonical_public_url,
      raw_object_uri: source.raw_object_uri,
      url_kind: source.url_kind,
      link_status: source.link_status,
      unavailable_reason: source.unavailable_reason,
      favicon_url: source.favicon_url,
      no_favicon_reason: source.no_favicon_reason,
      image_url: source.image_url,
      no_image_reason: source.no_image_reason,
      contribution_summary: source.contribution_summary,
      contribution_type: source.contribution_type,
      contribution_link_basis: source.contribution_link_basis,
      no_contribution_summary_reason: source.no_contribution_summary_reason,
      published_at: iso(input.observed_at),
      admitted_at: iso(input.inserted_at),
      evidence_refs: refs
    }
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

  defp key_claims(state, story_id) do
    state.story_fact_versions
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.map(fn fact ->
      %{
        claim_ref: fact.claim_node_id,
        fact_key: fact.fact_key,
        text: fact.fact_value,
        status: fact.status,
        evidence_refs: evidence_refs_for_subject(state, fact.id)
      }
    end)
  end

  defp refresh_reason(nil), do: "story_projected"
  defp refresh_reason(event), do: event.classification

  defp field_completeness(story_summary, story_deck, inputs) do
    %{
      summary: completeness(story_summary.text),
      deck: completeness(story_deck.text),
      canonical_public_url:
        if(Enum.any?(inputs, &(public_uri(&1.normalized["canonical_uri"]) != nil)),
          do: "partial",
          else: "incomplete"
        ),
      source_domain: if(source_domains(inputs) == [], do: "incomplete", else: "partial")
    }
  end

  defp completeness(nil), do: "incomplete"
  defp completeness(_), do: "complete"

  defp projection_provenance(state, story, inputs, story_card_version_id, source_coverage) do
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
      claim_refs:
        key_claims(state, story.id) |> Enum.map(& &1.claim_ref) |> Enum.reject(&is_nil/1),
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
        key_claims: %{claim_refs: key_claims(state, story.id) |> Enum.map(& &1.claim_ref)},
        source_coverage: %{
          source_refs: source_coverage |> Enum.map(& &1.source_ref) |> Enum.reject(&is_nil/1)
        },
        changed_since_seen: %{
          status: "unavailable",
          unavailable_reason: "no_reader_seen_state_projection_committed"
        },
        topic_salience: %{
          status: "unavailable",
          unavailable_reason: "no_story_topic_salience_projection_committed"
        }
      }
    }
  end

  defp evidence_refs_for_subject(state, subject_id) do
    state.evidence_refs
    |> Enum.filter(&(&1.subject_id == subject_id))
    |> Enum.map(& &1.evidence_label)
    |> Enum.uniq()
  end

  defp article_story_edge(state, story, input) do
    input_node_ids =
      state.soup_nodes
      |> Enum.filter(&(&1.input_id == input.id))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    story_node_ids =
      state.soup_nodes
      |> Enum.filter(&(&1.story_id == story.id))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    state.edges
    |> Enum.filter(fn edge ->
      edge.attrs["edge_contract"] == "article_story_contribution" and
        MapSet.member?(input_node_ids, edge.from_node_id) and
        MapSet.member?(story_node_ids, edge.to_node_id)
    end)
    |> List.first()
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

  defp changed_since_seen_state do
    %{
      status: "unavailable",
      unavailable_reason: "no_reader_seen_state_projection_committed"
    }
  end

  defp topic_salience_state do
    %{
      status: "unavailable",
      unavailable_reason: "no_story_topic_salience_projection_committed"
    }
  end

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
