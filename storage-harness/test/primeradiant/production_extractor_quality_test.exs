defmodule Primeradiant.ProductionExtractorQualityTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Extraction.ProductionExtractor
  alias Primeradiant.StorageHarness.{ChangesetStore, Input, KnowledgeWork, RealIngestion}

  @tenant Ecto.UUID.generate()

  test "production_extractor_v1 validates contract and spans across six eval packs" do
    results = Enum.map(extractor_corpus(), &extract_case!/1)

    assert pack_count(results, :public_messy) >= 12
    assert pack_count(results, :opinion_framing) >= 6
    assert pack_count(results, :private_task) >= 8
    assert pack_count(results, :temporal_contradiction) >= 8
    assert pack_count(results, :low_confidence_refusal) >= 6
    assert pack_count(results, :downstream_integration) >= 10

    Enum.each(results, fn %{input: input, result: result} ->
      assert result.extractor_run["contract_version"] == "production_extractor_v1"
      assert result.extractor_run["extractor_version"] =~ "production_extractor_v1"
      assert result.input_context["acl"] == input.acl

      if result.quality["status"] not in ["low_confidence", "refused"] do
        refute result.evidence_index == []
      end

      assert_every_material_item_has_visible_evidence(input, result)
      refute Enum.any?(result.framing, &(&1["promoted_to_structural_fact"] == true))
    end)
  end

  test "extractor quality metrics meet T321 thresholds without fixture labels or noun shortcuts" do
    scored = Enum.map(extractor_corpus(), &extract_case!/1)
    metrics = metrics(scored)

    assert metrics.claim_precision >= 0.85
    assert metrics.claim_recall >= 0.75
    assert metrics.entity_precision >= 0.85
    assert metrics.entity_role_accuracy >= 0.80
    assert metrics.time_accuracy >= 0.80
    assert metrics.stance_modality_accuracy >= 0.85
    assert metrics.framing_accuracy >= 0.85
    assert metrics.refusal_accuracy >= 0.90
    assert metrics.false_structural_fact_rate < 0.05
  end

  test "forbidden pass inputs do not steer extraction" do
    body = "Metro Clinic triage is open for current venue is north"

    baseline =
      extract_case!(
        case_item(:public_messy, "baseline", body,
          expected_facts: %{"triage" => "open", "venue" => "north"}
        )
      )

    misleading =
      extract_case!(
        case_item(:public_messy, "fixture-story-south-expected-conflict", body,
          title: "Misleading south headline",
          expected_facts: %{"triage" => "open", "venue" => "north"}
        )
      )

    assert fact_map(baseline.result) == fact_map(misleading.result)

    assert_raise ArgumentError, "real admission rejects trusted label story_hint", fn ->
      RealIngestion.ingest_items([
        Map.put(source_item("bad-story-hint", body), :metadata, %{story_hint: "trusted"})
      ])
    end
  end

  test "production extractor output drives downstream story semantics" do
    {:ok, state, report} = RealIngestion.ingest_items(downstream_items())
    decisions = Map.new(report.decisions, &{&1.input_ref, &1.decision_type})

    assert :split in Map.values(decisions)
    assert :attach in Map.values(decisions)
    assert :conflict in Map.values(decisions)
    assert :no_op in Map.values(decisions)
    assert :color in Map.values(decisions)
    assert :stale in Map.values(decisions)
    assert :abstain in Map.values(decisions)

    graph_evidence =
      Enum.filter(
        state.evidence_refs,
        &(&1.subject_type in [
            "proposal_op",
            "graph_commit",
            "story_fact_version",
            "edge",
            "conflict",
            "story_event"
          ])
      )

    assert graph_evidence != []
    assert Enum.all?(graph_evidence, &String.contains?(&1.evidence_label, "#ev:"))

    state =
      KnowledgeWork.attach_watches(state, [
        %{
          watch_key: "watch:flynn:device-deadline",
          intent: "Track device battery deadline",
          priority: 80,
          match_any: ["device", "battery", "deadline"]
        }
      ])

    {:ok, state, first_delta} = KnowledgeWork.record_verified_delta(state, "flynn")
    assert first_delta.verified
    assert first_delta.text =~ "[watch]"

    {state, _repeat} =
      RealIngestion.ingest_item(state, %{
        source_item(
          "clinic-repeat-after-seen",
          "Metro Clinic triage is open for current venue is north hours is evening"
        )
        | observed_at: "2026-05-17T11:00:00Z"
      })

    {state, _spin} =
      RealIngestion.ingest_item(state, %{
        source_item(
          "clinic-spin-after-seen",
          "Opinion column: Metro Clinic leaders spin the triage disruption as modernization."
        )
        | source_type: "opinion_column",
          observed_at: "2026-05-17T11:05:00Z"
      })

    {state, _update} =
      RealIngestion.ingest_item(state, %{
        source_item(
          "device-update-after-seen",
          "Device Battery battery is low for current location is lab deadline is friday"
        )
        | source_type: "user_note",
          observed_at: "2026-05-17T11:10:00Z",
          acl: %{"privacy" => "private", "participants" => ["flynn"]}
      })

    {:ok, _state, second_delta} = KnowledgeWork.record_verified_delta(state, "flynn")
    assert second_delta.text =~ "deadline=friday"
    refute second_delta.text =~ "spin-after-seen"
    refute second_delta.text =~ "repeat-after-seen"
  end

  defp extract_case!(case_def) do
    input = input_from_case(case_def)
    {:ok, result} = ProductionExtractor.extract(input)
    Map.merge(case_def, %{input: input, result: result})
  end

  defp input_from_case(case_def) do
    item = source_item(case_def.id, case_def.body, case_def.opts || [])

    ChangesetStore.insert!(Input, %{
      tenant_id: item.tenant_id,
      fixture_id: nil,
      source_type: item.source_type,
      external_id: item.external_id,
      observed_at: item.observed_at,
      title: item.title,
      body_text: item.body_text,
      object_uri: item.canonical_uri,
      content_sha256: ChangesetStore.hash(item.body_text || ""),
      acl: item.acl,
      normalized: %{
        "source_mode" => item.source_mode,
        "ingestion_run_key" => item.ingestion_run_key,
        "retrieved_at" => item.retrieved_at,
        "occurred_at" => item.occurred_at,
        "canonical_uri" => item.canonical_uri,
        "source_name" => item.source_name,
        "source_actor" => item.source_actor,
        "metadata" => item.metadata
      },
      facts: %{},
      background: %{},
      questions: %{},
      colors: [],
      topic_tokens: []
    })
  end

  defp assert_every_material_item_has_visible_evidence(input, result) do
    evidence = Map.new(result.evidence_index, &{&1["evidence_id"], &1})

    material_items =
      result.claims ++
        result.entities ++
        result.times ++ result.source_stance ++ result.framing ++ result.questions

    Enum.each(material_items, fn item ->
      refs = item["evidence_refs"]
      assert is_list(refs) and refs != []

      Enum.each(refs, fn ref ->
        evidence_span = Map.fetch!(evidence, ref)
        assert evidence_span["acl"] == input.acl
        assert evidence_span["span_start"] < evidence_span["span_end"]

        assert byte_size(evidence_span["quote"]) ==
                 evidence_span["span_end"] - evidence_span["span_start"]
      end)
    end)
  end

  defp metrics(scored) do
    expected_facts = scored |> Enum.flat_map(&Map.to_list(&1.expected_facts || %{})) |> length()
    found_facts = Enum.reduce(scored, 0, &(&2 + count_expected_facts(&1)))
    actual_facts = scored |> Enum.flat_map(&Map.to_list(fact_map(&1.result))) |> length()
    false_structural = max(actual_facts - found_facts, 0)

    expected_entities = scored |> Enum.flat_map(&(&1.expected_entities || [])) |> length()
    found_entities = Enum.reduce(scored, 0, &(&2 + count_expected_entities(&1)))

    role_total = scored |> Enum.flat_map(&Map.to_list(&1.expected_roles || %{})) |> length()
    role_hits = Enum.reduce(scored, 0, &(&2 + count_expected_roles(&1)))

    time_total = Enum.count(scored, &(&1.expected_time != nil))
    time_hits = Enum.count(scored, &time_hit?/1)

    stance_total = Enum.count(scored, &(&1.expected_stance != nil or &1.expected_modality != nil))
    stance_hits = Enum.count(scored, &stance_hit?/1)

    framing_total = Enum.count(scored, &(&1.expected_framing != nil))
    framing_hits = Enum.count(scored, &framing_hit?/1)

    refusal_total = Enum.count(scored, &(&1.expected_quality in ["low_confidence", "refused"]))

    refusal_hits =
      Enum.count(
        scored,
        &(&1.expected_quality in ["low_confidence", "refused"] and
            &1.result.quality["status"] == &1.expected_quality)
      )

    %{
      claim_precision: ratio(found_facts, max(actual_facts, 1)),
      claim_recall: ratio(found_facts, max(expected_facts, 1)),
      entity_precision: ratio(found_entities, max(expected_entities, 1)),
      entity_role_accuracy: ratio(role_hits, max(role_total, 1)),
      time_accuracy: ratio(time_hits, max(time_total, 1)),
      stance_modality_accuracy: ratio(stance_hits, max(stance_total, 1)),
      framing_accuracy: ratio(framing_hits, max(framing_total, 1)),
      refusal_accuracy: ratio(refusal_hits, max(refusal_total, 1)),
      false_structural_fact_rate: ratio(false_structural, max(actual_facts, 1))
    }
  end

  defp count_expected_facts(case_def) do
    facts = fact_map(case_def.result)
    Enum.count(case_def.expected_facts || %{}, fn {key, value} -> facts[key] == value end)
  end

  defp count_expected_entities(case_def) do
    entities = MapSet.new(Enum.map(case_def.result.entities, & &1["canonical_key"]))
    Enum.count(case_def.expected_entities || [], &MapSet.member?(entities, &1))
  end

  defp count_expected_roles(case_def) do
    roles = Map.new(case_def.result.entities, &{&1["canonical_key"], &1["role_in_source"]})
    Enum.count(case_def.expected_roles || %{}, fn {entity, role} -> roles[entity] == role end)
  end

  defp time_hit?(%{expected_time: nil}), do: false

  defp time_hit?(case_def) do
    Enum.any?(case_def.result.times, fn time ->
      time["relative_text"] == case_def.expected_time or
        time["legacy_scope"] == case_def.expected_time
    end)
  end

  defp stance_hit?(case_def) do
    stance_hit =
      case case_def.expected_stance do
        nil -> true
        expected -> Enum.any?(case_def.result.source_stance, &(&1["stance_kind"] == expected))
      end

    modality_hit =
      case case_def.expected_modality do
        nil -> true
        expected -> Enum.any?(case_def.result.claims, &(&1["modality"] == expected))
      end

    stance_hit and modality_hit
  end

  defp framing_hit?(%{expected_framing: nil}), do: false

  defp framing_hit?(case_def),
    do: Enum.any?(case_def.result.framing, &(&1["framing_kind"] == case_def.expected_framing))

  defp fact_map(result) do
    result.claims
    |> Enum.filter(&(&1["structural_candidate"] == true))
    |> Map.new(&{&1["fact_key"], &1["fact_value"]})
  end

  defp pack_count(results, pack), do: Enum.count(results, &(&1.pack == pack))
  defp ratio(numerator, denominator), do: numerator / denominator

  defp extractor_corpus do
    public_pack() ++
      opinion_pack() ++ private_pack() ++ temporal_pack() ++ refusal_pack() ++ downstream_pack()
  end

  defp public_pack do
    [
      case_item(
        :public_messy,
        "pub-1",
        "Advertisement\nMetro Clinic triage is open for current venue is north.\nSubscribe now.",
        expected_facts: %{"triage" => "open", "venue" => "north"},
        expected_entities: ["metro-clinic"],
        expected_roles: %{"metro-clinic" => "subject"},
        expected_stance: "reporting"
      ),
      case_item(
        :public_messy,
        "pub-2",
        "Headline: shutdown chaos. Body: River Ferry service normal now; route east.",
        expected_facts: %{"service_state" => "normal", "route" => "east"},
        expected_entities: ["river-ferry"]
      ),
      case_item(
        :public_messy,
        "pub-3",
        "Correction: Harbor Gate service is restored for afternoon after earlier delay.",
        expected_facts: %{"service_state" => "restored"},
        expected_modality: "corrected",
        expected_time: "afternoon",
        expected_stance: "correction"
      ),
      case_item(:public_messy, "pub-4", "North Hall status is open for today venue is atrium.",
        expected_facts: %{"status" => "open", "venue" => "atrium"},
        expected_time: "today"
      ),
      case_item(:public_messy, "pub-5", "South Hall status is open for today venue is plaza.",
        expected_facts: %{"status" => "open", "venue" => "plaza"},
        expected_time: "today"
      ),
      case_item(
        :public_messy,
        "pub-6",
        "The agency said Lake Line halted service for morning.",
        expected_facts: %{"service_state" => "halted"},
        expected_modality: "reported",
        expected_time: "morning"
      ),
      case_item(
        :public_messy,
        "pub-7",
        "A source said talks may resume Friday while Gate Project status is paused.",
        expected_facts: %{"status" => "paused"},
        expected_modality: "possible",
        expected_time: "friday"
      ),
      case_item(
        :public_messy,
        "pub-8",
        "Library Board decision is approved for current amount is 1200.",
        expected_facts: %{"decision" => "approved", "amount" => "1200"},
        expected_entities: ["library-board"]
      ),
      case_item(
        :public_messy,
        "pub-9",
        "Campus Shuttle route west; service normal.",
        expected_facts: %{"route" => "west", "service_state" => "normal"},
        expected_entities: ["campus-shuttle"]
      ),
      case_item(
        :public_messy,
        "pub-10",
        "Museum Office deadline moved to monday; owner registrar.",
        expected_facts: %{"deadline" => "monday", "owner" => "registrar"},
        expected_time: "monday"
      ),
      case_item(
        :public_messy,
        "pub-11",
        "One article mentions two events. Clinic Desk triage is open. Permit Office status is closed.",
        expected_facts: %{"triage" => "open", "status" => "closed"},
        expected_entities: ["clinic-desk", "permit-office"]
      ),
      case_item(
        :public_messy,
        "pub-12",
        "Recommended stories below. Market Agency amount is 4500 for current venue is annex.",
        expected_facts: %{"amount" => "4500", "venue" => "annex"},
        expected_entities: ["market-agency"]
      )
    ]
  end

  defp opinion_pack do
    [
      case_item(
        :opinion_framing,
        "op-1",
        "Opinion column: Metro Clinic leaders spin the triage disruption as modernization.",
        source_type: "opinion_column",
        expected_framing: "strategic_spin",
        expected_stance: "opinion"
      ),
      case_item(
        :opinion_framing,
        "op-2",
        "Columnist blamed officials while River Ferry service is halted for current.",
        source_type: "opinion_column",
        expected_facts: %{"service_state" => "halted"},
        expected_framing: "blame",
        expected_stance: "opinion"
      ),
      case_item(:opinion_framing, "op-3", "Analysis: the avoidable shutdown shows failure.",
        source_type: "opinion_column",
        expected_framing: "criticism",
        expected_stance: "opinion"
      ),
      case_item(:opinion_framing, "op-4", "Officials reassured riders that crews are calm.",
        source_type: "opinion_column",
        expected_framing: "reassurance"
      ),
      case_item(
        :opinion_framing,
        "op-5",
        "Families waited in the rain while the column criticized delays.",
        source_type: "opinion_column",
        expected_framing: "human_color"
      ),
      case_item(:opinion_framing, "op-6", "Campaign allies minimized the outage as minor.",
        source_type: "opinion_column",
        expected_framing: "minimization"
      )
    ]
  end

  defp private_pack do
    [
      case_item(:private_task, "priv-1", "Roof Board quoted_amount is 14800 deadline is friday.",
        source_type: "message",
        acl: private_acl(),
        expected_facts: %{"quoted_amount" => "14800", "deadline" => "friday"},
        expected_time: "friday"
      ),
      case_item(:private_task, "priv-2", "Roof Board quoted_amount is 15000. Who has the packet?",
        source_type: "message",
        acl: private_acl(),
        expected_facts: %{"quoted_amount" => "15000"}
      ),
      case_item(
        :private_task,
        "priv-3",
        "Account Team owner is dana for current deadline is tomorrow.",
        source_type: "email",
        acl: private_acl(),
        expected_facts: %{"owner" => "dana", "deadline" => "tomorrow"},
        expected_time: "tomorrow"
      ),
      case_item(
        :private_task,
        "priv-4",
        "Project Note task is blocked for current assignee is lee.",
        source_type: "user_note",
        acl: private_acl(),
        expected_facts: %{"task" => "blocked", "assignee" => "lee"}
      ),
      case_item(
        :private_task,
        "priv-5",
        "Invoice Thread amount is 9300 for current status is pending.",
        source_type: "email",
        acl: private_acl(),
        expected_facts: %{"amount" => "9300", "status" => "pending"}
      ),
      case_item(
        :private_task,
        "priv-6",
        "Device Battery battery low; location lab; deadline tonight.",
        source_type: "user_note",
        acl: private_acl(),
        expected_facts: %{"battery" => "low", "location" => "lab", "deadline" => "tonight"},
        expected_time: "tonight"
      ),
      case_item(
        :private_task,
        "priv-7",
        "Board Packet decision is approved. Who sends the signature?",
        source_type: "message",
        acl: private_acl(),
        expected_facts: %{"decision" => "approved"}
      ),
      case_item(:private_task, "priv-8", "Ops Thread status is duplicate for current.",
        source_type: "message",
        acl: private_acl(),
        expected_facts: %{"status" => "duplicate"}
      )
    ]
  end

  defp temporal_pack do
    [
      case_item(:temporal_contradiction, "tmp-1", "Bridge Line service is halted for morning.",
        expected_facts: %{"service_state" => "halted"},
        expected_time: "morning"
      ),
      case_item(
        :temporal_contradiction,
        "tmp-2",
        "Bridge Line service is restored for afternoon.",
        expected_facts: %{"service_state" => "restored"},
        expected_time: "afternoon"
      ),
      case_item(
        :temporal_contradiction,
        "tmp-3",
        "The agency denied Bridge Line service is halted for current.",
        expected_facts: %{},
        expected_modality: "denied",
        expected_stance: "denial"
      ),
      case_item(
        :temporal_contradiction,
        "tmp-4",
        "Correction: Bridge Line service is normal for current.",
        expected_facts: %{"service_state" => "normal"},
        expected_modality: "corrected"
      ),
      case_item(
        :temporal_contradiction,
        "tmp-5",
        "Permit Office deadline is tomorrow for current.",
        expected_facts: %{"deadline" => "tomorrow"},
        expected_time: "tomorrow"
      ),
      case_item(:temporal_contradiction, "tmp-6", "Permit Office deadline is today for current.",
        expected_facts: %{"deadline" => "today"},
        expected_time: "today"
      ),
      case_item(:temporal_contradiction, "tmp-7", "A source said Market Talks may resume Friday.",
        expected_facts: %{},
        expected_modality: "possible",
        expected_time: "friday"
      ),
      case_item(
        :temporal_contradiction,
        "tmp-8",
        "Council Vote decision is approved for last week.",
        expected_facts: %{"decision" => "approved"},
        expected_time: "last week"
      )
    ]
  end

  defp refusal_pack do
    [
      case_item(:low_confidence_refusal, "ref-1", "Subscribe. Advertisement. Related articles.",
        expected_quality: "low_confidence"
      ),
      case_item(:low_confidence_refusal, "ref-2", "Tiny note.",
        expected_quality: "low_confidence"
      ),
      case_item(
        :low_confidence_refusal,
        "ref-3",
        "Miscellaneous page without supported anchors or claims.",
        expected_quality: "low_confidence"
      ),
      case_item(
        :low_confidence_refusal,
        "ref-4",
        "No readable source facts here, only share buttons and cookie settings.",
        expected_quality: "low_confidence"
      ),
      case_item(:low_confidence_refusal, "ref-5", "¿Servicio normal?",
        expected_quality: "refused"
      ),
      case_item(
        :low_confidence_refusal,
        "ref-6",
        "Rumor and vibes with no actor, object, or observable state.",
        expected_quality: "low_confidence"
      )
    ]
  end

  defp downstream_pack do
    [
      case_item(
        :downstream_integration,
        "down-1",
        "Metro Clinic triage is open for current venue is north hours is evening",
        expected_facts: %{"triage" => "open", "venue" => "north", "hours" => "evening"}
      ),
      case_item(
        :downstream_integration,
        "down-2",
        "Metro Clinic triage is open for current venue is south hours is evening",
        expected_facts: %{"triage" => "open", "venue" => "south", "hours" => "evening"}
      ),
      case_item(
        :downstream_integration,
        "down-3",
        "Metro Clinic triage is open for current venue is north hours is evening. Who has the plan?",
        expected_facts: %{"triage" => "open", "venue" => "north", "hours" => "evening"}
      ),
      case_item(
        :downstream_integration,
        "down-4",
        "Metro Clinic triage is closed for current venue is north hours is evening",
        expected_facts: %{"triage" => "closed", "venue" => "north", "hours" => "evening"}
      ),
      case_item(
        :downstream_integration,
        "down-5",
        "Metro Clinic triage is open for current venue is north hours is evening",
        expected_facts: %{"triage" => "open", "venue" => "north", "hours" => "evening"}
      ),
      case_item(
        :downstream_integration,
        "down-6",
        "Opinion column: Metro Clinic leaders spin the triage disruption as modernization.",
        source_type: "opinion_column",
        expected_framing: "strategic_spin"
      ),
      case_item(
        :downstream_integration,
        "down-7",
        "Metro Clinic background check: no new activity today.",
        source_type: "user_note"
      ),
      case_item(:downstream_integration, "down-8", "Thin page without evidence.",
        expected_quality: "low_confidence"
      ),
      case_item(
        :downstream_integration,
        "down-9",
        "Device Battery battery is low for current location is lab deadline is tonight.",
        source_type: "user_note",
        acl: private_acl(),
        expected_facts: %{"battery" => "low", "location" => "lab", "deadline" => "tonight"}
      ),
      case_item(
        :downstream_integration,
        "down-10",
        "Device Battery battery is low for current location is lab deadline is tomorrow.",
        source_type: "user_note",
        acl: private_acl(),
        expected_facts: %{"battery" => "low", "location" => "lab", "deadline" => "tomorrow"}
      )
    ]
  end

  defp downstream_items do
    downstream_pack()
    |> Enum.map(fn case_def -> source_item(case_def.id, case_def.body, case_def.opts || []) end)
  end

  defp case_item(pack, id, body, opts) do
    %{
      pack: pack,
      id: id,
      body: body,
      opts: opts,
      expected_facts: Keyword.get(opts, :expected_facts, %{}),
      expected_entities: Keyword.get(opts, :expected_entities, []),
      expected_roles: Keyword.get(opts, :expected_roles, %{}),
      expected_time: Keyword.get(opts, :expected_time),
      expected_stance: Keyword.get(opts, :expected_stance),
      expected_modality: Keyword.get(opts, :expected_modality),
      expected_framing: Keyword.get(opts, :expected_framing),
      expected_quality: Keyword.get(opts, :expected_quality, "usable")
    }
  end

  defp source_item(id, body, opts \\ []) do
    %{
      tenant_id: @tenant,
      ingestion_run_key: "run-production-extractor-eval",
      source_type: Keyword.get(opts, :source_type, "news_article"),
      source_mode: "manual_real_ingest_v1",
      external_id: id,
      observed_at: Keyword.get(opts, :observed_at, "2026-05-17T10:00:00Z"),
      retrieved_at: "2026-05-17T10:01:00Z",
      occurred_at: nil,
      canonical_uri: "https://extractor.example.test/#{id}",
      raw_object_uri: nil,
      source_name: "Extractor Eval",
      source_actor:
        Keyword.get(opts, :source_actor, %{
          kind: "publisher",
          name: "Extractor Eval",
          stable_id: "extractor-eval"
        }),
      title: Keyword.get(opts, :title, "Extractor eval #{id}"),
      body_text: body,
      extracted_text: nil,
      metadata: Keyword.get(opts, :metadata, %{}),
      acl: Keyword.get(opts, :acl, %{"privacy" => "public"})
    }
  end

  defp private_acl, do: %{"privacy" => "private", "participants" => ["flynn"]}
end
