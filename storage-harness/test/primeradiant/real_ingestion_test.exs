defmodule Primeradiant.RealIngestionTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StorageHarness.{LiveStoryAgentLoop, RealIngestion, State}

  @tenant Ecto.UUID.generate()

  test "T1223 source ingest admits evidence and provenance only" do
    {:ok, state, report} = RealIngestion.ingest_items(real_style_items())

    assert report.source_behavior == :evidence_admission_only
    assert report.substrate_proof_only
    assert report.meaning_proof == :requires_packet_grounded_agent_runs
    assert report.stories == 0
    assert report.proposals == 0
    assert report.graph_commits == 0

    assert length(report.admissions) == 3
    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))
    assert Enum.all?(state.inputs, &(&1.facts == %{}))
    assert Enum.all?(state.inputs, &(&1.background == %{}))
    assert Enum.all?(state.inputs, &(&1.questions == %{}))
    assert Enum.all?(state.inputs, &(&1.colors == []))
    assert Enum.all?(state.inputs, &(&1.topic_tokens == []))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["admission_status"]) == "admitted"))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["source_ref"]) != nil))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["content_span_refs"]) != []))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["evidence_refs"]) != []))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["story_identity"]) == nil))

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["meaning_proof"]) == "not_ingest_owned")
           )

    Enum.each(report.admissions, fn admission ->
      assert admission.event == :source_admitted
      assert admission.admission_status == "admitted"
      assert is_binary(admission.source_ref)
      assert is_binary(admission.content_sha256)
      assert admission.content_span_refs != []
      assert admission.evidence_refs != []
      assert admission.normalized_evidence.content_hash == admission.content_sha256
      assert admission.visibility["privacy"] in ["public", "private"]
      assert admission.adapter_version in ["manual_real_ingest_v1", "daemon_news_event_r1"]
      assert admission.story_identity == nil
      assert admission.story_classification == nil
      assert admission.materiality_decision == nil
      assert admission.relevance_decision == nil
      assert admission.narrative_dedupe == nil
      assert admission.meaning_proof == :not_ingest_owned
    end)

    refute Enum.any?(state.audit_events, &Map.has_key?(&1, :story_key))
    refute Enum.any?(state.audit_events, &Map.has_key?(&1, :classification))
  end

  test "T1223 ingest-event activations are scheduler substrate only" do
    {:ok, state, report} = RealIngestion.ingest_items(real_style_items())

    activations = Enum.filter(state.audit_events, &(&1.event == :ingest_event_activation))

    assert report.ingest_event_activations == 3
    assert length(activations) == 3

    Enum.each(activations, fn activation ->
      assert activation.activation_kind == :ingest_event
      assert activation.scheduler_substrate
      refute activation.story_meaning_proof
      assert is_binary(activation.trigger_id)
      assert activation.packet_seed_refs != []
    end)
  end

  test "T1223 admission rejects source-owned story labels and preserves source read boundary" do
    assert_raise ArgumentError, "real admission rejects trusted label fixture_id", fn ->
      Admission.admit_source_item(
        State.new(tenant_id: @tenant),
        Map.put(base_item("bad-1"), :fixture_id, "fixture_story_001")
      )
    end

    assert_raise ArgumentError, "real admission rejects trusted label story_hint", fn ->
      item = put_in(base_item("bad-2"), [:metadata], %{story_hint: "trusted-story"})
      Admission.admit_source_item(State.new(tenant_id: @tenant), item)
    end

    assert_raise ArgumentError, "private source items require participants", fn ->
      item = %{
        base_item("bad-3")
        | acl: %{"privacy" => "private", "participants" => []},
          source_type: "message"
      }

      Admission.admit_source_item(State.new(tenant_id: @tenant), item)
    end

    assert_raise ArgumentError, "real source item requires tenant_id", fn ->
      item = Map.delete(base_item("bad-4"), :tenant_id)
      Admission.admit_source_item(State.new(tenant_id: @tenant), item)
    end

    assert_raise ArgumentError, "source_mode must be manual_real_ingest_v1", fn ->
      item = Map.delete(base_item("bad-5"), :source_mode)
      Admission.admit_source_item(State.new(tenant_id: @tenant), item)
    end
  end

  test "T1223 replay/idempotency does not dedupe narrative meaning" do
    {:ok, state, report} = RealIngestion.ingest_items(repetition_items())

    assert report.inputs == 2
    assert report.proposals == 0
    assert report.graph_commits == 0

    refs = Enum.map(report.admissions, & &1.source_ref)
    assert "news_article:harbor-repeat-1" in refs
    assert "news_article:harbor-repeat-2" in refs

    assert Enum.map(state.inputs, & &1.external_id) |> Enum.sort() ==
             ["harbor-repeat-1", "harbor-repeat-2"]
  end

  test "T1326 polluted story hint remains context and does not override agent story identity" do
    {:ok, state, report} = RealIngestion.ingest_items(polluted_hint_items())
    capture_key = {__MODULE__, make_ref()}

    adapter = fn config, packet, ctx ->
      if config.role in [:story_identity, :meaning_update] do
        Process.put(
          capture_key,
          Process.get(capture_key, []) ++ [{config.role, packet}]
        )
      end

      config
      |> polluted_hint_story_agent(packet, ctx)
      |> Map.put(:model_packet, %{role: config.role, raw_database_access: false})
    end

    {state, loop_report} =
      LiveStoryAgentLoop.run(state, report.admissions, "flynn", adapter: adapter)

    assert loop_report.story_meaning_proof

    assert Enum.map(state.stories, & &1.story_key) |> Enum.sort() == [
             "forgotlings-ps5-launch",
             "trump-israel-newsweek"
           ]

    assert Enum.count(state.story_events) == 3

    [_first_chain, second_chain, third_chain] = loop_report.correlation_chains
    assert second_chain.story_key == "forgotlings-ps5-launch"

    second_op =
      Enum.find(state.proposal_ops, &(&1.id == second_chain.proposal_op_id))

    assert second_op.payload["soup_candidate_hint"] == nil

    second_edge =
      Enum.find(state.edges, &(&1.proposal_op_id == second_chain.proposal_op_id))

    assert second_edge.attrs["edge_contract"] == "article_story_contribution"
    assert second_edge.attrs["link_basis"] =~ "agent selected"
    assert second_edge.attrs["source_ref"] == "news_article:forgotlings-ps5"

    assert third_chain.story_key == "forgotlings-ps5-launch"
    assert third_chain.classification == "attach"

    third_op =
      Enum.find(state.proposal_ops, &(&1.id == third_chain.proposal_op_id))

    assert get_in(third_op.payload, ["soup_candidate_hint", :suggested_story_key]) ==
             "trump-israel-newsweek"

    [
      {:story_identity, first_identity_packet},
      {:meaning_update, first_meaning_packet},
      {:story_identity, second_identity_packet},
      {:meaning_update, second_meaning_packet},
      {:story_identity, third_identity_packet},
      {:meaning_update, third_meaning_packet}
    ] = Process.get(capture_key)

    assert first_identity_packet.visible_story_refs == []
    assert first_meaning_packet.visible_story_refs == []

    second_identity_refs = second_identity_packet.visible_story_refs
    second_meaning_refs = second_meaning_packet.visible_story_refs
    third_identity_refs = third_identity_packet.visible_story_refs
    third_meaning_refs = third_meaning_packet.visible_story_refs

    assert Enum.map(second_identity_refs, & &1.story_key) == ["trump-israel-newsweek"]
    assert second_meaning_refs == second_identity_refs

    assert Enum.map(third_identity_refs, & &1.story_key) == [
             "trump-israel-newsweek",
             "forgotlings-ps5-launch"
           ]

    assert third_meaning_refs == third_identity_refs

    assert Enum.all?(third_identity_refs, &Map.has_key?(&1, :structural_facts))
    assert Enum.all?(third_identity_refs, &Map.has_key?(&1, :evidence_input_refs))

    first_identity_run = hd(state.agent_runs)

    durable_packet_hash =
      :sha256
      |> :crypto.hash(:erlang.term_to_binary(first_identity_packet))
      |> Base.encode16(case: :lower)

    reduced_packet_hash =
      :sha256
      |> :crypto.hash(
        :erlang.term_to_binary(%{role: :story_identity, raw_database_access: false})
      )
      |> Base.encode16(case: :lower)

    assert first_identity_run.scope["packet_hash"] == durable_packet_hash
    refute first_identity_run.scope["packet_hash"] == reduced_packet_hash
  end

  test "T1223 direct ingestion enforces tenant boundary" do
    other_tenant = Ecto.UUID.generate()
    state = State.new(tenant_id: @tenant)

    assert_raise ArgumentError, "real source item tenant_id must match ingestion state", fn ->
      RealIngestion.ingest_item(state, %{base_item("tenant-mismatch") | tenant_id: other_tenant})
    end

    assert_raise ArgumentError, "real ingestion batch must use one tenant_id", fn ->
      [first, second | _] = real_style_items()
      RealIngestion.ingest_items([first, %{second | tenant_id: other_tenant}])
    end
  end

  defp base_item(external_id) do
    %{
      tenant_id: @tenant,
      ingestion_run_key: "run-real-eval",
      source_type: "news_article",
      source_mode: "manual_real_ingest_v1",
      external_id: external_id,
      observed_at: "2026-05-17T10:00:00Z",
      retrieved_at: "2026-05-17T10:01:00Z",
      occurred_at: nil,
      canonical_uri: "https://example.test/#{external_id}",
      raw_object_uri: nil,
      source_name: "Example",
      source_actor: %{kind: "publisher", name: "Example", stable_id: "example"},
      title: "Real item #{external_id}",
      body_text: "Harbor Ferry service is halted for current route is harbor",
      extracted_text: nil,
      metadata: %{},
      acl: %{"privacy" => "public"}
    }
  end

  defp real_style_items do
    [
      base_item("harbor-1"),
      %{
        base_item("harbor-2")
        | observed_at: "2026-05-17T10:05:00Z",
          body_text: "Harbor Ferry service is halted for current crossings are evening"
      },
      %{
        base_item("roof-private")
        | source_type: "message",
          observed_at: "2026-05-17T10:10:00Z",
          body_text: "Roof Board quoted amount is 14800 deadline is friday",
          acl: %{"privacy" => "private", "participants" => ["flynn"]}
      }
    ]
  end

  defp repetition_items do
    [
      %{
        base_item("harbor-repeat-1")
        | body_text: "Harbor Ferry service is halted for current route is harbor"
      },
      %{
        base_item("harbor-repeat-2")
        | observed_at: "2026-05-17T10:05:00Z",
          body_text: "Harbor Ferry service is halted for current route is harbor"
      }
    ]
  end

  defp polluted_hint_items do
    shared =
      "analysis report update officials readers today market international security statement details"

    [
      %{
        base_item("trump-israel-newsweek")
        | title: "Trump says Israel would not exist without him - Newsweek",
          body_text:
            "Trump Israel Newsweek #{shared} election diplomacy quote campaign government regional alliance"
      },
      %{
        base_item("forgotlings-ps5")
        | observed_at: "2026-05-17T10:05:00Z",
          title: "Forgotlings for PS5 launches next month",
          body_text:
            "Forgotlings PS5 game launches next month developer trailer console preorder platform release"
      },
      %{
        base_item("forgotlings-ps5-update")
        | observed_at: "2026-05-17T10:10:00Z",
          title: "Forgotlings PS5 update adds preorder details",
          body_text:
            "Trump Israel Newsweek election diplomacy quote campaign government regional alliance #{shared} preorder details"
      }
    ]
  end

  defp polluted_hint_story_agent(%{role: :story_identity}, packet, _ctx) do
    story_key =
      case packet.external_id do
        "trump-israel-newsweek" -> "trump-israel-newsweek"
        "forgotlings-ps5" -> "forgotlings-ps5-launch"
        "forgotlings-ps5-update" -> "forgotlings-ps5-launch"
      end

    classification =
      if packet.external_id == "forgotlings-ps5-update",
        do: "substantive_update",
        else: "new_story"

    %{
      output: %{
        "story_key" => story_key,
        "classification" => classification,
        "confidence" => 0.82,
        "rationale" => "agent selected #{story_key} from the bounded packet"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "live_model_inference",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp polluted_hint_story_agent(%{role: :meaning_update}, packet, _ctx) do
    story_key = packet.story_identity.story_key

    %{
      output: %{
        "story_key" => story_key,
        "operation_family" => "commit_story_meaning",
        "classification" => packet.story_identity.classification,
        "changed_facts" => %{"source" => packet.external_id},
        "confidence" => 0.79,
        "rationale" => "agent selected #{story_key}; hint is context only"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "live_model_inference",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp polluted_hint_story_agent(%{role: :story_synthesis}, packet, _ctx) do
    %{
      output: %{
        "status" => "complete",
        "title" => %{
          "text" => packet.committed_story_state.title,
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "deck" => %{
          "text" => "Deck for #{packet.story_key}",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "summary" => %{
          "text" => "Summary for #{packet.story_key}",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "key_claims" => [
          %{
            "claim_ref" => "claim:#{packet.external_id}",
            "text" => packet.snippet,
            "status" => "current",
            "materiality" => "material",
            "evidence_refs" => packet.evidence_refs,
            "conflict_refs" => [],
            "uncertainty" => %{"state" => "known", "reason" => nil},
            "appears_in_current_card" => true
          }
        ],
        "source_coverage" => [
          %{
            "source_ref" => packet.source_ref,
            "materiality" => "material",
            "source_posture" => %{"state" => "complete", "value" => "reported"},
            "contribution_reason" => %{
              "text" => "Packet-grounded source for this story.",
              "state" => "complete",
              "provenance_refs" => packet.evidence_refs
            },
            "source_weight" => %{"state" => "complete", "value" => 1.0}
          }
        ],
        "field_completeness" => %{},
        "topic_salience" => %{
          "durable_topic_nodes" => %{"state" => "refused", "reason" => "test_stub"}
        },
        "changed_field_keys" => ["deck", "summary", "source_coverage", "key_claims"],
        "change_summary" => %{
          "text" => "Story card synthesized by regression test stub.",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        }
      },
      model: "stub-story-agent",
      model_route: "test://story-synthesis",
      producer_kind: "live_model_inference",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-synthesis",
      duration_ms: 1
    }
  end
end
