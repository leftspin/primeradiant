defmodule Primeradiant.RealIngestionTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StoryIdentity.Candidates
  alias Primeradiant.StorageHarness.{KnowledgeWork, RealIngestion, State}

  @tenant Ecto.UUID.generate()

  test "real admission rejects fixture and trusted story labels" do
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

  test "real-style packs drive seven identity decisions without fixture ids or generic edges" do
    {:ok, state, report} = RealIngestion.ingest_items(real_style_items())

    decisions = Map.new(report.decisions, &{&1.input_ref, &1.decision_type})

    assert decisions["news_article:harbor-1"] == :split
    assert decisions["news_article:harbor-2"] == :attach
    assert decisions["news_article:harbor-part"] == :part_of
    assert decisions["news_article:harbor-conflict"] == :conflict
    assert decisions["news_article:harbor-repeat"] == :no_op
    assert decisions["opinion_column:harbor-color"] == :color
    assert decisions["user_note:harbor-stale"] == :stale
    assert decisions["web_page:thin-low-confidence"] == :abstain

    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))

    assert Enum.all?(
             state.inputs ++ state.proposals ++ state.graph_commits,
             &(&1.tenant_id == @tenant)
           )

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["source_mode"]) == "manual_real_ingest_v1")
           )

    assert Enum.all?(state.edges, &(&1.edge_type != "related"))
    assert Enum.any?(state.proposals, &(&1.status == "needs_more_evidence"))

    committed_rows =
      state.soup_nodes ++
        state.edges ++ state.story_fact_versions ++ state.story_events ++ state.conflicts

    proposal_ids = MapSet.new(Enum.map(state.proposals, & &1.id))
    op_ids = MapSet.new(Enum.map(state.proposal_ops, & &1.id))
    commit_ids = MapSet.new(Enum.map(state.graph_commits, & &1.id))

    assert Enum.all?(committed_rows, fn row ->
             MapSet.member?(proposal_ids, row.proposal_id) and
               MapSet.member?(op_ids, row.proposal_op_id) and
               MapSet.member?(commit_ids, row.graph_commit_id)
           end)

    assert Enum.any?(
             state.conflicts,
             &(&1.fact_key == "service_state" and &1.prior_value == "halted" and
                 &1.incoming_value == "normal")
           )

    assert Enum.any?(state.edges, &(&1.edge_type == "part_of"))
    assert Enum.any?(state.story_events, &(&1.classification == "stale"))
  end

  test "real ingestion rejects mixed tenant batches" do
    [first, second | _] = real_style_items()
    other_tenant = Ecto.UUID.generate()

    assert_raise ArgumentError, "real ingestion batch must use one tenant_id", fn ->
      RealIngestion.ingest_items([first, %{second | tenant_id: other_tenant}])
    end
  end

  test "direct real ingestion and admission enforce tenant boundary" do
    other_tenant = Ecto.UUID.generate()
    state = State.new(tenant_id: @tenant)

    assert_raise ArgumentError, "real source item tenant_id must match ingestion state", fn ->
      RealIngestion.ingest_item(state, %{base_item("tenant-mismatch") | tenant_id: other_tenant})
    end

    {state, first_input, _ref} = Admission.admit_source_item(state, base_item("tenant-scope"))

    {state, second_input, _ref} =
      Admission.admit_source_item(
        %{state | tenant_id: other_tenant},
        %{base_item("tenant-scope") | tenant_id: other_tenant}
      )

    assert first_input.id != second_input.id

    assert Enum.map(state.inputs, & &1.tenant_id) |> Enum.sort() ==
             [@tenant, other_tenant] |> Enum.sort()
  end

  test "private candidate retrieval is ACL-aware" do
    {:ok, state, _report} = RealIngestion.ingest_items(private_items(), "flynn")
    private_input = Enum.find(state.inputs, &(&1.source_type == "message"))
    envelope = Primeradiant.Ingestion.RealNormalizer.normalize(private_input)

    assert Candidates.retrieve(state, envelope, "flynn") != []
    assert Candidates.retrieve(state, envelope, "stranger") == []

    later_private = %{
      base_item("roof-private-3")
      | source_type: "message",
        external_id: "roof-private-3",
        title: "Roof private unauthorized",
        body_text: "Roof Board follow up quoted amount is 15000",
        acl: %{"privacy" => "private", "participants" => ["flynn"]}
    }

    assert_raise ArgumentError, "proposal evidence is not accessible to actor", fn ->
      {:ok, _state, _report} = RealIngestion.ingest_items([later_private], "stranger")
    end
  end

  test "public candidate retrieval does not expose private story metadata" do
    private_secret = %{
      base_item("roof-secret-1")
      | source_type: "message",
        title: "Private roof title secret",
        body_text: "Roof Board quoted amount is 14800 deadline is friday",
        source_actor: %{kind: "person", name: "Flynn", stable_id: "roof-secret"},
        acl: %{"privacy" => "private", "participants" => ["flynn"]}
    }

    public_followup = %{
      base_item("roof-public-1")
      | observed_at: "2026-05-17T10:06:00Z",
        title: "Roof public filing",
        body_text: "Roof Board quoted amount is 14800 deadline is friday speaker is clerk",
        acl: %{"privacy" => "public"}
    }

    {:ok, state, _report} = RealIngestion.ingest_items([private_secret, public_followup], "flynn")
    public_input = Enum.find(state.inputs, &(&1.external_id == "roof-public-1"))
    envelope = Primeradiant.Ingestion.RealNormalizer.normalize(public_input)
    [candidate | _] = Candidates.retrieve(state, envelope, "stranger")

    refute candidate.story.title == "Private roof title secret"
    assert candidate.story.title == "Roof public filing"
    assert candidate.visible_facts != []
    refute Enum.any?(candidate.evidence_refs, &String.contains?(&1, "roof-secret"))
  end

  test "story key collisions are deterministic and non-destructive" do
    {:ok, state, report} = RealIngestion.ingest_items(collision_items())

    split_keys =
      report.decisions
      |> Enum.filter(&(&1.decision_type == :split))
      |> Enum.map(& &1.proposed_story_key)

    assert "summit-status-open" in split_keys
    assert Enum.any?(split_keys, &String.starts_with?(&1, "summit-status-open-chicago"))

    assert Enum.uniq(Enum.map(state.stories, & &1.story_key)) ==
             Enum.map(state.stories, & &1.story_key)

    attach = Enum.find(report.decisions, &(&1.input_ref == "news_article:summit-3"))
    assert attach.decision_type in [:attach, :no_op]
    assert attach.candidate_story_key == "summit-status-open"
  end

  test "conflict scope distinguishes contradiction from update over time and opinion color" do
    {:ok, state, report} = RealIngestion.ingest_items(conflict_scope_items())

    decisions = Map.new(report.decisions, &{&1.input_ref, &1.decision_type})
    assert decisions["news_article:service-contradiction"] == :conflict
    assert decisions["news_article:service-update"] == :attach
    assert decisions["opinion_column:service-opinion"] == :color
    assert decisions["web_page:service-low"] == :abstain

    assert Enum.any?(state.conflicts, &(&1.fact_key == "service_state"))
    refute Enum.any?(state.conflicts, &(&1.incoming_value == "restored"))

    assert Enum.any?(
             state.story_fact_versions,
             &(&1.fact_key == "service_state" and &1.fact_value == "halted" and
                 &1.time_scope == "morning")
           )

    assert Enum.any?(
             state.story_fact_versions,
             &(&1.fact_key == "service_state" and &1.fact_value == "restored" and
                 &1.time_scope == "afternoon")
           )
  end

  test "hard knowledge eval decides identity novelty framing relevance and seen deltas from real evidence" do
    {:ok, state, report} = RealIngestion.ingest_items(hard_knowledge_items())
    decisions = Map.new(report.decisions, &{&1.input_ref, &1})

    assert decisions["news_article:civic-initial"].decision_type == :split
    assert decisions["news_article:civic-near-miss"].decision_type == :split
    assert decisions["news_article:civic-meaningful-repeat"].decision_type == :attach
    assert decisions["news_article:civic-partial-contradiction"].decision_type == :conflict
    assert decisions["opinion_column:civic-spin"].decision_type == :color
    assert decisions["news_article:civic-repeat"].decision_type == :no_op
    assert decisions["user_note:civic-stale"].decision_type == :stale
    assert decisions["user_note:basement-low"].decision_type == :split

    refute decisions["news_article:civic-initial"].proposed_story_key ==
             decisions["news_article:civic-near-miss"].proposed_story_key

    assert Enum.any?(
             state.conflicts,
             &(&1.fact_key == "triage" and &1.prior_value == "open" and
                 &1.incoming_value == "closed")
           )

    refute Enum.any?(state.conflicts, &(&1.fact_key == "hours"))
    assert Enum.any?(state.edges, &(&1.edge_type == "adds_color"))

    repeat_input = Enum.find(state.inputs, &(&1.external_id == "civic-repeat"))
    repeat_event = Enum.find(state.story_events, &(&1.input_id == repeat_input.id))
    repeat_story = Enum.find(state.stories, &(&1.id == repeat_event.story_id))
    material_before_repeat = repeat_story.last_material_at

    assert DateTime.compare(repeat_event.observed_at, material_before_repeat) == :gt
    assert repeat_story.last_material_at == material_before_repeat

    {state, south_followup_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("civic-south-followup")
          | observed_at: "2026-05-17T10:24:00Z",
            title: "Civic Clinic south speaker update",
            body_text:
              "Civic Clinic triage is open for current hours is evening venue is south. speaker is clerk"
        }
      )

    assert south_followup_decision.decision_type == :attach

    assert south_followup_decision.candidate_story_key ==
             decisions["news_article:civic-near-miss"].proposed_story_key

    state =
      KnowledgeWork.attach_watches(state, [
        %{
          watch_key: "watch:flynn:basement-sensor",
          intent: "Track basement sensor battery even if global rank is low",
          priority: 90,
          match_any: ["basement", "battery"]
        }
      ])

    assert Enum.any?(state.edges, &(&1.edge_type == "watch_applies_to"))

    {:ok, state, first_delta} = KnowledgeWork.record_verified_delta(state, "flynn")

    assert first_delta.verified
    assert first_delta.text =~ "what changed for Flynn"
    assert first_delta.text =~ "Civic Clinic"
    assert first_delta.text =~ "partially contradicts"
    assert first_delta.bullets |> hd() |> String.starts_with?("[watch] Basement Sensor")

    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "input"))
    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "claim"))
    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "authored_output"))

    stranger_delta = KnowledgeWork.render_delta(state, "stranger")
    refute Enum.any?(stranger_delta.bullets, &String.starts_with?(&1, "[watch]"))

    {state, repeat_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("civic-after-seen-repeat")
          | observed_at: "2026-05-17T11:00:00Z",
            title: "Civic Clinic repeat after seen",
            body_text: "Civic Clinic triage is open for current hours is evening venue is north"
        }
      )

    {state, spin_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("civic-after-seen-spin")
          | source_type: "opinion_column",
            observed_at: "2026-05-17T11:05:00Z",
            title: "Civic Clinic spin after seen",
            body_text:
              "Opinion column: Civic Clinic leaders are trying to reframe the same triage story."
        }
      )

    {state, civic_update_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("civic-after-seen-update")
          | observed_at: "2026-05-17T11:10:00Z",
            title: "Civic Clinic director update",
            body_text:
              "Civic Clinic triage is open for current hours is evening venue is north. speaker is director"
        }
      )

    {state, basement_update_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("basement-after-seen-update")
          | source_type: "user_note",
            observed_at: "2026-05-17T11:15:00Z",
            title: "Basement Sensor battery deadline",
            body_text:
              "Basement Sensor battery is low for current location is basement deadline is tonight"
        }
      )

    {state, civic_second_update_decision} =
      RealIngestion.ingest_item(
        state,
        %{
          hard_item("civic-after-seen-update-2")
          | observed_at: "2026-05-17T11:20:00Z",
            title: "Civic Clinic deadline update",
            body_text:
              "Civic Clinic triage is open for current hours is evening venue is north. speaker is director. deadline is monday"
        }
      )

    assert repeat_decision.decision_type == :no_op
    assert spin_decision.decision_type == :color
    assert civic_update_decision.decision_type == :attach
    assert basement_update_decision.decision_type == :attach
    assert civic_second_update_decision.decision_type == :attach

    {:ok, state, second_delta} = KnowledgeWork.record_verified_delta(state, "flynn")

    assert second_delta.verified
    assert second_delta.text =~ "speaker=director"
    assert second_delta.text =~ "deadline=monday"
    assert second_delta.text =~ "deadline=tonight"
    refute second_delta.text =~ "civic-after-seen-repeat"
    refute second_delta.text =~ "civic-after-seen-spin"

    assert length(state.authored_outputs) == 2

    assert Enum.all?(
             state.authored_output_units,
             &(&1.evidence_refs != [] and &1.claim_refs != [])
           )

    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))
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
      %{
        base_item("harbor-1")
        | title: "Harbor Ferry halt",
          body_text: "Harbor Ferry service is halted for current route is harbor"
      },
      %{
        base_item("harbor-2")
        | observed_at: "2026-05-17T10:05:00Z",
          title: "Harbor Ferry crews add timing",
          body_text: "Harbor Ferry service is halted for current crossings are evening"
      },
      %{
        base_item("harbor-part")
        | observed_at: "2026-05-17T10:10:00Z",
          title: "Harbor Ferry terminal staffing",
          body_text:
            "Harbor Ferry terminal staffing is short for current part of Harbor Ferry operations"
      },
      %{
        base_item("harbor-conflict")
        | observed_at: "2026-05-17T10:15:00Z",
          title: "Harbor Ferry conflicting update",
          body_text: "Harbor Ferry service is normal for current route is harbor"
      },
      %{
        base_item("harbor-repeat")
        | observed_at: "2026-05-17T10:20:00Z",
          title: "Harbor Ferry repeat",
          body_text: "Harbor Ferry service is halted for current route is harbor"
      },
      %{
        base_item("harbor-color")
        | source_type: "opinion_column",
          observed_at: "2026-05-17T10:25:00Z",
          title: "Harbor Ferry opinion",
          body_text: "Opinion column: Harbor Ferry riders describe the shutdown as avoidable."
      },
      %{
        base_item("harbor-stale")
        | source_type: "user_note",
          observed_at: "2026-05-17T10:30:00Z",
          title: "Harbor Ferry stale check",
          body_text: "Harbor Ferry background check: no new activity today."
      },
      %{
        base_item("thin-low-confidence")
        | source_type: "web_page",
          observed_at: "2026-05-17T10:35:00Z",
          title: "Thin page",
          body_text: "Miscellaneous note without anchors."
      }
    ]
  end

  defp private_items do
    [
      %{
        base_item("roof-private-1")
        | source_type: "message",
          external_id: "roof-private-1",
          title: "Roof private request",
          body_text: "Roof Board quoted amount is 14800 deadline is friday",
          source_actor: %{kind: "person", name: "Flynn", stable_id: "roof-thread"},
          acl: %{"privacy" => "private", "participants" => ["flynn"]}
      },
      %{
        base_item("roof-private-2")
        | source_type: "message",
          external_id: "roof-private-2",
          observed_at: "2026-05-17T10:08:00Z",
          title: "Roof private question",
          body_text: "Roof Board quoted amount is 14800. Who has the board packet?",
          source_actor: %{kind: "person", name: "Flynn", stable_id: "roof-thread"},
          acl: %{"privacy" => "private", "participants" => ["flynn"]}
      }
    ]
  end

  defp collision_items do
    [
      %{
        base_item("summit-1")
        | title: "Summit status",
          body_text: "Summit status is open for current venue is denver"
      },
      %{
        base_item("summit-2")
        | observed_at: "2026-05-17T10:05:00Z",
          title: "Summit status",
          body_text: "Summit status is open for current venue is chicago"
      },
      %{
        base_item("summit-3")
        | observed_at: "2026-05-17T10:10:00Z",
          title: "Summit status followup",
          body_text: "Summit status is open for current venue is denver speaker is mayor"
      }
    ]
  end

  defp conflict_scope_items do
    [
      %{
        base_item("service-base")
        | title: "Bridge service",
          body_text: "Bridge Service service is halted for morning"
      },
      %{
        base_item("service-contradiction")
        | observed_at: "2026-05-17T10:05:00Z",
          title: "Bridge service contradiction",
          body_text: "Bridge Service service is normal for morning"
      },
      %{
        base_item("service-update")
        | observed_at: "2026-05-17T10:10:00Z",
          title: "Bridge service restored",
          body_text: "Bridge Service service is restored for afternoon"
      },
      %{
        base_item("service-opinion")
        | source_type: "opinion_column",
          observed_at: "2026-05-17T10:15:00Z",
          title: "Bridge service opinion",
          body_text: "Opinion column: Bridge Service communications were confusing."
      },
      %{
        base_item("service-low")
        | source_type: "web_page",
          observed_at: "2026-05-17T10:20:00Z",
          title: "Bridge note",
          body_text: "Loose mention without usable claim."
      }
    ]
  end

  defp hard_knowledge_items do
    [
      %{
        hard_item("civic-initial")
        | title: "Civic Clinic north triage",
          body_text: "Civic Clinic triage is open for current hours is evening venue is north"
      },
      %{
        hard_item("civic-near-miss")
        | observed_at: "2026-05-17T10:03:00Z",
          title: "Civic Clinic south triage",
          body_text: "Civic Clinic triage is open for current hours is evening venue is south"
      },
      %{
        hard_item("civic-meaningful-repeat")
        | observed_at: "2026-05-17T10:06:00Z",
          title: "Civic Clinic board packet question",
          body_text:
            "Civic Clinic triage is open for current hours is evening venue is north. Who has the ambulance plan?"
      },
      %{
        hard_item("civic-partial-contradiction")
        | observed_at: "2026-05-17T10:09:00Z",
          title: "Civic Clinic partial contradiction",
          body_text: "Civic Clinic triage is closed for current hours is evening venue is north"
      },
      %{
        hard_item("civic-spin")
        | source_type: "opinion_column",
          observed_at: "2026-05-17T10:12:00Z",
          title: "Civic Clinic framing",
          body_text:
            "Opinion column: Civic Clinic officials are spinning the triage disruption as modernization."
      },
      %{
        hard_item("civic-repeat")
        | observed_at: "2026-05-17T10:15:00Z",
          title: "Civic Clinic repeat",
          body_text: "Civic Clinic triage is open for current hours is evening venue is north"
      },
      %{
        hard_item("civic-stale")
        | source_type: "user_note",
          observed_at: "2026-05-17T10:18:00Z",
          title: "Civic Clinic background check",
          body_text: "Civic Clinic background check: no new activity today."
      },
      %{
        hard_item("basement-low")
        | source_type: "user_note",
          observed_at: "2026-05-17T10:21:00Z",
          title: "Basement Sensor battery",
          body_text: "Basement Sensor battery is low for current location is basement"
      }
    ]
  end

  defp hard_item(external_id) do
    %{
      base_item(external_id)
      | ingestion_run_key: "run-hard-knowledge-eval",
        canonical_uri: "https://knowledge.example.test/#{external_id}",
        source_name: "Knowledge Eval Wire",
        source_actor: %{
          kind: "publisher",
          name: "Knowledge Eval Wire",
          stable_id: "knowledge-wire"
        },
        title: "Knowledge eval #{external_id}",
        body_text: "Civic Clinic triage is open for current hours is evening venue is north"
    }
  end
end
