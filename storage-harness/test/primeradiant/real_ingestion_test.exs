defmodule Primeradiant.RealIngestionTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StorageHarness.{RealIngestion, State}

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
end
