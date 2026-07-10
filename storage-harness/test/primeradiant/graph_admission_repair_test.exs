defmodule Primeradiant.GraphAdmissionRepairTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Admission

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    GraphAdmissionRepair,
    Input,
    LiveStoryAgentLoop,
    State
  }

  @tenant "00000000-0000-0000-0000-000000001649"

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-t1649-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    db_path = Path.join(root, "snapshot.sqlite3")
    seed_polluted_snapshot!(db_path)
    %{root: root, db_path: db_path}
  end

  test "dry-run is deterministic, read-only, and preserves every historical ID", %{
    db_path: db_path
  } do
    before_hash = GraphAdmissionRepair.file_hash!(db_path)
    first = plan(db_path)
    second = plan(db_path)

    assert first == second
    assert first["plan_hash"] == second["plan_hash"]
    assert first["writes_attempted"] == false
    assert GraphAdmissionRepair.file_hash!(db_path) == before_hash

    assert [story] = first["stories"]
    assert story["story_key"] == "new-story"
    assert length(story["story_event_ids"]) == 1
    assert length(story["edge_ids"]) == 1
    assert length(story["proposal_ids"]) == 1
    assert length(story["proposal_op_ids"]) == 1
    assert length(story["graph_commit_ids"]) == 1
    assert length(story["agent_run_ids"]) >= 2
    assert length(story["evidence_ref_ids"]) > 0
    assert story["input_ids"] != []
    assert story["source_refs"] != []
  end

  test "apply refuses absent, mismatched, and stale exact approval before mutation", %{
    root: root,
    db_path: db_path
  } do
    plan = plan(db_path)
    plan_path = write_plan!(root, plan)
    before_hash = GraphAdmissionRepair.file_hash!(db_path)

    assert_raise KeyError, fn ->
      GraphAdmissionRepair.apply!(
        soup_db: db_path,
        plan: plan_path,
        approval_evidence: "approval:T1649:test",
        actor: "operator"
      )
    end

    assert_raise ArgumentError, ~r/approved plan hash mismatch/, fn ->
      apply_plan!(db_path, plan_path, String.duplicate("0", 64))
    end

    assert GraphAdmissionRepair.file_hash!(db_path) == before_hash
    assert DurableSoupDb.load_tenant(db_path, @tenant).repair_runs == []

    System.cmd("sqlite3", [
      db_path,
      "INSERT INTO replay_runs VALUES ('stale', '#{@tenant}', 'test', 'test', 0, 'read_only', '2026-07-10T00:00:00Z');"
    ])

    assert_raise ArgumentError, ~r/snapshot hash mismatch/, fn ->
      apply_plan!(db_path, plan_path, plan["plan_hash"])
    end
  end

  test "approved apply quarantines, replays through the membrane, records provenance, and validates for T1325",
       %{
         root: root,
         db_path: db_path
       } do
    plan = plan(db_path)
    plan_path = write_plan!(root, plan)
    before = DurableSoupDb.load_tenant(db_path, @tenant)
    before_history = history_ids(before)

    report = apply_plan!(db_path, plan_path, plan["plan_hash"])
    after_state = DurableSoupDb.load_tenant(db_path, @tenant)

    assert report["plan_hash"] == plan["plan_hash"]
    assert report["mutation_ids"] != []
    assert report["rollback"]["snapshot_hash"] == plan["source"]["snapshot_hash"]
    assert report["validation"]["placeholder_story_count"] == 0
    assert report["validation"]["active_quarantined_exposure_count"] == 0
    assert report["validation"]["replacement_edge_metadata_failure_count"] == 0
    assert report["validation"]["history_ids_preserved"] == true

    assert [run] = after_state.repair_runs
    assert run.status == "succeeded"
    assert run.plan_hash == plan["plan_hash"]
    assert run.snapshot_hash == plan["source"]["snapshot_hash"]
    assert run.approval_evidence == "approval:T1649:test"
    assert run.source_commit == "source-commit-test"
    assert run.actor == "operator"
    assert run.started_at
    assert run.finished_at
    assert Enum.sort(run.mutation_ids) == Enum.sort(report["mutation_ids"])

    assert [quarantine] = after_state.story_quarantines
    assert quarantine.story_id == hd(plan["stories"])["story_id"]
    assert quarantine.original_story_key == "new-story"
    assert quarantine.preserved_ids["story_event_ids"] == hd(plan["stories"])["story_event_ids"]
    assert quarantine.source_refs == hd(plan["stories"])["source_refs"]

    current_history = history_ids(after_state)
    missing_evidence = before_history.evidence_refs -- current_history.evidence_refs

    assert missing_evidence == [],
           inspect(Enum.filter(before.evidence_refs, &(&1.id in missing_evidence)))

    assert_history_preserved(before_history, current_history)
    assert Enum.any?(after_state.stories, &(&1.story_key == "repaired-clinic-event"))

    replacement_edges = Enum.reject(after_state.edges, &(&1.id in before_history.edges))
    assert replacement_edges != []
    assert Enum.all?(replacement_edges, &complete_edge_metadata?/1)
  end

  test "replay refusal is recorded without losing rollback or historical proof", %{
    root: root,
    db_path: db_path
  } do
    plan = plan(db_path)
    plan_path = write_plan!(root, plan)

    report =
      GraphAdmissionRepair.apply!(
        soup_db: db_path,
        plan: plan_path,
        approved_plan_hash: plan["plan_hash"],
        approval_evidence: "approval:T1649:test",
        actor: "operator",
        adapter: &refusing_adapter/3
      )

    assert report["validation"]["replay_refusal_count"] == 1
    assert report["validation"]["history_ids_preserved"] == true
    assert report["rollback"]["strategy"] == "restore_snapshot_or_mark_repair_run_failed"
  end

  test "failed replay durably records failure and snapshot rollback proof without graph mutation",
       %{
         root: root,
         db_path: db_path
       } do
    plan = plan(db_path)
    plan_path = write_plan!(root, plan)
    before = DurableSoupDb.load_tenant(db_path, @tenant)

    assert_raise RuntimeError, "fixture replay failure", fn ->
      GraphAdmissionRepair.apply!(
        soup_db: db_path,
        plan: plan_path,
        approved_plan_hash: plan["plan_hash"],
        approval_evidence: "approval:T1649:test",
        actor: "operator",
        adapter: fn _config, _packet, _ctx -> raise "fixture replay failure" end
      )
    end

    failed = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = failed.repair_runs
    assert run.status == "failed"
    assert run.validation["failure"] == "fixture replay failure"
    assert run.rollback_proof["snapshot_hash"] == plan["source"]["snapshot_hash"]
    assert failed.story_quarantines == []
    assert history_ids(failed) == history_ids(before)
  end

  defp seed_polluted_snapshot!(db_path) do
    observed_at = ~U[2026-07-10 00:00:00.000000Z]

    input =
      ChangesetStore.insert!(Input, %{
        tenant_id: @tenant,
        source_type: "fixture_article",
        external_id: "article-1649",
        observed_at: observed_at,
        title: "Clinic event",
        body_text: "Clinic event evidence with a named venue and bounded source facts.",
        object_uri: "fixture://article-1649",
        content_sha256: ChangesetStore.hash("article-1649"),
        acl: %{"privacy" => "public"},
        normalized: %{"source_ref" => "source:article-1649"},
        facts: %{},
        background: %{},
        questions: %{},
        colors: [],
        topic_tokens: []
      })

    state =
      State.new(tenant_id: @tenant)
      |> State.append(:inputs, input)
      |> State.put_source_id(:input, "#{input.source_type}:#{input.external_id}", input.id)

    admission = %{
      source_ref: Admission.input_ref(input),
      source_type: input.source_type,
      external_id: input.external_id,
      observed_at: input.observed_at,
      content_sha256: input.content_sha256,
      content_span_refs: [],
      evidence_refs: ["evidence:article-1649"],
      source_provenance: %{"source_ref" => Admission.input_ref(input)},
      visibility: input.acl,
      normalized_evidence: %{title: input.title}
    }

    {state, _report} =
      LiveStoryAgentLoop.run(state, [admission], "fixture-actor", adapter: &adapter/3)

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "t1649_fixture_snapshot",
      source_db_path: "fixture://t1649",
      source_row_count: 1
    })

    sql = """
    PRAGMA ignore_check_constraints = ON;
    UPDATE stories SET story_key = 'new-story';
    UPDATE soup_nodes SET node_key = 'new-story' WHERE node_type = 'story';
    PRAGMA ignore_check_constraints = OFF;
    """

    {_, 0} = System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true)
  end

  defp plan(db_path) do
    GraphAdmissionRepair.build_plan(
      source_db: "/source/live/soup.sqlite3",
      snapshot: db_path,
      tenant: @tenant,
      source_commit: "source-commit-test"
    )
  end

  defp write_plan!(root, plan) do
    path = Path.join(root, "plan.json")
    File.write!(path, Jason.encode!(plan))
    path
  end

  defp apply_plan!(db_path, plan_path, plan_hash) do
    GraphAdmissionRepair.apply!(
      soup_db: db_path,
      plan: plan_path,
      approved_plan_hash: plan_hash,
      approval_evidence: "approval:T1649:test",
      actor: "operator",
      adapter: &adapter/3
    )
  end

  defp adapter(%{role: :story_identity}, _packet, _ctx),
    do:
      response(%{
        "story_key" => "repaired-clinic-event",
        "classification" => "new_story",
        "confidence" => 0.9,
        "rationale" => "bounded fixture evidence identifies a stable clinic event"
      })

  defp adapter(%{role: :meaning_update}, _packet, _ctx),
    do:
      response(%{
        "story_key" => "repaired-clinic-event",
        "operation_family" => "commit_story_meaning",
        "classification" => "new_story",
        "changed_facts" => %{"event" => "clinic"},
        "confidence" => 0.88,
        "rationale" => "bounded fixture evidence supports the clinic event"
      })

  defp adapter(%{role: :story_synthesis}, packet, _ctx), do: refusal_response(packet)

  defp refusing_adapter(%{role: :meaning_update}, _packet, _ctx),
    do:
      response(%{
        "story_key" => "repaired-clinic-event",
        "operation_family" => "mark_no_op",
        "classification" => "no_op",
        "changed_facts" => %{},
        "confidence" => 0.7,
        "rationale" => "fixture agent refuses a semantic replacement",
        "refusal_reason" => "insufficient_fixture_evidence"
      })

  defp refusing_adapter(config, packet, ctx), do: adapter(config, packet, ctx)

  defp refusal_response(packet) do
    response(%{
      "status" => "refused",
      "title" => %{
        "text" => packet.story_key,
        "state" => "complete",
        "provenance_refs" => packet.evidence_refs
      },
      "refusal_provenance" => %{
        "reason" => "fixture synthesis is not production-trusted",
        "evidence_refs" => packet.evidence_refs,
        "quarantine_recommendation" => "retain bounded fixture refusal"
      }
    })
  end

  defp response(output) do
    %{
      output: output,
      model: "fixture-model",
      model_route: "fixture://t1649",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "fixture-invocation",
      duration_ms: 1
    }
  end

  defp history_ids(state) do
    %{
      story_events: Enum.map(state.story_events, & &1.id),
      edges: Enum.map(state.edges, & &1.id),
      proposals: Enum.map(state.proposals, & &1.id),
      proposal_ops: Enum.map(state.proposal_ops, & &1.id),
      graph_commits: Enum.map(state.graph_commits, & &1.id),
      evidence_refs: Enum.map(state.evidence_refs, & &1.id)
    }
  end

  defp assert_history_preserved(before, current) do
    Enum.each(before, fn {field, ids} ->
      missing = MapSet.difference(MapSet.new(ids), MapSet.new(current[field])) |> MapSet.to_list()

      assert MapSet.subset?(MapSet.new(ids), MapSet.new(current[field])),
             "historical #{field} IDs were not preserved: #{inspect(missing)}"
    end)
  end

  defp complete_edge_metadata?(edge) do
    edge.attrs["edge_contract"] == "article_story_contribution" and
      Enum.all?(
        ~w(link_basis contribution_type source_ref evidence_refs agent_run_id agent_prompt_version agent_output_hash packet_hash correlation_id),
        fn key ->
          edge.attrs[key] not in [nil, "", []]
        end
      )
  end
end
