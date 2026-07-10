defmodule Primeradiant.GraphAdmissionRepairTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.Soup

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

    snapshot_path = Path.join(root, "snapshot.sqlite3")
    db_path = Path.join(root, "repair-target.sqlite3")
    seed_polluted_snapshot!(snapshot_path)
    File.cp!(snapshot_path, db_path)
    %{root: root, snapshot_path: snapshot_path, db_path: db_path}
  end

  test "dry-run is deterministic, read-only, and preserves every historical ID", %{
    snapshot_path: snapshot_path
  } do
    before_hash = GraphAdmissionRepair.file_hash!(snapshot_path)
    first = plan(snapshot_path)
    second = plan(snapshot_path)

    assert first == second
    assert first["plan_hash"] == second["plan_hash"]
    assert first["writes_attempted"] == false

    assert first["source"]["tenant_revision"] ==
             DurableSoupDb.tenant_revision(snapshot_path, @tenant)

    assert GraphAdmissionRepair.file_hash!(snapshot_path) == before_hash

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
    assert story["title_history"] == ["Clinic event", "repaired-clinic-event"]
    assert plan_replay_memberships(first) == ["repaired-clinic-event"]
    assert length(first["replay_outputs"]) == 3
  end

  test "discovery includes edge-only input material when no story event remains", %{
    snapshot_path: snapshot_path
  } do
    {_, 0} =
      System.cmd("sqlite3", [snapshot_path, "DELETE FROM story_events;"], stderr_to_stdout: true)

    [story] = plan(snapshot_path)["stories"]
    assert story["story_event_ids"] == []
    assert length(story["edge_ids"]) == 1
    assert length(story["input_ids"]) == 1
    assert length(story["source_refs"]) == 1
    assert length(story["evidence_ref_ids"]) > 0
  end

  test "plan hash binds proposed replacement membership and exact agent outputs", %{
    snapshot_path: snapshot_path
  } do
    approved = plan(snapshot_path)
    different = plan(snapshot_path, &alternate_adapter/3)

    assert approved["plan_hash"] != different["plan_hash"]
    assert plan_replay_memberships(approved) == ["repaired-clinic-event"]
    assert plan_replay_memberships(different) == ["alternate-clinic-event"]
    assert approved["replay_outputs"] != different["replay_outputs"]
  end

  test "dry-run refuses nondeterministic agent proposals for the same snapshot", %{
    snapshot_path: snapshot_path
  } do
    Process.put(:t1649_adapter_calls, 0)

    unstable = fn config, packet, ctx ->
      count = Process.get(:t1649_adapter_calls) + 1
      Process.put(:t1649_adapter_calls, count)

      if count > 3 do
        alternate_adapter(config, packet, ctx)
      else
        adapter(config, packet, ctx)
      end
    end

    assert_raise ArgumentError, ~r/replay proposal is not deterministic/, fn ->
      plan(snapshot_path, unstable)
    end
  end

  test "dry-run refuses a snapshot with uncheckpointed WAL state", %{
    snapshot_path: snapshot_path
  } do
    File.write!(snapshot_path <> "-wal", "uncheckpointed")

    assert_raise ArgumentError, ~r/transactionally complete SQLite copy without WAL/, fn ->
      plan(snapshot_path)
    end
  end

  test "apply refuses absent, mismatched, and stale exact approval before mutation", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path)
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

    assert_raise ArgumentError, ~r/must not be the preserved snapshot file/, fn ->
      apply_plan!(snapshot_path, plan_path, plan["plan_hash"])
    end

    hard_link = Path.join(root, "snapshot-hard-link.sqlite3")
    File.ln!(snapshot_path, hard_link)

    assert_raise ArgumentError, ~r/must not be the preserved snapshot file/, fn ->
      apply_plan!(hard_link, plan_path, plan["plan_hash"])
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
         snapshot_path: snapshot_path,
         db_path: db_path
       } do
    plan = plan(snapshot_path)
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
    assert report["validation"]["approved_memberships_match"] == true
    assert report["validation"]["replacement_story_ids"] != []
    assert report["validation"]["replacement_edge_ids"] != []
    assert report["validation"]["feed_blockers"] == []

    assert report["validation"]["feed_story_ids"] ==
             report["validation"]["replacement_story_ids"]

    assert hd(plan["stories"])["story_id"] not in report["validation"]["feed_story_ids"]

    durable_projection =
      DurableSoupDb.load_soup_feed_projection(db_path, @tenant, %{"limit" => 20})

    assert Enum.any?(
             durable_projection.repair_runs,
             &(&1.id == report["repair_run_id"] and &1.status == "succeeded")
           )

    durable_feed =
      Soup.feed(durable_projection, %{
        "consumer" => "reporter",
        "projection" => "story_cards",
        "limit" => 20
      })

    assert hd(plan["stories"])["story_id"] not in Enum.map(durable_feed.items, & &1.story_id)

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

    replay_run_ids = sqlite_ids(db_path, "replay_runs")
    assert length(replay_run_ids) == 3
    assert Enum.all?(replay_run_ids -- [hd(replay_run_ids)], &(&1 in report["mutation_ids"]))

    assert [quarantine] = after_state.story_quarantines
    assert quarantine.story_id == hd(plan["stories"])["story_id"]
    assert quarantine.original_story_key == "new-story"
    assert quarantine.preserved_ids["story_event_ids"] == hd(plan["stories"])["story_event_ids"]
    assert quarantine.source_refs == hd(plan["stories"])["source_refs"]

    current_history = history_ids(after_state)
    missing_evidence = before_history.evidence_refs -- current_history.evidence_refs

    assert missing_evidence == [],
           inspect(%{
             evidence: Enum.filter(before.evidence_refs, &(&1.id in missing_evidence)),
             nodes:
               Enum.filter(before.soup_nodes, fn node ->
                 Enum.any?(
                   before.evidence_refs,
                   &(&1.id in missing_evidence and &1.subject_id == node.id)
                 )
               end)
           })

    assert_history_preserved(before_history, current_history)
    assert Enum.any?(after_state.stories, &(&1.story_key == "repaired-clinic-event"))

    replacement_edges = Enum.reject(after_state.edges, &(&1.id in before_history.edges))
    assert replacement_edges != []
    assert Enum.all?(replacement_edges, &complete_edge_metadata?/1)
  end

  test "replay refusal is recorded without losing rollback or historical proof", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path, &refusing_adapter/3)
    plan_path = write_plan!(root, plan)

    report =
      GraphAdmissionRepair.apply!(
        soup_db: db_path,
        plan: plan_path,
        approved_plan_hash: plan["plan_hash"],
        approval_evidence: "approval:T1649:test",
        actor: "operator"
      )

    assert report["validation"]["replay_refusal_count"] == 1
    assert report["validation"]["history_ids_preserved"] == true
    assert report["rollback"]["strategy"] == "restore_snapshot_or_mark_repair_run_failed"
  end

  test "failed replay durably records failure and snapshot rollback proof without graph mutation",
       %{
         root: root,
         snapshot_path: snapshot_path,
         db_path: db_path
       } do
    plan = plan(snapshot_path)

    plan =
      plan
      |> Map.update!("replay_outputs", &tl/1)
      |> Map.delete("plan_hash")
      |> then(&Map.put(&1, "plan_hash", test_stable_hash(&1)))

    plan_path = write_plan!(root, plan)
    before = DurableSoupDb.load_tenant(db_path, @tenant)

    assert_raise ArgumentError, ~r/approved replay output sequence does not match/, fn ->
      GraphAdmissionRepair.apply!(
        soup_db: db_path,
        plan: plan_path,
        approved_plan_hash: plan["plan_hash"],
        approval_evidence: "approval:T1649:test",
        actor: "operator"
      )
    end

    failed = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = failed.repair_runs
    assert run.status == "failed"
    assert run.validation["failure"] =~ "approved replay output sequence does not match"
    assert run.rollback_proof["snapshot_hash"] == plan["source"]["snapshot_hash"]
    assert failed.story_quarantines == []
    assert history_ids(failed) == history_ids(before)
    assert run.id in run.mutation_ids
  end

  test "post-first-write concurrency refusal durably terminates the repair run", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path)
    plan_path = write_plan!(root, plan)
    before = DurableSoupDb.load_tenant(db_path, @tenant)

    assert_raise ArgumentError, ~r/tenant revision changed after repair write/, fn ->
      apply_plan!(db_path, plan_path, plan["plan_hash"],
        after_first_persist: fn -> append_concurrent_replay!(db_path, "post-first") end
      )
    end

    failed = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = failed.repair_runs
    assert run.status == "failed"
    assert run.validation["failure_stage"] == "post_first_write"
    assert run.validation["terminal_refusal"] == true
    refute Enum.any?(failed.repair_runs, &(&1.status == "running"))
    assert history_ids(failed) == history_ids(before)
  end

  test "second-phase concurrency refusal durably terminates without graph mutation", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path)
    plan_path = write_plan!(root, plan)
    before = DurableSoupDb.load_tenant(db_path, @tenant)

    assert_raise RuntimeError, ~r/sqlite durable soup operation failed/, fn ->
      apply_plan!(db_path, plan_path, plan["plan_hash"],
        before_final_persist: fn -> append_concurrent_replay!(db_path, "second-phase") end
      )
    end

    failed = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = failed.repair_runs
    assert run.status == "failed"
    assert run.validation["failure_stage"] == "second_phase"
    assert run.validation["terminal_refusal"] == true
    refute Enum.any?(failed.repair_runs, &(&1.status == "running"))
    assert history_ids(failed) == history_ids(before)
  end

  test "same approved plan replay refuses without creating another repair run", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path)
    plan_path = write_plan!(root, plan)
    apply_plan!(db_path, plan_path, plan["plan_hash"])

    assert_raise ArgumentError, ~r/snapshot hash mismatch/, fn ->
      apply_plan!(db_path, plan_path, plan["plan_hash"])
    end

    state = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = state.repair_runs
    assert run.status == "succeeded"
  end

  test "rollback restores original feed visibility and quarantines replacement material", %{
    root: root,
    snapshot_path: snapshot_path,
    db_path: db_path
  } do
    plan = plan(snapshot_path)
    plan_path = write_plan!(root, plan)
    apply_report = apply_plan!(db_path, plan_path, plan["plan_hash"])

    rollback_report =
      GraphAdmissionRepair.rollback!(
        soup_db: db_path,
        plan: plan_path,
        approved_plan_hash: plan["plan_hash"],
        repair_run_id: apply_report["repair_run_id"],
        actor: "rollback-operator"
      )

    state = DurableSoupDb.load_tenant(db_path, @tenant)
    assert [run] = state.repair_runs
    assert run.status == "rolled_back"
    assert run.rollback_proof["snapshot_verified"] == true
    assert run.rollback_proof["rolled_back_by"] == "rollback-operator"
    assert rollback_report["validation"]["feed_blockers"] == []
    assert rollback_report["validation"]["original_story_ids_visible"] == true
    assert rollback_report["validation"]["replacement_story_ids_hidden"] == true
    assert Enum.any?(state.story_quarantines, &(&1.rollback_status == "restored"))
    assert Enum.any?(state.story_quarantines, &(&1.reason == "rollback_replacement_story"))

    durable_projection =
      DurableSoupDb.load_soup_feed_projection(db_path, @tenant, %{"limit" => 20})

    assert Enum.any?(
             durable_projection.repair_runs,
             &(&1.id == apply_report["repair_run_id"] and &1.status == "rolled_back")
           )

    durable_feed =
      Soup.feed(durable_projection, %{
        "consumer" => "reporter",
        "projection" => "story_cards",
        "limit" => 20
      })

    durable_story_ids = Enum.map(durable_feed.items, & &1.story_id)
    assert hd(plan["stories"])["story_id"] in durable_story_ids

    assert Enum.all?(
             apply_report["validation"]["replacement_story_ids"],
             &(&1 not in durable_story_ids)
           )

    input = hd(state.inputs)
    capture_key = {__MODULE__, make_ref()}

    capture_adapter = fn config, packet, ctx ->
      if config.role == :story_identity do
        Process.put(capture_key, packet.visible_story_refs)
        raise "story-agent packet captured"
      end

      adapter(config, packet, ctx)
    end

    assert_raise RuntimeError, "story-agent packet captured", fn ->
      LiveStoryAgentLoop.run(state, [admission_for(input)], "fixture-actor",
        adapter: capture_adapter
      )
    end

    visible_story_keys = capture_key |> Process.get() |> Enum.map(& &1.story_key)
    assert "new-story" in visible_story_keys
    refute "repaired-clinic-event" in visible_story_keys
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

    admission = admission_for(input)

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
    UPDATE soup_nodes SET node_key = replace(node_key, 'repaired-clinic-event', 'new-story');
    PRAGMA ignore_check_constraints = OFF;
    """

    {_, 0} = System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true)
  end

  defp plan(db_path, replay_adapter \\ &adapter/3) do
    GraphAdmissionRepair.build_plan(
      source_db: "/source/live/soup.sqlite3",
      snapshot: db_path,
      tenant: @tenant,
      source_commit: "source-commit-test",
      adapter: replay_adapter
    )
  end

  defp write_plan!(root, plan) do
    path = Path.join(root, "plan.json")
    File.write!(path, Jason.encode!(plan))
    path
  end

  defp apply_plan!(db_path, plan_path, plan_hash, extra_opts \\ []) do
    base_opts = [
      soup_db: db_path,
      plan: plan_path,
      approved_plan_hash: plan_hash,
      approval_evidence: "approval:T1649:test",
      actor: "operator"
    ]

    GraphAdmissionRepair.apply!(Keyword.merge(base_opts, extra_opts))
  end

  defp admission_for(input) do
    %{
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
  end

  defp append_concurrent_replay!(db_path, label) do
    state = DurableSoupDb.load_tenant(db_path, @tenant)

    DurableSoupDb.persist_delta!(db_path, state, state, %{
      source_kind: "t1649_concurrency_fixture",
      source_db_path: "fixture://#{label}",
      source_row_count: 0,
      replay_run_id: Ecto.UUID.generate(),
      expected_tenant_revision: DurableSoupDb.tenant_revision(db_path, @tenant)
    })
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

  defp alternate_adapter(%{role: role} = config, packet, ctx)
       when role in [:story_identity, :meaning_update] do
    response = adapter(config, packet, ctx)
    put_in(response, [:output, "story_key"], "alternate-clinic-event")
  end

  defp alternate_adapter(config, packet, ctx), do: adapter(config, packet, ctx)

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

  defp plan_replay_memberships(plan),
    do: Enum.map(plan["proposed_replay"]["replacement_memberships"], & &1["story_key"])

  defp sqlite_ids(db_path, table) do
    {json, 0} =
      System.cmd("sqlite3", [
        "-json",
        db_path,
        "SELECT id FROM #{table} ORDER BY inserted_at, id;"
      ])

    json |> Jason.decode!() |> Enum.map(& &1["id"])
  end

  defp test_stable_hash(value) do
    value
    |> test_canonicalize()
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp test_canonicalize(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _} -> to_string(key) end)
    |> Enum.map(fn {key, nested} -> {key, test_canonicalize(nested)} end)
    |> Jason.OrderedObject.new()
  end

  defp test_canonicalize(value) when is_list(value), do: Enum.map(value, &test_canonicalize/1)
  defp test_canonicalize(value), do: value
end
