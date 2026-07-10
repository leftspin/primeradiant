defmodule Primeradiant.StorageHarness.GraphAdmissionRepair do
  @moduledoc false

  alias Primeradiant.Ingestion.Admission

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    LiveStoryAgentLoop,
    RepairRun,
    State,
    StoryQuarantine
  }

  alias Primeradiant.Soup

  @placeholder_keys ~w(new-story new_story newstory story news-story)
  @history_fields ~w(story_events edges proposals proposal_ops graph_commits evidence_refs)a
  @mutation_fields ~w(repair_runs story_quarantines agent_runs proposals proposal_ops proposal_decisions graph_commits stories soup_nodes edges story_fact_versions story_events story_card_versions story_source_coverage story_key_claims story_card_change_sets evidence_refs)a

  def build_plan(opts) do
    snapshot_path = Keyword.fetch!(opts, :snapshot)
    tenant_id = Keyword.fetch!(opts, :tenant)
    source_db_path = require_nonblank!(opts, :source_db)
    source_commit = require_nonblank!(opts, :source_commit)
    ensure_wal_free!(snapshot_path, "snapshot")
    snapshot_hash = file_hash!(snapshot_path)
    snapshot_revision_before = DurableSoupDb.tenant_revision(snapshot_path, tenant_id)
    state = DurableSoupDb.load_tenant(snapshot_path, tenant_id)
    snapshot_revision = DurableSoupDb.tenant_revision(snapshot_path, tenant_id)
    ensure_wal_free!(snapshot_path, "snapshot")

    if file_hash!(snapshot_path) != snapshot_hash or snapshot_revision != snapshot_revision_before do
      raise ArgumentError, "snapshot changed while building the dry-run plan"
    end

    adapter = Keyword.get(opts, :adapter, &LiveStoryAgentLoop.invoke_live_agent/3)

    stories =
      state.stories
      |> Enum.filter(&(String.downcase(&1.story_key) in @placeholder_keys))
      |> Enum.sort_by(& &1.id)

    story_plans = Enum.map(stories, &story_plan(state, &1))

    body = %{
      "contract" => "primeradiant.graph_admission_repair.v1",
      "ticket" => "T1649",
      "mode" => "dry_run",
      "source" => %{
        "db_path" => source_db_path,
        "tenant_id" => tenant_id,
        "source_commit" => source_commit,
        "snapshot_path" => snapshot_path,
        "snapshot_hash" => snapshot_hash,
        "tenant_revision" => snapshot_revision
      },
      "placeholder_keys" => @placeholder_keys,
      "stories" => story_plans,
      "replay_groups" => replay_groups(state, story_plans),
      "counts" => %{
        "polluted_stories" => length(story_plans),
        "replay_inputs" =>
          story_plans |> Enum.flat_map(& &1["input_ids"]) |> Enum.uniq() |> length()
      },
      "writes_attempted" => false,
      "history_policy" => "append_only_no_rewrite_or_delete",
      "rollback" => %{
        "strategy" => "restore_snapshot_or_mark_repair_run_failed",
        "snapshot_path" => snapshot_path,
        "snapshot_hash" => snapshot_hash
      }
    }

    first_proposal = propose_replay(state, body, adapter)
    second_proposal = propose_replay(state, body, adapter)

    if first_proposal != second_proposal do
      raise ArgumentError, "replay proposal is not deterministic for the approved snapshot"
    end

    {proposed_replay, replay_outputs} = first_proposal

    body =
      body
      |> Map.put("proposed_replay", proposed_replay)
      |> Map.put("replay_outputs", replay_outputs)

    Map.put(body, "plan_hash", stable_hash(body))
  end

  def verify_plan_hash!(plan, expected_hash) when is_binary(expected_hash) do
    embedded = Map.fetch!(plan, "plan_hash")
    actual = plan |> Map.delete("plan_hash") |> stable_hash()

    if embedded != expected_hash or actual != expected_hash do
      raise ArgumentError,
            "approved plan hash mismatch: approved #{expected_hash}, embedded #{embedded}, recomputed #{actual}"
    end

    :ok
  end

  def apply!(opts) do
    db_path = Keyword.fetch!(opts, :soup_db)
    plan = opts |> Keyword.fetch!(:plan) |> File.read!() |> Jason.decode!()
    approved_hash = require_nonblank!(opts, :approved_plan_hash)
    approval_evidence = require_nonblank!(opts, :approval_evidence)
    actor = require_nonblank!(opts, :actor)
    verify_plan_hash!(plan, approved_hash)

    snapshot_path = get_in(plan, ["source", "snapshot_path"])

    ensure_distinct_files!(db_path, snapshot_path)

    ensure_wal_free!(snapshot_path, "snapshot")
    ensure_wal_free!(db_path, "apply database")

    if file_hash!(snapshot_path) != get_in(plan, ["source", "snapshot_hash"]) do
      raise ArgumentError, "snapshot hash mismatch: preserved snapshot changed"
    end

    if file_hash!(db_path) != get_in(plan, ["source", "snapshot_hash"]) do
      raise ArgumentError, "snapshot hash mismatch: apply database is not the approved snapshot"
    end

    tenant_id = get_in(plan, ["source", "tenant_id"])
    approved_revision = get_in(plan, ["source", "tenant_revision"])

    if DurableSoupDb.tenant_revision(db_path, tenant_id) != approved_revision do
      raise ArgumentError,
            "snapshot revision mismatch: apply database is not the approved tenant revision"
    end

    before = DurableSoupDb.load_tenant(db_path, tenant_id)

    ensure_wal_free!(db_path, "apply database")

    if file_hash!(db_path) != get_in(plan, ["source", "snapshot_hash"]) do
      raise ArgumentError, "snapshot hash mismatch: apply database changed while loading"
    end

    history_before = history_ids(before)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    first_replay_run_id = Ecto.UUID.generate()
    second_replay_run_id = Ecto.UUID.generate()

    run =
      ChangesetStore.insert!(RepairRun, %{
        tenant_id: tenant_id,
        plan_hash: approved_hash,
        snapshot_hash: get_in(plan, ["source", "snapshot_hash"]),
        snapshot_path: get_in(plan, ["source", "snapshot_path"]),
        source_db_path: get_in(plan, ["source", "db_path"]),
        source_commit: get_in(plan, ["source", "source_commit"]),
        approval_evidence: approval_evidence,
        actor: actor,
        status: "running",
        started_at: now,
        mutation_ids: [first_replay_run_id],
        rollback_proof: plan["rollback"],
        validation: %{}
      })

    running = State.append(before, :repair_runs, run)

    DurableSoupDb.persist_delta!(db_path, before, running, %{
      source_kind: "graph_admission_repair",
      source_db_path: get_in(plan, ["source", "db_path"]),
      source_row_count: 0,
      replay_run_id: first_replay_run_id,
      expected_tenant_revision: approved_revision
    })

    running_revision =
      try do
        invoke_hook(opts, :after_first_persist)

        DurableSoupDb.tenant_revision_after_replay!(
          db_path,
          tenant_id,
          first_replay_run_id,
          approved_revision
        )
      rescue
        error ->
          record_failed_run!(db_path, plan, run, first_replay_run_id, error, "post_first_write")
          reraise error, __STACKTRACE__
      end

    try do
      quarantined = quarantine(running, plan, run, now)
      admissions = admissions(quarantined, plan)

      {adapter, assert_outputs_consumed} = planned_adapter(plan["replay_outputs"])

      {replayed, replay_report} =
        LiveStoryAgentLoop.run(quarantined, admissions, actor, adapter: adapter)

      assert_outputs_consumed.()
      validate_no_node_key_replacement!(before, replayed)

      mutation_ids =
        clean_ids([first_replay_run_id, second_replay_run_id] ++ mutation_ids(before, replayed))

      validation = validation(replayed, plan, history_before, replay_report)

      finished_run =
        ChangesetStore.update!(run, %{
          status: "succeeded",
          finished_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
          mutation_ids: mutation_ids,
          validation: validation
        })

      final = State.replace(replayed, :repair_runs, run.id, finished_run)

      invoke_hook(opts, :before_final_persist)

      DurableSoupDb.persist_delta!(db_path, running, final, %{
        source_kind: "graph_admission_repair",
        source_db_path: get_in(plan, ["source", "db_path"]),
        source_row_count: length(admissions),
        replay_run_id: second_replay_run_id,
        expected_tenant_revision: running_revision
      })

      %{
        "ticket" => "T1649",
        "repair_run_id" => run.id,
        "plan_hash" => approved_hash,
        "mutation_ids" => mutation_ids,
        "replay" => replay_report,
        "validation" => validation,
        "rollback" => plan["rollback"]
      }
    rescue
      error ->
        record_failed_run!(db_path, plan, run, first_replay_run_id, error, "second_phase")
        reraise error, __STACKTRACE__
    end
  end

  def rollback!(opts) do
    db_path = Keyword.fetch!(opts, :soup_db)
    plan = opts |> Keyword.fetch!(:plan) |> File.read!() |> Jason.decode!()
    approved_hash = require_nonblank!(opts, :approved_plan_hash)
    repair_run_id = require_nonblank!(opts, :repair_run_id)
    actor = require_nonblank!(opts, :actor)
    verify_plan_hash!(plan, approved_hash)

    snapshot_path = get_in(plan, ["source", "snapshot_path"])
    ensure_wal_free!(snapshot_path, "snapshot")
    ensure_wal_free!(db_path, "rollback database")

    if file_hash!(snapshot_path) != get_in(plan, ["source", "snapshot_hash"]) do
      raise ArgumentError, "snapshot hash mismatch: rollback snapshot changed"
    end

    tenant_id = get_in(plan, ["source", "tenant_id"])
    expected_revision = DurableSoupDb.tenant_revision(db_path, tenant_id)
    before = DurableSoupDb.load_tenant(db_path, tenant_id)

    run =
      Enum.find(before.repair_runs, &(&1.id == repair_run_id and &1.plan_hash == approved_hash)) ||
        raise ArgumentError, "approved repair run not found"

    if run.status != "succeeded" do
      raise ArgumentError, "only a succeeded repair run can be rolled back"
    end

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    restored =
      Enum.reduce(before.story_quarantines, before, fn quarantine, state ->
        if quarantine.repair_run_id == run.id do
          updated = ChangesetStore.update!(quarantine, %{rollback_status: "restored"})
          State.replace(state, :story_quarantines, quarantine.id, updated)
        else
          state
        end
      end)

    approved_memberships = get_in(plan, ["proposed_replay", "replacement_memberships"]) || []
    applied_memberships = run.validation["replacement_memberships"] || []

    replacement_memberships =
      Enum.map(approved_memberships, fn approved ->
        applied =
          Enum.find(applied_memberships, &(&1["story_key"] == approved["story_key"])) || %{}

        Map.put(approved, "story_id", applied["story_id"] || approved["story_id"])
      end)

    replacement_story_ids = Enum.map(replacement_memberships, & &1["story_id"])

    restored =
      Enum.reduce(replacement_memberships, restored, fn membership, state ->
        story_id = membership["story_id"]
        story = Enum.find(state.stories, &(&1.id == story_id))

        if membership["created_by_repair"] do
          quarantine =
            ChangesetStore.insert!(StoryQuarantine, %{
              tenant_id: tenant_id,
              repair_run_id: run.id,
              story_id: story.id,
              reason: "rollback_replacement_story",
              original_story_key: story.story_key,
              original_story_state: story.state,
              preserved_ids: %{},
              source_refs: [],
              quarantined_at: now,
              rollback_status: "snapshot_available"
            })

          State.append(state, :story_quarantines, quarantine)
        else
          prior_story = ChangesetStore.update!(story, membership["prior_story"])
          State.replace(state, :stories, story.id, prior_story)
        end
      end)

    rollback_replay_run_id = Ecto.UUID.generate()
    rollback_ids = clean_ids([rollback_replay_run_id] ++ mutation_ids(before, restored))

    rolled_back_run =
      ChangesetStore.update!(run, %{
        status: "rolled_back",
        mutation_ids: clean_ids(run.mutation_ids ++ [run.id] ++ rollback_ids),
        rollback_proof:
          Map.merge(run.rollback_proof, %{
            "rolled_back_at" => DateTime.to_iso8601(now),
            "rolled_back_by" => actor,
            "snapshot_verified" => true,
            "strategy" => "restore_original_visibility_quarantine_replacements"
          })
      })

    rolled_back = State.replace(restored, :repair_runs, run.id, rolled_back_run)

    DurableSoupDb.persist_delta!(db_path, before, rolled_back, %{
      source_kind: "graph_admission_repair_rollback",
      source_db_path: get_in(plan, ["source", "db_path"]),
      source_row_count: length(replacement_story_ids),
      replay_run_id: rollback_replay_run_id,
      expected_tenant_revision: expected_revision
    })

    feed = repair_feed(rolled_back)
    original_story_ids = Enum.map(plan["stories"], & &1["story_id"])

    created_replacement_story_ids =
      replacement_memberships
      |> Enum.filter(& &1["created_by_repair"])
      |> Enum.map(& &1["story_id"])

    restored_replacement_story_ids =
      replacement_memberships
      |> Enum.reject(& &1["created_by_repair"])
      |> Enum.map(& &1["story_id"])

    feed_story_ids = Enum.map(feed.items, & &1.story_id)

    %{
      "ticket" => "T1649",
      "repair_run_id" => run.id,
      "status" => "rolled_back",
      "mutation_ids" => rolled_back_run.mutation_ids,
      "rollback" => rolled_back_run.rollback_proof,
      "validation" => %{
        "feed_blockers" => feed.blockers,
        "original_story_ids_visible" =>
          MapSet.subset?(
            MapSet.new(original_story_ids),
            MapSet.new(feed_story_ids)
          ),
        "replacement_story_ids_hidden" =>
          Enum.all?(created_replacement_story_ids, &(&1 not in feed_story_ids)),
        "restored_replacement_story_ids_visible" =>
          Enum.all?(restored_replacement_story_ids, &(&1 in feed_story_ids))
      }
    }
  end

  def file_hash!(path) do
    path
    |> File.stream!([], 1_048_576)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp story_plan(state, story) do
    events = Enum.filter(state.story_events, &(&1.story_id == story.id))
    story_node_ids = state.soup_nodes |> Enum.filter(&(&1.story_id == story.id)) |> ids()

    edges =
      Enum.filter(
        state.edges,
        &(&1.from_node_id in story_node_ids or &1.to_node_id in story_node_ids)
      )

    edge_node_ids =
      edges
      |> Enum.flat_map(&[&1.from_node_id, &1.to_node_id])
      |> Enum.reject(&(&1 in story_node_ids))
      |> clean_ids()

    edge_input_ids =
      state.soup_nodes
      |> Enum.filter(&(&1.id in edge_node_ids and not is_nil(&1.input_id)))
      |> Enum.map(& &1.input_id)

    input_ids = clean_ids(Enum.map(events, & &1.input_id) ++ edge_input_ids)

    proposal_ids =
      (Enum.map(events, & &1.proposal_id) ++ Enum.map(edges, & &1.proposal_id)) |> clean_ids()

    op_ids =
      (Enum.map(events, & &1.proposal_op_id) ++ Enum.map(edges, & &1.proposal_op_id))
      |> clean_ids()

    commit_ids =
      (Enum.map(events, & &1.graph_commit_id) ++ Enum.map(edges, & &1.graph_commit_id))
      |> clean_ids()

    proposal_agent_run_ids =
      state.proposals
      |> Enum.filter(&(&1.id in proposal_ids))
      |> Enum.map(& &1.agent_run_id)

    story_agent_run_ids =
      [
        (story.attrs || %{})["identity_agent_run_id"],
        (story.attrs || %{})["meaning_agent_run_id"]
      ]

    card_agent_run_ids =
      state.story_card_versions
      |> Enum.filter(&(&1.story_id == story.id))
      |> Enum.map(& &1.producing_agent_run_id)

    agent_run_ids = clean_ids(proposal_agent_run_ids ++ story_agent_run_ids ++ card_agent_run_ids)

    evidence =
      Enum.filter(state.evidence_refs, fn row ->
        row.input_id in input_ids or row.proposal_id in proposal_ids or
          row.proposal_op_id in op_ids or row.edge_id in Enum.map(edges, & &1.id) or
          row.subject_id in Enum.map(events, & &1.id) or row.subject_id in commit_ids
      end)

    title_history =
      state.story_card_versions
      |> Enum.filter(&(&1.story_id == story.id))
      |> Enum.sort_by(&{&1.card_version, &1.id})
      |> Enum.map(&get_in(&1.title || %{}, ["text"]))
      |> then(
        &([scalar_title(story.title) | &1]
          |> Enum.reject(fn title -> title in [nil, ""] end)
          |> Enum.uniq())
      )

    %{
      "story_id" => story.id,
      "story_key" => story.story_key,
      "title_history" => title_history,
      "original_state" => story.state,
      "story_event_ids" => ids(events),
      "edge_ids" => ids(edges),
      "proposal_ids" => proposal_ids,
      "proposal_op_ids" => op_ids,
      "graph_commit_ids" => commit_ids,
      "agent_run_ids" => agent_run_ids,
      "evidence_ref_ids" => ids(evidence),
      "input_ids" => input_ids,
      "source_refs" => source_refs(state, input_ids),
      "quarantine" => %{"reason" => "placeholder_durable_story_identity"}
    }
  end

  defp replay_groups(state, story_plans) do
    input_ids = story_plans |> Enum.flat_map(& &1["input_ids"]) |> MapSet.new()

    state.inputs
    |> Enum.filter(&MapSet.member?(input_ids, &1.id))
    |> Enum.group_by(&"#{&1.source_type}:#{&1.external_id}:#{&1.content_sha256}")
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {identity, inputs} ->
      %{
        "source_identity_content_hash" => identity,
        "input_ids" => ids(inputs),
        "source_refs" => inputs |> Enum.map(&Admission.input_ref/1) |> Enum.sort()
      }
    end)
  end

  defp propose_replay(state, plan, adapter) do
    simulated =
      Enum.reduce(plan["stories"], state, fn story, acc ->
        quarantine = %StoryQuarantine{
          id: "dry-run:#{story["story_id"]}",
          tenant_id: state.tenant_id,
          story_id: story["story_id"],
          repair_run_id: "dry-run"
        }

        State.append(acc, :story_quarantines, quarantine)
      end)

    key = {__MODULE__, make_ref()}
    Process.put(key, [])

    capture_adapter = fn config, packet, ctx ->
      response = adapter.(config, packet, ctx)

      captured = %{
        "role" => Atom.to_string(config.role),
        "input_id" => packet.input_id,
        "response" => normalize_response(response)
      }

      Process.put(key, [captured | Process.get(key)])
      response
    end

    {replayed, report} =
      LiveStoryAgentLoop.run(simulated, admissions(simulated, plan), "dry-run-planner",
        adapter: capture_adapter
      )

    outputs = key |> Process.get() |> Enum.reverse()
    Process.delete(key)

    original_edge_ids = MapSet.new(ids(state.edges))
    replacement_story_ids = report.correlation_chains |> Enum.map(& &1.story_id) |> clean_ids()

    replacement_stories =
      replayed.stories
      |> Enum.filter(&(&1.id in replacement_story_ids))
      |> Enum.map(fn story ->
        prior_story = Enum.find(state.stories, &(&1.id == story.id))

        input_ids =
          replayed.story_events
          |> Enum.filter(&(&1.story_id == story.id))
          |> Enum.map(& &1.input_id)
          |> clean_ids()

        %{
          "story_id" => prior_story && story.id,
          "story_key" => story.story_key,
          "input_ids" => input_ids,
          "source_refs" => source_refs(replayed, input_ids),
          "created_by_repair" => is_nil(prior_story),
          "prior_story" => prior_story && story_rollback_snapshot(prior_story)
        }
      end)
      |> Enum.sort_by(& &1["story_key"])

    replacement_edges =
      replayed.edges
      |> Enum.reject(&MapSet.member?(original_edge_ids, &1.id))
      |> Enum.map(fn edge ->
        from_node = Enum.find(replayed.soup_nodes, &(&1.id == edge.from_node_id))
        to_node = Enum.find(replayed.soup_nodes, &(&1.id == edge.to_node_id))
        story = to_node && Enum.find(replayed.stories, &(&1.id == to_node.story_id))

        %{
          "input_id" => from_node && from_node.input_id,
          "story_key" => story && story.story_key,
          "edge_type" => edge.edge_type
        }
      end)
      |> Enum.sort_by(&{&1["input_id"], &1["story_key"], &1["edge_type"]})

    refusals =
      report.correlation_chains
      |> Enum.filter(&(is_binary(&1.refusal_reason) and &1.refusal_reason != ""))
      |> Enum.map(&%{"source_ref" => &1.source_ref, "reason" => &1.refusal_reason})
      |> Enum.sort_by(&{&1["source_ref"], &1["reason"]})

    {%{
       "replacement_memberships" => replacement_stories,
       "replacement_edges" => replacement_edges,
       "refusals" => refusals,
       "old_polluted_memberships" =>
         Enum.map(plan["stories"], &Map.take(&1, ~w(story_id story_key input_ids source_refs)))
     }, outputs}
  end

  defp normalize_response(response) do
    output_hash =
      response[:output_hash] || ChangesetStore.hash(Jason.encode!(response.output))

    %{
      "output" => response.output,
      "model" => response.model,
      "model_route" => response.model_route,
      "producer_kind" => response.producer_kind,
      "decision_source" => response.decision_source,
      "output_hash" => output_hash,
      "invocation_transport_id" => "approved-dry-run:#{output_hash}",
      "duration_ms" => 0
    }
  end

  defp planned_adapter(outputs) do
    key = {__MODULE__, make_ref()}
    Process.put(key, outputs)

    adapter = fn config, packet, _ctx ->
      expected_role = Atom.to_string(config.role)

      case Process.get(key) do
        [%{"role" => role, "input_id" => input_id, "response" => response} | rest]
        when role == expected_role and input_id == packet.input_id ->
          Process.put(key, rest)

          %{
            output: response["output"],
            model: response["model"],
            model_route: response["model_route"],
            producer_kind: response["producer_kind"],
            decision_source: response["decision_source"],
            output_hash: response["output_hash"],
            invocation_transport_id: response["invocation_transport_id"],
            duration_ms: response["duration_ms"]
          }

        _ ->
          raise ArgumentError,
                "approved replay output sequence does not match #{config.role} for #{packet.input_id}"
      end
    end

    assert_consumed = fn ->
      case Process.get(key) do
        [] ->
          Process.delete(key)

        remaining ->
          raise ArgumentError,
                "approved replay output sequence has #{length(remaining)} unused outputs"
      end
    end

    {adapter, assert_consumed}
  end

  defp quarantine(state, plan, run, now) do
    Enum.reduce(plan["stories"], state, fn item, acc ->
      story = Enum.find(acc.stories, &(&1.id == item["story_id"]))

      quarantine =
        ChangesetStore.insert!(StoryQuarantine, %{
          tenant_id: acc.tenant_id,
          repair_run_id: run.id,
          story_id: story.id,
          reason: get_in(item, ["quarantine", "reason"]),
          original_story_key: story.story_key,
          original_story_state: story.state,
          preserved_ids:
            Map.take(
              item,
              ~w(story_event_ids edge_ids proposal_ids proposal_op_ids graph_commit_ids agent_run_ids evidence_ref_ids input_ids)
            ),
          source_refs: item["source_refs"],
          quarantined_at: now,
          rollback_status: "snapshot_available"
        })

      State.append(acc, :story_quarantines, quarantine)
    end)
  end

  defp admissions(state, plan) do
    evidence_by_input =
      state.evidence_refs
      |> Enum.group_by(& &1.input_id, & &1.evidence_label)

    plan["replay_groups"]
    |> Enum.flat_map(& &1["input_ids"])
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(fn input_id ->
      input = Enum.find(state.inputs, &(&1.id == input_id))
      normalized = input.normalized || %{}

      %{
        source_ref: Admission.input_ref(input),
        source_type: input.source_type,
        external_id: input.external_id,
        observed_at: input.observed_at,
        content_sha256: input.content_sha256,
        content_span_refs: normalized["content_span_refs"] || [],
        evidence_refs: Map.get(evidence_by_input, input.id, []) |> Enum.uniq() |> Enum.sort(),
        source_provenance: normalized["source_provenance"] || %{},
        visibility: input.acl,
        normalized_evidence: %{
          title: input.title,
          source_type: input.source_type,
          external_id: input.external_id,
          object_uri: input.object_uri,
          content_hash: input.content_sha256
        }
      }
    end)
  end

  defp validation(state, plan, history_before, replay_report) do
    quarantined_story_ids = Enum.map(plan["stories"], & &1["story_id"])
    replacement_edges = Enum.reject(state.edges, &(&1.id in history_before.edges))

    replacement_story_ids =
      replay_report.correlation_chains |> Enum.map(& &1.story_id) |> clean_ids()

    replacement_stories = Enum.filter(state.stories, &(&1.id in replacement_story_ids))
    feed = repair_feed(state)
    feed_story_ids = Enum.map(feed.items, & &1.story_id)

    replacement_memberships =
      replacement_stories
      |> Enum.map(fn story ->
        input_ids =
          state.story_events
          |> Enum.filter(&(&1.story_id == story.id))
          |> Enum.map(& &1.input_id)
          |> clean_ids()

        %{
          "story_id" => story.id,
          "story_key" => story.story_key,
          "input_ids" => input_ids,
          "source_refs" => source_refs(state, input_ids)
        }
      end)
      |> Enum.sort_by(& &1["story_key"])

    approved_memberships =
      plan["proposed_replay"]["replacement_memberships"]
      |> Enum.map(&Map.take(&1, ~w(story_key input_ids source_refs)))

    actual_memberships =
      Enum.map(replacement_memberships, &Map.take(&1, ~w(story_key input_ids source_refs)))

    %{
      "placeholder_story_count" =>
        Enum.count(
          state.stories,
          &(String.downcase(&1.story_key) in @placeholder_keys and
              &1.id not in quarantined_story_ids)
        ),
      "active_quarantined_exposure_count" =>
        Enum.count(quarantined_story_ids, &(&1 in feed_story_ids)),
      "replacement_placeholder_key_count" =>
        Enum.count(
          state.stories,
          &(String.downcase(&1.story_key) in @placeholder_keys and
              &1.id not in quarantined_story_ids)
        ),
      "replacement_edge_metadata_failure_count" =>
        Enum.count(
          replacement_edges,
          &(get_in(&1.attrs, ["edge_contract"]) == "article_story_contribution" and
              missing_edge_metadata?(&1.attrs))
        ),
      "history_ids_preserved" => history_preserved?(history_before, history_ids(state)),
      "approved_memberships_match" => approved_memberships == actual_memberships,
      "feed_story_ids" => feed_story_ids,
      "feed_blockers" => feed.blockers,
      "quarantined_story_ids" => quarantined_story_ids,
      "replacement_story_ids" => ids(replacement_stories),
      "replacement_edge_ids" => ids(replacement_edges),
      "replacement_memberships" => replacement_memberships,
      "membership_comparison" => %{
        "polluted" => plan["proposed_replay"]["old_polluted_memberships"],
        "approved_replacement" => approved_memberships,
        "applied_replacement" => actual_memberships
      },
      "replay_refusal_count" =>
        Enum.count(
          replay_report.correlation_chains,
          &(is_binary(&1[:refusal_reason]) and &1[:refusal_reason] != "")
        )
    }
  end

  defp missing_edge_metadata?(attrs) do
    Enum.any?(
      ~w(link_basis contribution_type source_ref evidence_refs agent_run_id agent_prompt_version agent_output_hash packet_hash correlation_id),
      fn key ->
        Map.get(attrs, key) in [nil, "", []]
      end
    )
  end

  defp history_ids(state), do: Map.new(@history_fields, &{&1, ids(Map.fetch!(state, &1))})

  defp validate_no_node_key_replacement!(before, replayed) do
    original = Map.new(before.soup_nodes, &{&1.node_key, &1.id})

    case Enum.find(replayed.soup_nodes, fn node ->
           Map.has_key?(original, node.node_key) and original[node.node_key] != node.id
         end) do
      nil ->
        :ok

      node ->
        raise ArgumentError,
              "approved replay would replace historical soup node key #{node.node_key}"
    end
  end

  defp history_preserved?(before_ids, current_ids) do
    Enum.all?(before_ids, fn {field, ids} ->
      MapSet.subset?(MapSet.new(ids), MapSet.new(current_ids[field]))
    end)
  end

  defp mutation_ids(before_state, current_state) do
    Enum.flat_map(@mutation_fields, fn field ->
      old = before_state |> Map.fetch!(field) |> Map.new(&{&1.id, &1})

      current_state
      |> Map.fetch!(field)
      |> Enum.reject(&(Map.get(old, &1.id) == &1))
      |> ids()
    end)
  end

  defp repair_feed(state) do
    Soup.feed(state, %{
      "consumer" => "reporter",
      "projection" => "story_cards",
      "limit" => max(length(state.stories), 1)
    })
  end

  defp ensure_wal_free!(path, label) do
    wal_path = path <> "-wal"

    if File.exists?(wal_path) and File.stat!(wal_path).size > 0 do
      raise ArgumentError, "#{label} must be a transactionally complete SQLite copy without WAL"
    end

    :ok
  end

  defp ensure_distinct_files!(left_path, right_path) do
    left = File.stat!(left_path)
    right = File.stat!(right_path)

    if left.major_device == right.major_device and left.inode == right.inode do
      raise ArgumentError, "apply database must not be the preserved snapshot file"
    end

    :ok
  end

  defp record_failed_run!(db_path, plan, run, first_replay_run_id, error, stage) do
    tenant_id = get_in(plan, ["source", "tenant_id"])
    current_revision = DurableSoupDb.tenant_revision(db_path, tenant_id)
    current = DurableSoupDb.load_tenant(db_path, tenant_id)
    durable_run = Enum.find(current.repair_runs, &(&1.id == run.id)) || run
    failed_replay_run_id = Ecto.UUID.generate()

    failed_run =
      ChangesetStore.update!(durable_run, %{
        status: "failed",
        finished_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
        mutation_ids:
          clean_ids(
            durable_run.mutation_ids ++ [run.id, first_replay_run_id, failed_replay_run_id]
          ),
        validation: %{
          "failure" => Exception.message(error),
          "failure_stage" => stage,
          "terminal_refusal" => true
        }
      })

    failed = State.replace(current, :repair_runs, run.id, failed_run)

    DurableSoupDb.persist_delta!(db_path, current, failed, %{
      source_kind: "graph_admission_repair_failed",
      source_db_path: get_in(plan, ["source", "db_path"]),
      source_row_count: 0,
      replay_run_id: failed_replay_run_id,
      expected_tenant_revision: current_revision
    })
  end

  defp invoke_hook(opts, key) do
    opts |> Keyword.get(key, fn -> :ok end) |> then(& &1.())
  end

  defp source_refs(state, input_ids) do
    state.inputs
    |> Enum.filter(&(&1.id in input_ids))
    |> Enum.map(&Admission.input_ref/1)
    |> Enum.sort()
  end

  defp scalar_title([title]) when is_binary(title), do: title
  defp scalar_title(title) when is_binary(title), do: title
  defp scalar_title(_title), do: nil

  defp story_rollback_snapshot(story) do
    Map.take(story, [
      :story_key,
      :title,
      :state,
      :version,
      :first_observed_at,
      :updated_at_story,
      :last_material_at,
      :structural_facts,
      :background_facts,
      :colors,
      :questions,
      :topic_tokens,
      :attrs
    ])
  end

  defp ids(rows), do: rows |> Enum.map(& &1.id) |> clean_ids()
  defp clean_ids(values), do: values |> Enum.reject(&is_nil/1) |> Enum.uniq() |> Enum.sort()

  defp require_nonblank!(opts, key) do
    case Keyword.fetch!(opts, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "--#{key |> to_string() |> String.replace("_", "-")} is required"
    end
  end

  defp stable_hash(value) do
    value
    |> canonicalize()
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp canonicalize(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _} -> to_string(key) end)
    |> Enum.map(fn {key, nested} -> {key, canonicalize(nested)} end)
    |> Jason.OrderedObject.new()
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value), do: value
end
