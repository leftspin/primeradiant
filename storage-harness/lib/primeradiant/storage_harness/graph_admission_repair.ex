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

  @placeholder_keys ~w(new-story new_story newstory story news-story)
  @history_fields ~w(story_events edges proposals proposal_ops graph_commits evidence_refs)a
  @mutation_fields ~w(repair_runs story_quarantines agent_runs proposals proposal_ops proposal_decisions graph_commits stories soup_nodes edges story_fact_versions story_events story_card_versions story_source_coverage story_key_claims story_card_change_sets evidence_refs)a

  def build_plan(opts) do
    snapshot_path = Keyword.fetch!(opts, :snapshot)
    tenant_id = Keyword.fetch!(opts, :tenant)
    source_db_path = require_nonblank!(opts, :source_db)
    source_commit = require_nonblank!(opts, :source_commit)
    snapshot_hash = file_hash!(snapshot_path)
    state = DurableSoupDb.load_tenant(snapshot_path, tenant_id)

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
        "snapshot_hash" => snapshot_hash
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
    adapter = Keyword.get(opts, :adapter, &LiveStoryAgentLoop.invoke_live_agent/3)

    verify_plan_hash!(plan, approved_hash)

    if file_hash!(db_path) != get_in(plan, ["source", "snapshot_hash"]) do
      raise ArgumentError, "snapshot hash mismatch: apply database is not the approved snapshot"
    end

    tenant_id = get_in(plan, ["source", "tenant_id"])
    before = DurableSoupDb.load_tenant(db_path, tenant_id)
    history_before = history_ids(before)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

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
        mutation_ids: [],
        rollback_proof: plan["rollback"],
        validation: %{}
      })

    running = State.append(before, :repair_runs, run)

    DurableSoupDb.persist_delta!(db_path, before, running, %{
      source_kind: "graph_admission_repair",
      source_db_path: get_in(plan, ["source", "db_path"]),
      source_row_count: 0
    })

    try do
      quarantined = quarantine(running, plan, run, now)
      admissions = admissions(quarantined, plan)

      {replayed, replay_report} =
        LiveStoryAgentLoop.run(quarantined, admissions, actor, adapter: adapter)

      mutation_ids = mutation_ids(before, replayed)
      validation = validation(replayed, plan, history_before, replay_report)

      finished_run =
        ChangesetStore.update!(run, %{
          status: "succeeded",
          finished_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
          mutation_ids: mutation_ids,
          validation: validation
        })

      final = State.replace(replayed, :repair_runs, run.id, finished_run)

      DurableSoupDb.persist_delta!(db_path, running, final, %{
        source_kind: "graph_admission_repair",
        source_db_path: get_in(plan, ["source", "db_path"]),
        source_row_count: length(admissions)
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
        failed_run =
          ChangesetStore.update!(run, %{
            status: "failed",
            finished_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
            validation: %{"failure" => Exception.message(error)}
          })

        failed = State.replace(running, :repair_runs, run.id, failed_run)

        DurableSoupDb.persist_delta!(db_path, running, failed, %{
          source_kind: "graph_admission_repair_failed",
          source_db_path: get_in(plan, ["source", "db_path"]),
          source_row_count: 0
        })

        reraise error, __STACKTRACE__
    end
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
    input_ids = events |> Enum.map(& &1.input_id) |> Enum.uniq() |> Enum.sort()
    story_node_ids = state.soup_nodes |> Enum.filter(&(&1.story_id == story.id)) |> ids()
    input_node_ids = state.soup_nodes |> Enum.filter(&(&1.input_id in input_ids)) |> ids()

    edges =
      Enum.filter(
        state.edges,
        &(&1.from_node_id in input_node_ids or &1.to_node_id in story_node_ids)
      )

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

    %{
      "story_id" => story.id,
      "story_key" => story.story_key,
      "title_history" => [story.title],
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

    %{
      "placeholder_story_count" =>
        Enum.count(
          state.stories,
          &(String.downcase(&1.story_key) in @placeholder_keys and
              &1.id not in quarantined_story_ids)
        ),
      "active_quarantined_exposure_count" =>
        Enum.count(quarantined_story_ids, fn story_id ->
          not Enum.any?(state.story_quarantines, &(&1.story_id == story_id))
        end),
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

  defp history_preserved?(before_ids, current_ids) do
    Enum.all?(before_ids, fn {field, ids} ->
      MapSet.subset?(MapSet.new(ids), MapSet.new(current_ids[field]))
    end)
  end

  defp mutation_ids(before_state, current_state) do
    Enum.flat_map(@mutation_fields, fn field ->
      old = before_state |> Map.fetch!(field) |> ids() |> MapSet.new()

      current_state
      |> Map.fetch!(field)
      |> Enum.reject(&MapSet.member?(old, &1.id))
      |> ids()
    end)
  end

  defp source_refs(state, input_ids) do
    state.inputs
    |> Enum.filter(&(&1.id in input_ids))
    |> Enum.map(&Admission.input_ref/1)
    |> Enum.sort()
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

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _} -> to_string(key) end)
    |> Enum.map(fn {key, nested} -> {key, canonicalize(nested)} end)
    |> Jason.OrderedObject.new()
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value), do: value
end
