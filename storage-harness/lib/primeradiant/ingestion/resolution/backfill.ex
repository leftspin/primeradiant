defmodule Primeradiant.Ingestion.Resolution.Backfill do
  @moduledoc """
  Finite, approved reprocessing of historical source-resolution material.

  Approval authorization is the operator boundary of the trusted caller, matching the
  GraphAdmissionRepair contract. This module records the caller-supplied actor kind and id
  verbatim; it does not implement an authorization framework.
  """

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.Ingestion.Resolution.{AdmissionHandoff, Case}

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    ResolutionBackfillApplication,
    ResolutionBackfillApproval,
    ResolutionBackfillPlan,
    ResolutionBackfillRun,
    ResolutionCase
  }

  @selection_version "source_evidence_backfill_selection_v1"
  @historical_states ~w(unresolved quarantined refused failed_terminal)
  @unfinished_states ~w(received normalizing resolving interpreting validating retry_scheduled)
  @exclusion_rules [
    "exclude_received_and_mid_pipeline",
    "exclude_retry_scheduled",
    "never_mutate_historical_rows",
    "never_advance_source_cursor_or_close_gap"
  ]

  def build_plan(db_path, selection) when is_map(selection) do
    tenant_id = value(selection, :tenant_id)

    if is_binary(tenant_id) do
      state = DurableSoupDb.load_tenant(db_path, tenant_id)

      with :ok <- validate_explicit_candidates(state, selection) do
        candidates = discover_candidates(state, selection)
        registrations = registrations(db_path, tenant_id, source_keys(candidates))

        body = %{
          "selection" => stringify(selection),
          "selection_version" => @selection_version,
          "candidates" => candidates,
          "source_versions" => source_versions(registrations),
          "policy_snapshots" => policy_snapshots(registrations),
          "estimated_budgets" => estimated_budgets(registrations, candidates),
          "exclusion_rules" => @exclusion_rules
        }

        plan =
          ChangesetStore.insert!(ResolutionBackfillPlan, %{
            tenant_id: tenant_id,
            selection: body["selection"],
            selection_version: @selection_version,
            candidates: candidates,
            source_versions: body["source_versions"],
            policy_snapshots: body["policy_snapshots"],
            estimated_budgets: body["estimated_budgets"],
            exclusion_rules: @exclusion_rules,
            content_hash: SourceRegistry.policy_hash(body)
          })
          |> then(&DurableSoupDb.insert_resolution_backfill_plan!(db_path, &1))

        {:ok, plan}
      end
    else
      {:error, :invalid_backfill_selection}
    end
  end

  def build_plan(_db_path, _selection), do: {:error, :invalid_backfill_selection}

  def approve(db_path, plan_id, plan_hash, actor) do
    with {:ok, plan} <- find_plan(db_path, plan_id),
         {:ok, actor_kind, actor_id} <- actor_parts(actor),
         :ok <- verify_plan_hash(plan, plan_hash) do
      approval =
        ChangesetStore.insert!(ResolutionBackfillApproval, %{
          tenant_id: plan.tenant_id,
          plan_id: plan.id,
          plan_hash: plan_hash,
          actor_kind: actor_kind,
          actor_id: actor_id,
          approved_at: now()
        })
        |> then(&DurableSoupDb.insert_resolution_backfill_approval!(db_path, &1))

      {:ok, approval}
    end
  end

  def apply(db_path, plan_id, opts) do
    run_id = Keyword.get(opts, :run_id, Ecto.UUID.generate())

    with {:ok, plan} <- find_plan(db_path, plan_id),
         :ok <- approved(db_path, plan),
         :ok <- verify_plan_hash(plan, plan.content_hash) do
      case DurableSoupDb.resolution_backfill_run(db_path, plan.tenant_id, run_id) do
        nil -> start_new_run(db_path, plan, run_id, opts)
        run -> resume_existing_run(db_path, plan, run, opts)
      end
    end
  end

  def completion_report(db_path, tenant_id, run_id) do
    case DurableSoupDb.resolution_backfill_run(db_path, tenant_id, run_id) do
      nil -> {:error, :backfill_run_not_found}
      run -> {:ok, report(run)}
    end
  end

  defp start_run(plan, db_path, run_id) do
    ChangesetStore.insert!(ResolutionBackfillRun, %{
      tenant_id: plan.tenant_id,
      run_id: run_id,
      plan_id: plan.id,
      applied_plan_hash: plan.content_hash,
      status: "running",
      counts: empty_counts(length(plan.candidates)),
      dispositions: [],
      duplicate_count: 0,
      residual_proof: %{"zero_pending" => false, "explicit_residuals" => []}
    })
    |> then(&DurableSoupDb.insert_resolution_backfill_run!(db_path, &1))
  end

  defp start_new_run(db_path, plan, run_id, opts) do
    with :ok <- verify_current_configuration(db_path, plan),
         :ok <- verify_historical_candidates(db_path, plan) do
      plan |> start_run(db_path, run_id) |> resume_run(db_path, plan, opts)
    end
  end

  defp resume_existing_run(db_path, plan, run, opts) do
    cond do
      run.plan_id != plan.id or run.applied_plan_hash != plan.content_hash ->
        {:error, :run_plan_mismatch}

      run.status != "running" ->
        {:ok, report(run)}

      true ->
        resume_run(run, db_path, plan, opts)
    end
  end

  defp resume_run(run, db_path, plan, opts) do
    existing_by_candidate =
      db_path
      |> DurableSoupDb.resolution_backfill_applications_for_run(plan.tenant_id, run.run_id)
      |> Map.new(&{&1.historical_case_id, &1})

    resumed_by_candidate =
      plan.candidates
      |> Enum.filter(&Map.has_key?(existing_by_candidate, &1["resolution_case_id"]))
      |> Map.new(fn candidate ->
        application = existing_by_candidate[candidate["resolution_case_id"]]

        {candidate["resolution_case_id"],
         resume_application(db_path, plan, candidate, application, opts)}
      end)

    {dispositions, duplicate_count} =
      Enum.map_reduce(plan.candidates, 0, fn candidate, duplicates ->
        case resumed_by_candidate[candidate["resolution_case_id"]] do
          disposition when is_map(disposition) ->
            {disposition, duplicates + 1}

          nil ->
            invoke_hook(opts, :before_candidate_claim, candidate)

            case claim_candidate(db_path, plan, run.run_id, candidate, opts) do
              {:duplicate, disposition} -> {disposition, duplicates + 1}
              {:ok, disposition} -> {disposition, duplicates}
              {:stale, disposition} -> {disposition, duplicates}
            end
        end
      end)

    residuals = Enum.filter(dispositions, & &1["residual"])

    finalized =
      run
      |> ChangesetStore.update!(%{
        status: if(residuals == [], do: "completed", else: "completed_with_residuals"),
        counts: counts(plan.candidates, dispositions),
        dispositions: dispositions,
        duplicate_count: duplicate_count,
        residual_proof: %{
          "zero_pending" => residuals == [],
          "explicit_residuals" => Enum.map(residuals, &residual_ref/1)
        },
        completed_at: now()
      })
      |> then(&DurableSoupDb.put_resolution_backfill_run!(db_path, &1))

    {:ok, report(finalized)}
  end

  defp claim_candidate(db_path, plan, run_id, candidate, opts) do
    if source_configuration_current?(db_path, plan, candidate["source_key"]) do
      policy_hash = get_in(plan.source_versions, [candidate["source_key"], "policy_hash"])

      proposed =
        ChangesetStore.insert!(ResolutionBackfillApplication, %{
          tenant_id: plan.tenant_id,
          run_id: run_id,
          plan_id: plan.id,
          raw_envelope_id: candidate["raw_envelope_id"],
          historical_case_id: candidate["resolution_case_id"],
          resolution_case_id: Ecto.UUID.generate(),
          policy_hash: policy_hash,
          idempotency_key: Enum.join([run_id, candidate["raw_envelope_id"], policy_hash], ":")
        })

      case DurableSoupDb.claim_resolution_backfill_application!(db_path, proposed, candidate) do
        {:claimed, application} ->
          ensure_application_case(db_path, plan, candidate, application)
          invoke_hook(opts, :after_application_claim, application)
          {:ok, resume_application(db_path, plan, candidate, application, opts)}

        {:existing, application} ->
          {:duplicate, resume_application(db_path, plan, candidate, application, opts)}

        {:stale, nil} ->
          {:stale, stale_disposition(candidate, :historical_case_changed)}
      end
    else
      {:stale, stale_disposition(candidate, :approved_plan_stale)}
    end
  end

  defp ensure_application_case(db_path, plan, candidate, application) do
    source_key = candidate["source_key"]
    version = plan.source_versions[source_key]
    snapshot = plan.policy_snapshots[source_key]
    case_version = "#{version["policy_version"]}:backfill:#{application.run_id}"

    ChangesetStore.insert!(ResolutionCase, %{
      id: application.resolution_case_id,
      tenant_id: plan.tenant_id,
      raw_envelope_id: candidate["raw_envelope_id"],
      policy_version: case_version,
      state: "received",
      attempt_count: 0,
      config_policy_hash: version["policy_hash"],
      policy_snapshot: %{
        "resolution_policy" => snapshot["resolution_policy"],
        "policy_version" => case_version,
        "policy_hash" => version["policy_hash"],
        "config" => snapshot["config"]
      },
      trace_id: "backfill:#{application.run_id}:#{candidate["raw_envelope_id"]}"
    })
    |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_cases, &1))
  end

  defp resume_application(db_path, plan, candidate, application, opts) do
    resolution_case =
      case DurableSoupDb.resolution_case(
             db_path,
             candidate["tenant_id"],
             application.resolution_case_id
           ) do
        nil -> ensure_application_case(db_path, plan, candidate, application)
        existing -> existing
      end

    if resolution_case.state in @unfinished_states do
      Case.run(db_path, resolution_case.id,
        tenant_id: candidate["tenant_id"],
        at: Keyword.get(opts, :at, now())
      )
    end

    current =
      DurableSoupDb.resolution_case(
        db_path,
        candidate["tenant_id"],
        application.resolution_case_id
      )

    invoke_hook(opts, :after_case_run, current)

    admission_result =
      if current.state == "eligible" and is_nil(current_admission_ref(db_path, current)) do
        AdmissionHandoff.admit(db_path, current.id,
          tenant_id: candidate["tenant_id"],
          actor_id: Keyword.get(opts, :actor_id, "flynn"),
          at: Keyword.get(opts, :at, now())
        )
      else
        :not_needed
      end

    disposition_for(db_path, candidate, current.id, admission_result)
  end

  defp disposition_for(db_path, candidate, case_id, admission_result) do
    resolution_case = DurableSoupDb.resolution_case(db_path, candidate["tenant_id"], case_id)
    outcome = current_outcome(db_path, resolution_case)
    admission_ref = outcome && outcome.admission_material_ref
    admission_error = match?({:error, _}, admission_result)

    residual =
      resolution_case.state in @unfinished_states or
        (resolution_case.state == "eligible" and is_nil(admission_ref))

    %{
      "historical_case_id" => candidate["resolution_case_id"],
      "raw_envelope_id" => candidate["raw_envelope_id"],
      "resolution_case_id" => case_id,
      "outcome_code" => resolution_case.outcome_code,
      "outcome_family" => resolution_case.state,
      "outcome_ref" => outcome && outcome.id,
      "admission_ref" => admission_ref,
      "failure" =>
        if(admission_error,
          do: %{"code" => "admission_failed", "error" => inspect(admission_result)},
          else: nil
        ),
      "residual" => residual
    }
  end

  defp stale_disposition(candidate, reason) do
    %{
      "historical_case_id" => candidate["resolution_case_id"],
      "raw_envelope_id" => candidate["raw_envelope_id"],
      "resolution_case_id" => nil,
      "outcome_code" => "backfill:stale_candidate",
      "outcome_family" => "stale_candidate",
      "outcome_ref" => nil,
      "admission_ref" => nil,
      "failure" => %{"code" => "stale_candidate", "reason" => to_string(reason)},
      "residual" => true
    }
  end

  defp candidate_current(db_path, candidate) do
    case DurableSoupDb.resolution_case(
           db_path,
           candidate["tenant_id"],
           candidate["resolution_case_id"]
         ) do
      %ResolutionCase{} = resolution_case ->
        if resolution_case.raw_envelope_id == candidate["raw_envelope_id"] and
             resolution_case.outcome_code == candidate["historical_outcome_code"] and
             resolution_case.config_policy_hash == candidate["historical_policy_hash"] and
             resolution_case.state == candidate["historical_state"] and
             resolution_case.state in @historical_states,
           do: :ok,
           else: {:error, :historical_case_changed}

      nil ->
        {:error, :historical_case_missing}
    end
  end

  defp current_outcome(db_path, resolution_case) do
    db_path
    |> DurableSoupDb.resolution_outcomes_for_case(resolution_case.tenant_id, resolution_case.id)
    |> Enum.filter(&(&1.outcome_code == resolution_case.outcome_code))
    |> Enum.sort_by(&{&1.inserted_at, &1.id}, :desc)
    |> List.first()
  end

  defp current_admission_ref(db_path, resolution_case) do
    case current_outcome(db_path, resolution_case) do
      nil -> nil
      outcome -> outcome.admission_material_ref
    end
  end

  defp counts(candidates, dispositions) do
    %{
      "planned" => length(candidates),
      "attempted" => Enum.count(dispositions, &is_binary(&1["resolution_case_id"])),
      "eligible" => Enum.count(dispositions, &(&1["outcome_family"] == "eligible")),
      "admitted" => Enum.count(dispositions, &is_binary(&1["admission_ref"])),
      "unresolved" => Enum.count(dispositions, &(&1["outcome_family"] == "unresolved")),
      "quarantined" => Enum.count(dispositions, &(&1["outcome_family"] == "quarantined")),
      "failed" => Enum.count(dispositions, &(&1["outcome_family"] == "failed_terminal"))
    }
  end

  defp empty_counts(planned), do: counts(List.duplicate(nil, planned), [])
  defp residual_ref(disposition), do: disposition["historical_case_id"]

  defp discover_candidates(state, selection) do
    raw_by_id = Map.new(state.raw_envelopes, &{&1.id, &1})
    explicit_ids = value(selection, :case_ids)
    outcome_codes = value(selection, :outcome_codes)
    source_key = value(selection, :source_key)
    received_after = parse_time(value(selection, :received_after))
    received_before = parse_time(value(selection, :received_before))

    state.resolution_cases
    |> Enum.filter(&(&1.state in @historical_states))
    |> Enum.filter(&(is_nil(explicit_ids) or &1.id in explicit_ids))
    |> Enum.filter(&(is_nil(outcome_codes) or &1.outcome_code in outcome_codes))
    |> Enum.filter(fn resolution_case ->
      raw = raw_by_id[resolution_case.raw_envelope_id]

      (is_nil(source_key) or raw.source_key == source_key) and
        time_matches?(raw.received_at, received_after, received_before)
    end)
    |> Enum.sort_by(
      &{raw_by_id[&1.raw_envelope_id].source_key, raw_by_id[&1.raw_envelope_id].received_at,
       &1.id}
    )
    |> Enum.map(fn resolution_case ->
      raw = raw_by_id[resolution_case.raw_envelope_id]

      %{
        "tenant_id" => resolution_case.tenant_id,
        "source_key" => raw.source_key,
        "raw_envelope_id" => raw.id,
        "resolution_case_id" => resolution_case.id,
        "historical_state" => resolution_case.state,
        "historical_outcome_code" => resolution_case.outcome_code,
        "historical_policy_hash" => resolution_case.config_policy_hash
      }
    end)
  end

  defp validate_explicit_candidates(state, selection) do
    case value(selection, :case_ids) do
      nil ->
        :ok

      ids when is_list(ids) ->
        cases = Enum.filter(state.resolution_cases, &(&1.id in ids))

        if length(cases) == length(Enum.uniq(ids)) and
             Enum.all?(cases, &(&1.state in @historical_states)),
           do: :ok,
           else: {:error, :non_historical_backfill_candidate}

      _ ->
        {:error, :invalid_backfill_selection}
    end
  end

  defp source_keys(candidates), do: candidates |> Enum.map(& &1["source_key"]) |> Enum.uniq()

  defp registrations(db_path, tenant_id, sources),
    do: Enum.map(sources, &DurableSoupDb.source_registration(db_path, tenant_id, &1))

  defp source_versions(registrations) do
    Map.new(registrations, fn registration ->
      {registration.source_key,
       %{
         "adapter_version" => registration.adapter_version,
         "policy_version" => registration.policy_version,
         "policy_hash" => registration.policy_hash,
         "resolver_versions" => Enum.map(registration.config["resolvers"] || [], & &1["version"])
       }}
    end)
  end

  defp policy_snapshots(registrations) do
    Map.new(registrations, fn registration ->
      {registration.source_key,
       %{
         "resolution_policy" => registration.resolution_policy,
         "policy_version" => registration.policy_version,
         "policy_hash" => registration.policy_hash,
         "config" => registration.config
       }}
    end)
  end

  defp estimated_budgets(registrations, candidates) do
    frequencies = Enum.frequencies_by(candidates, & &1["source_key"])

    Map.new(
      registrations,
      &{&1.source_key,
       %{"candidate_count" => frequencies[&1.source_key] || 0, "per_case" => &1.budgets}}
    )
  end

  defp verify_current_configuration(db_path, plan) do
    sources = Map.keys(plan.source_versions)
    registrations = registrations(db_path, plan.tenant_id, sources)

    if Enum.any?(registrations, &is_nil/1) do
      {:error, :approved_plan_stale}
    else
      current_versions = source_versions(registrations)
      current_snapshots = policy_snapshots(registrations)
      current_budgets = estimated_budgets(registrations, plan.candidates)

      if current_versions == plan.source_versions and current_snapshots == plan.policy_snapshots and
           current_budgets == plan.estimated_budgets,
         do: :ok,
         else: {:error, :approved_plan_stale}
    end
  end

  defp source_configuration_current?(db_path, plan, source_key) do
    registration = DurableSoupDb.source_registration(db_path, plan.tenant_id, source_key)

    if registration do
      source_versions([registration])[source_key] == plan.source_versions[source_key] and
        policy_snapshots([registration])[source_key] == plan.policy_snapshots[source_key] and
        estimated_budgets([registration], plan.candidates)[source_key] ==
          plan.estimated_budgets[source_key]
    else
      false
    end
  end

  defp verify_historical_candidates(db_path, plan) do
    if Enum.all?(plan.candidates, &(candidate_current(db_path, &1) == :ok)),
      do: :ok,
      else: {:error, :non_historical_backfill_candidate}
  end

  defp approved(db_path, plan) do
    if Enum.any?(
         DurableSoupDb.resolution_backfill_approvals_for_plan(db_path, plan.tenant_id, plan.id),
         &(&1.plan_hash == plan.content_hash)
       ),
       do: :ok,
       else: {:error, :backfill_plan_not_approved}
  end

  defp verify_plan_hash(plan, expected_hash) do
    actual = SourceRegistry.policy_hash(plan_body(plan))

    if plan.content_hash == expected_hash and actual == expected_hash,
      do: :ok,
      else: {:error, :backfill_plan_hash_mismatch}
  end

  defp plan_body(plan) do
    %{
      "selection" => plan.selection,
      "selection_version" => plan.selection_version,
      "candidates" => plan.candidates,
      "source_versions" => plan.source_versions,
      "policy_snapshots" => plan.policy_snapshots,
      "estimated_budgets" => plan.estimated_budgets,
      "exclusion_rules" => plan.exclusion_rules
    }
  end

  defp find_plan(db_path, plan_id) do
    tenant_id = DurableSoupDb.backfill_plan_tenant(db_path, plan_id)

    case tenant_id && DurableSoupDb.resolution_backfill_plan(db_path, tenant_id, plan_id) do
      nil -> {:error, :backfill_plan_not_found}
      plan -> {:ok, plan}
    end
  end

  defp actor_parts(actor) when is_map(actor) do
    kind = value(actor, :kind)
    id = value(actor, :id)

    if is_binary(kind) and kind != "" and is_binary(id) and id != "",
      do: {:ok, kind, id},
      else: {:error, :invalid_approval_actor}
  end

  defp actor_parts(_actor), do: {:error, :invalid_approval_actor}

  defp report(run) do
    %{
      run_id: run.run_id,
      plan_id: run.plan_id,
      status: run.status,
      applied_plan_hash: run.applied_plan_hash,
      counts: run.counts,
      dispositions: run.dispositions,
      duplicate_count: run.duplicate_count,
      residual_proof: run.residual_proof,
      completed_at: run.completed_at
    }
  end

  defp invoke_hook(opts, key, value) do
    case Keyword.get(opts, key) do
      fun when is_function(fun, 1) -> fun.(value)
      nil -> :ok
    end
  end

  defp time_matches?(received_at, after_time, before_time),
    do:
      (is_nil(after_time) or DateTime.compare(received_at, after_time) != :lt) and
        (is_nil(before_time) or DateTime.compare(received_at, before_time) != :gt)

  defp parse_time(nil), do: nil
  defp parse_time(%DateTime{} = value), do: value
  defp parse_time(value) when is_binary(value), do: ChangesetStore.iso!(value)

  defp stringify(value) when is_map(value),
    do: Map.new(value, fn {key, nested} -> {to_string(key), stringify(nested)} end)

  defp stringify(value) when is_list(value), do: Enum.map(value, &stringify/1)
  defp stringify(value), do: value
  defp value(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))
  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
