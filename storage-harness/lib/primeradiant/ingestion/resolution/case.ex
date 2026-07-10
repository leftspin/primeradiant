defmodule Primeradiant.Ingestion.Resolution.Case do
  @moduledoc false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.Ingestion.Resolution.{Eligibility, Inference, Normalizer, ResolverPlanner}

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    ResolutionAttempt,
    ResolutionEvidence,
    ResolutionOutcome,
    ResolvedSourceField
  }

  @terminal_states ~w(eligible unresolved quarantined refused failed_terminal)
  @field_kinds ~w(publisher_label publisher_domain public_url explanation)
  @pipeline_version "resolution_pipeline_v1"

  def run(db_path, attrs) when is_map(attrs) do
    run(db_path, value(attrs, :resolution_case_id),
      tenant_id: value(attrs, :tenant_id),
      at: value(attrs, :at, now())
    )
  end

  def run(db_path, resolution_case_id, opts) do
    tenant_id = Keyword.fetch!(opts, :tenant_id)
    now = Keyword.get(opts, :at, now())

    case DurableSoupDb.resolution_case(db_path, tenant_id, resolution_case_id) do
      nil ->
        {:error, :resolution_case_not_found}

      resolution_case ->
        run_loaded(db_path, resolution_case, now)
    end
  rescue
    _ ->
      durable_pipeline_crash(
        db_path,
        resolution_case_id,
        Keyword.fetch!(opts, :tenant_id),
        Keyword.get(opts, :at, now())
      )
  catch
    _, _ ->
      durable_pipeline_crash(
        db_path,
        resolution_case_id,
        Keyword.fetch!(opts, :tenant_id),
        Keyword.get(opts, :at, now())
      )
  end

  defp run_loaded(db_path, %{state: "validating"} = resolution_case, now),
    do: run_validator(db_path, resolution_case, now)

  defp run_loaded(db_path, %{state: state} = resolution_case, _now)
       when state in @terminal_states do
    ensure_terminal_outcome(db_path, resolution_case)
    {:outcome, resolution_case.outcome_code, resolution_case}
  end

  defp run_loaded(db_path, %{state: "retry_scheduled"} = resolution_case, now) do
    if DateTime.compare(now, resolution_case.next_retry_at) == :lt do
      {:outcome, resolution_case.outcome_code, resolution_case}
    else
      prepare_run(db_path, resolution_case, now, :retry)
    end
  end

  defp run_loaded(db_path, %{state: "received"} = resolution_case, now),
    do: prepare_run(db_path, resolution_case, now, :initial)

  defp run_loaded(db_path, %{state: state} = resolution_case, now)
       when state in ~w(normalizing resolving interpreting) do
    raw_envelope =
      DurableSoupDb.raw_envelope(
        db_path,
        resolution_case.tenant_id,
        resolution_case.raw_envelope_id
      )

    registration =
      DurableSoupDb.source_registration(
        db_path,
        resolution_case.tenant_id,
        raw_envelope.source_key
      )

    route = finalize_interrupted_attempt(db_path, resolution_case, now)

    finish(
      db_path,
      registration,
      resolution_case,
      "failed_terminal:interrupted_attempt",
      "interrupted_attempt",
      now,
      route
    )
  end

  defp run_loaded(_db_path, resolution_case, _now),
    do:
      {:outcome, resolution_case.outcome_code || "failed_terminal:invalid_case_state",
       resolution_case}

  defp prepare_run(db_path, resolution_case, now, mode) do
    raw_envelope =
      DurableSoupDb.raw_envelope(
        db_path,
        resolution_case.tenant_id,
        resolution_case.raw_envelope_id
      )

    registration =
      DurableSoupDb.source_registration(
        db_path,
        resolution_case.tenant_id,
        raw_envelope.source_key
      )

    ordinal = resolution_case.attempt_count + 1

    resolution_case =
      update_case(db_path, resolution_case, %{
        attempt_count: ordinal,
        next_retry_at: nil,
        outcome_code: nil
      })

    case mode do
      :initial -> run_adapter(db_path, registration, raw_envelope, resolution_case, ordinal, now)
      :retry -> run_resolvers(db_path, registration, raw_envelope, resolution_case, ordinal, now)
    end
  end

  defp run_adapter(db_path, registration, raw_envelope, resolution_case, ordinal, now) do
    begin_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "adapter",
      to_string(module(registration.adapter_module)),
      registration.adapter_version,
      now
    )

    resolution_case = update_case(db_path, resolution_case, %{state: "normalizing"})
    adapter = module(registration.adapter_module)
    budget = stage_budget(registration.budgets, "adapter")

    ctx = %{
      tenant_id: resolution_case.tenant_id,
      source_key: raw_envelope.source_key,
      resolution_case_id: resolution_case.id,
      policy_hash: registration.policy_hash
    }

    {result, consumed, error_class, ended_at} =
      execute(
        fn -> adapter.to_candidate(raw_envelope, ctx) end,
        cap_budget(budget, registration, resolution_case, now),
        now
      )

    result = enforce_case_budget(result, registration, resolution_case, ended_at)
    error_class = error_class(result) || error_class

    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "adapter",
      to_string(adapter),
      registration.adapter_version,
      %{raw_envelope_digest: raw_envelope.content_digest, context: ctx},
      consumed,
      attempt_outcome(result),
      error_class,
      [],
      now,
      ended_at
    )

    case result do
      {:ok, candidate} ->
        if valid_adapter_candidate(candidate) do
          run_normalizer(
            db_path,
            registration,
            raw_envelope,
            resolution_case,
            candidate,
            ordinal,
            ended_at
          )
        else
          finish(
            db_path,
            registration,
            resolution_case,
            "quarantined:malformed_envelope",
            "malformed_envelope",
            ended_at
          )
        end

      {:refused, reason} ->
        code = adapter_refusal_code(reason)

        finish(
          db_path,
          registration,
          resolution_case,
          code,
          reason,
          now
        )

      {:timeout, _} ->
        schedule_retry(
          db_path,
          registration,
          resolution_case,
          "adapter_timeout",
          ordinal,
          now,
          "adapter"
        )

      {:crash, _} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:adapter_crash",
          "adapter_crash",
          now
        )

      {:budget_exhausted, :total_case_ms} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:budget_exhausted",
          "budget_exhausted:total_case_ms",
          ended_at
        )

      {:budget_exhausted, budget_name} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:budget_exhausted",
          "budget_exhausted:" <> to_string(budget_name),
          ended_at,
          "adapter"
        )

      _ ->
        finish(
          db_path,
          registration,
          resolution_case,
          "quarantined:malformed_envelope",
          "malformed_envelope",
          now
        )
    end
  end

  defp run_normalizer(
         db_path,
         registration,
         raw_envelope,
         resolution_case,
         candidate,
         ordinal,
         started_at
       ) do
    budget = stage_budget(registration.budgets, "normalizer")

    begin_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "normalizer",
      nil,
      "normalizer_v1",
      started_at
    )

    {result, consumed, error_class, ended_at} =
      execute(
        fn -> Normalizer.normalize(resolution_case, raw_envelope, candidate, budget) end,
        cap_budget(budget, registration, resolution_case, started_at),
        started_at
      )

    result = enforce_case_budget(result, registration, resolution_case, ended_at)
    error_class = error_class(result) || error_class

    {evidence_refs, persisted_result} = persist_normalized(db_path, result)

    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "normalizer",
      nil,
      "normalizer_v1",
      candidate,
      consumed,
      attempt_outcome(persisted_result),
      error_class,
      evidence_refs,
      started_at,
      ended_at
    )

    case persisted_result do
      {:ok, _packet} ->
        run_resolvers(db_path, registration, raw_envelope, resolution_case, ordinal, ended_at)

      {:outcome, code} ->
        finish(db_path, registration, resolution_case, code, reason_code(code), ended_at)

      {:timeout, _} ->
        schedule_retry(
          db_path,
          registration,
          resolution_case,
          "normalizer_timeout",
          ordinal,
          ended_at,
          "normalizer"
        )

      {:crash, _} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:normalizer_crash",
          "normalizer_crash",
          ended_at
        )

      {:budget_exhausted, :total_case_ms} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:budget_exhausted",
          "budget_exhausted:total_case_ms",
          ended_at
        )

      {:budget_exhausted, budget_name} ->
        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:budget_exhausted",
          "budget_exhausted:" <> to_string(budget_name),
          ended_at,
          "normalizer"
        )
    end
  end

  defp run_resolvers(db_path, registration, raw_envelope, resolution_case, ordinal, now) do
    evidence =
      DurableSoupDb.resolution_evidence_for_case(
        db_path,
        resolution_case.tenant_id,
        resolution_case.id
      )

    evidence_types = evidence_types(registration, evidence)
    plans = ResolverPlanner.plan(registration, evidence_types)

    if invalid_runtime_policy?(registration, plans) do
      finish(
        db_path,
        registration,
        resolution_case,
        "unresolved:policy_not_registered",
        "policy_not_registered",
        now,
        "resolver_planner"
      )
    else
      Enum.reduce_while(plans, {:ok, evidence, now}, fn plan,
                                                        {:ok, packet_evidence, started_at} ->
        case run_resolver(
               db_path,
               registration,
               raw_envelope,
               resolution_case,
               ordinal,
               plan,
               packet_evidence,
               started_at
             ) do
          {:ok, added, ended_at} -> {:cont, {:ok, packet_evidence ++ added, ended_at}}
          {:halt, response} -> {:halt, {:halt, response}}
        end
      end)
      |> case do
        {:ok, packet_evidence, ended_at} ->
          run_inference(
            db_path,
            registration,
            raw_envelope,
            resolution_case,
            ordinal,
            packet_evidence,
            ended_at
          )

        {:halt, response} ->
          response
      end
    end
  end

  defp run_resolver(
         db_path,
         registration,
         raw_envelope,
         resolution_case,
         ordinal,
         plan,
         packet_evidence,
         started_at
       ) do
    packet = %{
      resolution_case_id: resolution_case.id,
      raw_envelope_id: raw_envelope.id,
      evidence: packet_evidence,
      evidence_types: plan.evidence_types,
      visibility: raw_envelope.visibility
    }

    begin_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "resolver",
      plan.id,
      plan.version,
      started_at
    )

    resolution_case = update_case(db_path, resolution_case, %{state: "resolving"})

    {result, consumed, error_class, ended_at} =
      execute(
        fn -> plan.module.resolve(packet, plan.budget) end,
        cap_budget(plan.budget, registration, resolution_case, started_at),
        started_at
      )

    result = enforce_case_budget(result, registration, resolution_case, ended_at)
    error_class = error_class(result) || error_class

    {result, evidence_refs, added} =
      case result do
        {:ok, returned, declared_consumed} when is_list(returned) and is_map(declared_consumed) ->
          consumed = merge_consumed(consumed, declared_consumed)

          case ResolverPlanner.exhausted?(consumed, plan.budget, length(returned)) do
            nil ->
              case safe_persist_resolver_evidence(
                     db_path,
                     resolution_case,
                     raw_envelope,
                     plan,
                     returned
                   ) do
                {:ok, persisted} ->
                  {{:ok, persisted, consumed}, Enum.map(persisted, & &1.id), persisted}

                {:invalid, persisted} ->
                  {{:invalid, :provenance_validation_failed}, Enum.map(persisted, & &1.id),
                   persisted}

                {:error, reason} ->
                  {{:crash, reason}, [], []}
              end

            exhausted ->
              {{:budget_exhausted, exhausted, consumed}, [], []}
          end

        other ->
          {other, [], []}
      end

    consumed = result_consumed(result, consumed) |> Map.put(:planner_reason, plan.why)

    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      "resolver",
      plan.id,
      plan.version,
      packet,
      consumed,
      attempt_outcome(result),
      error_class,
      evidence_refs,
      started_at,
      ended_at
    )

    case result do
      {:ok, _persisted, _consumed} ->
        SourceRegistry.record_resolution_signal(
          db_path,
          source_scope(registration, plan.id, "closed")
        )

        {:ok, added, ended_at}

      {:timeout, _} ->
        {:halt,
         schedule_retry(
           db_path,
           registration,
           resolution_case,
           "resolver_timeout",
           ordinal,
           ended_at,
           plan.id
         )}

      {:budget_exhausted, budget_name, _} ->
        {:halt,
         finish(
           db_path,
           registration,
           resolution_case,
           "failed_terminal:budget_exhausted",
           "budget_exhausted:" <> to_string(budget_name),
           ended_at,
           plan.id
         )}

      {:budget_exhausted, :total_case_ms} ->
        {:halt,
         finish(
           db_path,
           registration,
           resolution_case,
           "failed_terminal:budget_exhausted",
           "budget_exhausted:total_case_ms",
           ended_at,
           plan.id
         )}

      {:outcome, typed} ->
        {:halt,
         handle_typed_outcome(
           db_path,
           registration,
           resolution_case,
           typed,
           ordinal,
           ended_at,
           plan.id
         )}

      {:invalid, :provenance_validation_failed} ->
        {:halt,
         finish(
           db_path,
           registration,
           resolution_case,
           "quarantined:provenance_validation_failed",
           "provenance_validation_failed",
           ended_at,
           plan.id
         )}

      {:crash, _} ->
        {:halt,
         finish(
           db_path,
           registration,
           resolution_case,
           "failed_terminal:resolver_crash",
           "resolver_crash",
           ended_at,
           plan.id
         )}

      _ ->
        {:halt,
         finish(
           db_path,
           registration,
           resolution_case,
           "failed_terminal:resolver_invalid_result",
           "resolver_invalid_result",
           ended_at,
           plan.id
         )}
    end
  end

  defp run_inference(
         db_path,
         registration,
         raw_envelope,
         resolution_case,
         ordinal,
         evidence,
         started_at
       ) do
    case inference_plan(registration) do
      nil ->
        validating_ready(db_path, registration, resolution_case)

      plan ->
        packet = %{resolution_case_id: resolution_case.id, evidence: evidence, untrusted: true}

        begin_attempt(
          db_path,
          resolution_case,
          raw_envelope,
          ordinal,
          "inference",
          plan.id,
          plan.version,
          started_at
        )

        resolution_case = update_case(db_path, resolution_case, %{state: "interpreting"})

        {result, consumed, error_class, ended_at} =
          execute(
            fn -> plan.module.interpret(packet, plan.budget) end,
            cap_budget(plan.budget, registration, resolution_case, started_at),
            started_at
          )

        {result, consumed} = enforce_inference_budget(result, consumed, plan.budget)

        result = enforce_case_budget(result, registration, resolution_case, ended_at)
        error_class = error_class(result) || error_class

        {result, field_refs} = persist_inference(db_path, resolution_case, evidence, result)

        record_attempt(
          db_path,
          resolution_case,
          raw_envelope,
          ordinal,
          "inference",
          plan.id,
          plan.version,
          packet,
          consumed,
          attempt_outcome(result),
          error_class,
          field_refs,
          started_at,
          ended_at
        )

        case result do
          {:ok, _} ->
            validating_ready(db_path, registration, resolution_case)

          :no_value ->
            validating_ready(db_path, registration, resolution_case)

          {:outcome, typed} ->
            handle_typed_outcome(
              db_path,
              registration,
              resolution_case,
              typed,
              ordinal,
              ended_at,
              plan.id
            )

          {:invalid, _} ->
            finish(
              db_path,
              registration,
              resolution_case,
              "quarantined:provenance_validation_failed",
              "provenance_validation_failed",
              ended_at
            )

          {:timeout, _} ->
            schedule_retry(
              db_path,
              registration,
              resolution_case,
              "inference_timeout",
              ordinal,
              ended_at,
              plan.id
            )

          {:crash, _} ->
            finish(
              db_path,
              registration,
              resolution_case,
              "failed_terminal:inference_crash",
              "inference_crash",
              ended_at
            )

          {:budget_exhausted, :total_case_ms} ->
            finish(
              db_path,
              registration,
              resolution_case,
              "failed_terminal:budget_exhausted",
              "budget_exhausted:total_case_ms",
              ended_at
            )

          {:budget_exhausted, budget_name} ->
            finish(
              db_path,
              registration,
              resolution_case,
              "failed_terminal:budget_exhausted",
              "budget_exhausted:" <> to_string(budget_name),
              ended_at,
              plan.id
            )

          _ ->
            finish(
              db_path,
              registration,
              resolution_case,
              "failed_terminal:inference_invalid_result",
              "inference_invalid_result",
              ended_at,
              plan.id
            )
        end
    end
  end

  defp validating_ready(db_path, registration, resolution_case) do
    resolution_case = update_case(db_path, resolution_case, %{state: "validating"})

    SourceRegistry.record_resolution_signal(
      db_path,
      source_scope(registration, "pipeline", "closed")
    )

    run_validator(db_path, resolution_case, now())
  end

  defp run_validator(db_path, resolution_case, started_at) do
    raw_envelope =
      DurableSoupDb.raw_envelope(
        db_path,
        resolution_case.tenant_id,
        resolution_case.raw_envelope_id
      )

    registration =
      DurableSoupDb.source_registration(
        db_path,
        resolution_case.tenant_id,
        raw_envelope.source_key
      )

    begin_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      max(resolution_case.attempt_count, 1),
      "validator",
      nil,
      Eligibility.validator_version(),
      started_at
    )

    snapshot = resolution_case.policy_snapshot

    policy_registration =
      cond do
        is_map(snapshot) and
          SourceRegistry.policy_hash(value(snapshot, "resolution_policy")) ==
            value(snapshot, "policy_hash") and
          value(snapshot, "policy_hash") == resolution_case.config_policy_hash and
            value(snapshot, "policy_version") == resolution_case.policy_version ->
          %{
            resolution_policy: value(snapshot, "resolution_policy"),
            policy_hash: value(snapshot, "policy_hash")
          }

        is_nil(snapshot) and registration.policy_hash == resolution_case.config_policy_hash ->
          registration

        true ->
          nil
      end

    result =
      try do
        if policy_registration,
          do: Eligibility.evaluate(db_path, resolution_case, policy_registration),
          else: {:non_eligible, "unresolved:policy_not_registered", []}
      rescue
        _ -> {:validator_crash, []}
      catch
        _, _ -> {:validator_crash, []}
      end

    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      max(resolution_case.attempt_count, 1),
      "validator",
      nil,
      Eligibility.validator_version(),
      %{
        policy_hash: resolution_case.config_policy_hash,
        validator_version: Eligibility.validator_version()
      },
      %{},
      validator_attempt_outcome(result),
      nil,
      validator_refs(result),
      started_at,
      started_at
    )

    case result do
      {:eligible, winners} ->
        Eligibility.select!(db_path, winners)
        finish(db_path, registration, resolution_case, "eligible", "eligible", started_at)

      {:non_eligible, code, _winners} ->
        clear_selection(db_path, resolution_case)
        finish(db_path, registration, resolution_case, code, reason_code(code), started_at)

      {:validator_crash, _} ->
        clear_selection(db_path, resolution_case)

        finish(
          db_path,
          registration,
          resolution_case,
          "failed_terminal:validator_crash",
          "validator_crash",
          started_at,
          "validator"
        )
    end
  end

  defp validator_attempt_outcome({:eligible, _}), do: "eligible"
  defp validator_attempt_outcome({:non_eligible, code, _}), do: code
  defp validator_attempt_outcome({:validator_crash, _}), do: "failed_terminal:validator_crash"

  defp validator_refs({_, winners}) when is_list(winners),
    do: Enum.flat_map(winners, & &1.evidence_refs) |> Enum.uniq()

  defp validator_refs({_, _, winners}) when is_list(winners),
    do: Enum.flat_map(winners, & &1.evidence_refs) |> Enum.uniq()

  defp handle_typed_outcome(
         db_path,
         registration,
         resolution_case,
         typed,
         ordinal,
         now,
         route
       ) do
    code = typed_code(typed)

    if String.starts_with?(code, "retry_scheduled:") do
      schedule_retry(
        db_path,
        registration,
        resolution_case,
        reason_code(code),
        ordinal,
        now,
        route
      )
    else
      finish(db_path, registration, resolution_case, code, reason_code(code), now, route)
    end
  end

  defp schedule_retry(
         db_path,
         registration,
         resolution_case,
         reason,
         ordinal,
         now,
         route
       ) do
    max_attempts = integer_budget!(registration.budgets, "max_attempts")
    backoff_ms = integer_budget!(registration.budgets, "retry_backoff_ms")
    total_case_ms = integer_budget!(registration.budgets, "total_case_ms")
    next_retry_at = DateTime.add(now, backoff_ms, :millisecond)
    deadline = DateTime.add(resolution_case.inserted_at, total_case_ms, :millisecond)

    if ordinal >= max_attempts or DateTime.compare(next_retry_at, deadline) == :gt do
      finish(
        db_path,
        registration,
        resolution_case,
        "failed_terminal:attempt_budget_exhausted",
        "attempt_budget_exhausted",
        now
      )
    else
      code = "retry_scheduled:" <> reason_code(reason)

      resolution_case =
        update_case(db_path, resolution_case, %{
          state: "retry_scheduled",
          outcome_code: code,
          next_retry_at: next_retry_at
        })

      persist_outcome(db_path, resolution_case, code, reason, true, ordinal)

      SourceRegistry.record_resolution_signal(
        db_path,
        source_scope(registration, route, "degraded")
      )

      {:outcome, code, resolution_case}
    end
  end

  defp finish(db_path, registration, resolution_case, code, reason, now) do
    finish(db_path, registration, resolution_case, code, reason, now, "pipeline")
  end

  defp finish(db_path, registration, resolution_case, code, reason, now, route) do
    state = code |> String.split(":", parts: 2) |> hd()

    if state != "eligible" do
      clear_selection(db_path, resolution_case)
    end

    resolution_case =
      update_case(db_path, resolution_case, %{
        state: state,
        outcome_code: code,
        next_retry_at: nil
      })

    persist_outcome(db_path, resolution_case, code, reason, false, resolution_case.attempt_count)

    circuit_state = if state == "failed_terminal", do: "resolution_stalled", else: "closed"

    SourceRegistry.record_resolution_signal(
      db_path,
      source_scope(registration, route, circuit_state)
    )

    SourceRegistry.record_resolution_terminal(db_path, %{
      tenant_id: registration.tenant_id,
      source_key: registration.source_key,
      at: now
    })

    {:outcome, code, resolution_case}
  end

  defp persist_normalized(_db_path, {:timeout, _} = result), do: {[], result}
  defp persist_normalized(_db_path, {:crash, _} = result), do: {[], result}
  defp persist_normalized(_db_path, {:outcome, _} = result), do: {[], result}
  defp persist_normalized(_db_path, {:budget_exhausted, _} = result), do: {[], result}

  defp persist_normalized(db_path, {:ok, %{evidence: evidence, fields: fields}} = result) do
    Enum.each(evidence, &DurableSoupDb.insert_resolution_evidence!(db_path, &1))
    Enum.each(fields, &DurableSoupDb.put_resolved_source_field!(db_path, &1))
    {Enum.map(evidence, & &1.id), result}
  rescue
    error -> {[], {:crash, error}}
  end

  defp persist_resolver_evidence(db_path, resolution_case, raw_envelope, plan, returned) do
    returned
    |> Enum.with_index()
    |> Enum.reduce({[], true}, fn {attrs, index}, {acc, valid?} ->
      locator = value(attrs, :locator, %{})
      item_value = value(attrs, :value)

      evidence =
        ChangesetStore.insert!(ResolutionEvidence, %{
          id:
            value(attrs, :id) ||
              deterministic_id([
                resolution_case.id,
                plan.id,
                plan.version,
                index,
                item_value,
                Jason.encode!(locator)
              ]),
          tenant_id: resolution_case.tenant_id,
          resolution_case_id: resolution_case.id,
          kind: value(attrs, :kind),
          value: item_value,
          protected_ref: value(attrs, :protected_ref),
          source: "resolver_response",
          locator: locator,
          span_start: value(attrs, :span_start),
          span_end: value(attrs, :span_end),
          digest:
            value(attrs, :digest) ||
              ChangesetStore.hash(item_value || value(attrs, :protected_ref)),
          retrieved_at: value(attrs, :retrieved_at) || now(),
          visibility: raw_envelope.visibility,
          provenance:
            value(attrs, :provenance, %{})
            |> Map.put("resolver", plan.id)
            |> Map.put("resolver_version", plan.version)
            |> Map.put("planner_reason", plan.why),
          transformation_chain: value(attrs, :transformation_chain, [])
        })

      persisted = DurableSoupDb.insert_resolution_evidence!(db_path, evidence)

      field_valid? =
        persist_direct_resolver_field(db_path, resolution_case, persisted, attrs, plan)

      {[persisted | acc], valid? and field_valid?}
    end)
    |> then(fn {persisted, valid?} -> {Enum.reverse(persisted), valid?} end)
  end

  defp safe_persist_resolver_evidence(db_path, resolution_case, raw_envelope, plan, returned) do
    case persist_resolver_evidence(db_path, resolution_case, raw_envelope, plan, returned) do
      {persisted, true} -> {:ok, persisted}
      {persisted, false} -> {:invalid, persisted}
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp persist_direct_resolver_field(db_path, resolution_case, evidence, attrs, plan) do
    field_name = value(attrs, :field_name)
    normalized_value = value(attrs, :normalized_value)

    cond do
      is_nil(field_name) and is_nil(normalized_value) ->
        true

      field_name in @field_kinds and is_binary(normalized_value) and
          Inference.derived?(field_name, normalized_value, value(attrs, :transform), [evidence]) ->
        field =
          ChangesetStore.insert!(ResolvedSourceField, %{
            id: deterministic_id([resolution_case.id, field_name, normalized_value, evidence.id]),
            tenant_id: resolution_case.tenant_id,
            resolution_case_id: resolution_case.id,
            field_name: field_name,
            normalized_value: normalized_value,
            confidence: value(attrs, :confidence),
            evidence_refs: [evidence.id],
            derivation_evidence_ref: evidence.id,
            resolver_provenance: [%{"resolver" => plan.id, "version" => plan.version}],
            transform: value(attrs, :transform),
            contradiction_status: value(attrs, :contradiction_status, "none"),
            selected: false
          })

        DurableSoupDb.put_resolved_source_field!(db_path, field)
        true

      true ->
        false
    end
  end

  defp persist_inference(_db_path, _case, _evidence, :no_value), do: {:no_value, []}
  defp persist_inference(_db_path, _case, _evidence, {:outcome, _} = result), do: {result, []}
  defp persist_inference(_db_path, _case, _evidence, {:timeout, _} = result), do: {result, []}
  defp persist_inference(_db_path, _case, _evidence, {:crash, _} = result), do: {result, []}

  defp persist_inference(_db_path, _case, _evidence, {:invalid_result, _} = result),
    do: {result, []}

  defp persist_inference(_db_path, _case, _evidence, {:budget_exhausted, _} = result),
    do: {result, []}

  defp persist_inference(db_path, resolution_case, evidence, {:ok, candidates}) do
    case Inference.validate_candidates(candidates, evidence) do
      {:ok, candidates} ->
        fields =
          Enum.map(candidates, fn attrs ->
            field =
              ChangesetStore.insert!(
                ResolvedSourceField,
                Map.merge(attrs, %{
                  id:
                    deterministic_id([
                      resolution_case.id,
                      attrs.field_name,
                      attrs.normalized_value,
                      attrs.evidence_refs |> Enum.sort() |> Enum.join(",")
                    ]),
                  tenant_id: resolution_case.tenant_id,
                  resolution_case_id: resolution_case.id
                })
              )

            DurableSoupDb.put_resolved_source_field!(db_path, field)
          end)

        evidence_refs = candidates |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq()
        {{:ok, fields}, evidence_refs}

      {:error, reason} ->
        {{:invalid, reason}, []}
    end
  rescue
    error -> {{:crash, error}, []}
  end

  defp record_attempt(
         db_path,
         resolution_case,
         raw_envelope,
         ordinal,
         stage,
         resolver,
         version,
         input,
         consumed,
         outcome,
         error_class,
         evidence_refs,
         started_at,
         ended_at
       ) do
    key = attempt_key(raw_envelope, resolution_case, resolver, stage, version, ordinal)

    attempt =
      ChangesetStore.insert!(ResolutionAttempt, %{
        id: deterministic_id([resolution_case.id, key]),
        tenant_id: resolution_case.tenant_id,
        resolution_case_id: resolution_case.id,
        raw_envelope_id: raw_envelope.id,
        raw_envelope_digest: raw_envelope.content_digest,
        attempt_key: key,
        stage: stage,
        resolver: resolver,
        input_hash: input_hash(input),
        attempt_ordinal: ordinal,
        budgets_consumed: consumed,
        outcome: outcome,
        error_class: error_class,
        response_evidence_refs: evidence_refs,
        started_at: started_at,
        ended_at: ended_at
      })

    DurableSoupDb.put_resolution_attempt!(db_path, attempt)
  end

  defp begin_attempt(
         db_path,
         resolution_case,
         raw_envelope,
         ordinal,
         stage,
         resolver,
         version,
         started_at
       ) do
    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      ordinal,
      stage,
      resolver,
      version,
      %{stage: stage},
      %{},
      "running",
      nil,
      [],
      started_at,
      started_at
    )
  end

  defp attempt_key(raw_envelope, resolution_case, resolver, stage, version, ordinal) do
    Enum.join(
      [
        raw_envelope.idempotency_key,
        resolution_case.policy_version,
        Enum.join([resolver || stage, version], "@"),
        ordinal
      ],
      ":"
    )
  end

  defp persist_outcome(db_path, resolution_case, code, reason, retryable, ordinal) do
    outcome =
      ChangesetStore.insert!(ResolutionOutcome, %{
        id: deterministic_id([resolution_case.id, code, ordinal]),
        tenant_id: resolution_case.tenant_id,
        resolution_case_id: resolution_case.id,
        outcome_code: code,
        reason: reason_code(reason),
        retryable: retryable,
        validator_version: outcome_validator_version(code)
      })

    DurableSoupDb.insert_resolution_outcome!(db_path, outcome)
  end

  defp ensure_terminal_outcome(db_path, resolution_case) do
    if resolution_case.state != "eligible" do
      clear_selection(db_path, resolution_case)
    end

    exists? =
      db_path
      |> DurableSoupDb.resolution_outcomes_for_case(
        resolution_case.tenant_id,
        resolution_case.id
      )
      |> Enum.any?(&(&1.outcome_code == resolution_case.outcome_code))

    unless exists? do
      persist_outcome(
        db_path,
        resolution_case,
        resolution_case.outcome_code,
        resolution_case.outcome_code,
        false,
        resolution_case.attempt_count
      )
    end
  end

  defp clear_selection(db_path, resolution_case) do
    db_path
    |> DurableSoupDb.resolved_source_fields_for_case(
      resolution_case.tenant_id,
      resolution_case.id
    )
    |> Enum.filter(& &1.selected)
    |> Enum.each(fn field ->
      field
      |> ChangesetStore.update!(%{selected: false})
      |> then(&DurableSoupDb.put_resolved_source_field!(db_path, &1))
    end)
  end

  defp execute(fun, budget, started_at) do
    time_ms = integer_budget!(budget, "time_ms")
    monotonic_start = System.monotonic_time(:millisecond)
    caller = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = fun.()
        send(caller, {self(), :result, result})
      end)

    watchdog =
      spawn(fn ->
        caller_ref = Process.monitor(caller)

        receive do
          {:DOWN, ^caller_ref, :process, ^caller, _reason} -> Process.exit(pid, :kill)
          :stop -> Process.demonitor(caller_ref, [:flush])
        end
      end)

    result =
      receive do
        {^pid, :result, value} ->
          receive do
            {:DOWN, ^ref, :process, ^pid, _reason} -> value
          end

        {:DOWN, ^ref, :process, ^pid, reason} ->
          {:crash, reason}
      after
        time_ms ->
          Process.exit(pid, :kill)

          receive do
            {:DOWN, ^ref, :process, ^pid, _reason} -> {:timeout, :time_ms}
          end
          |> tap(fn _ ->
            receive do
              {^pid, :result, _value} -> :ok
            after
              0 -> :ok
            end
          end)
      end

    send(watchdog, :stop)

    elapsed = max(System.monotonic_time(:millisecond) - monotonic_start, 0)
    ended_at = DateTime.add(started_at, elapsed, :millisecond)
    error_class = error_class(result)
    {result, %{time_ms: elapsed}, error_class, ended_at}
  end

  defp update_case(db_path, resolution_case, attrs) do
    resolution_case
    |> ChangesetStore.update!(attrs)
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))
  end

  defp inference_plan(registration) do
    case value(registration.config, :inference) do
      nil ->
        nil

      config ->
        %{
          id: value(config, :id),
          module: module(value(config, :module)),
          version: value(config, :version),
          budget: stage_budget(registration.budgets, "inference")
        }
    end
  end

  defp evidence_types(registration, evidence) do
    configured = value(registration.config, :evidence_types, [])
    Enum.uniq(Enum.map(configured, &to_string/1) ++ Enum.map(evidence, & &1.kind))
  end

  defp stage_budget(budgets, stage) do
    value(budgets, stage, %{})
  end

  defp integer_budget!(budgets, key), do: value(budgets, key)

  defp cap_budget(budget, registration, resolution_case, started_at) do
    total_case_ms = integer_budget!(registration.budgets, "total_case_ms")
    elapsed = max(DateTime.diff(started_at, resolution_case.inserted_at, :millisecond), 0)
    remaining = max(total_case_ms - elapsed, 0)
    Map.put(budget, :time_ms, min(integer_budget!(budget, "time_ms"), remaining))
  end

  defp enforce_inference_budget({:ok, candidates, declared}, measured, budget)
       when is_list(candidates) and is_map(declared) do
    token_budget = value(budget, :tokens)
    declared_tokens = value(declared, :tokens)

    if not is_nil(token_budget) and
         (not is_integer(declared_tokens) or declared_tokens < 0) do
      {{:invalid_result, :token_consumption}, measured}
    else
      consumed = merge_consumed(measured, declared)

      cond do
        not is_nil(token_budget) and declared_tokens > token_budget ->
          {{:budget_exhausted, :tokens}, consumed}

        value(consumed, :time_ms, 0) > value(budget, :time_ms) ->
          {{:budget_exhausted, :time_ms}, consumed}

        true ->
          {{:ok, candidates}, consumed}
      end
    end
  end

  defp enforce_inference_budget({:ok, _candidates}, consumed, _budget),
    do: {{:invalid_result, :result_shape}, consumed}

  defp enforce_inference_budget(result, consumed, _budget), do: {result, consumed}

  defp invalid_runtime_policy?(registration, plans) do
    Enum.any?(plans, fn plan ->
      is_nil(plan.id) or is_nil(plan.version) or not is_map(plan.budget) or
        Enum.any?(~w(time_ms requests bytes redirects result_count), fn key ->
          not is_integer(value(plan.budget, key))
        end)
    end) or not is_integer(value(stage_budget(registration.budgets, "adapter"), "time_ms")) or
      not is_integer(value(stage_budget(registration.budgets, "normalizer"), "time_ms"))
  end

  defp valid_adapter_candidate(candidate) when is_map(candidate) do
    is_list(value(candidate, :raw_refs)) and value(candidate, :raw_refs) != [] and
      not is_nil(value(candidate, :declared_identity)) and
      not is_nil(value(candidate, :visibility)) and
      is_map(value(candidate, :adapter_provenance))
  end

  defp valid_adapter_candidate(_), do: false

  defp finalize_interrupted_attempt(db_path, resolution_case, at, synthesize? \\ true) do
    attempt =
      db_path
      |> DurableSoupDb.resolution_attempts_for_case(resolution_case.tenant_id, resolution_case.id)
      |> Enum.filter(&(&1.outcome == "running"))
      |> Enum.max_by(& &1.started_at, fn -> nil end)

    case attempt do
      nil when synthesize? ->
        raw_envelope =
          DurableSoupDb.raw_envelope(
            db_path,
            resolution_case.tenant_id,
            resolution_case.raw_envelope_id
          )

        record_attempt(
          db_path,
          resolution_case,
          raw_envelope,
          max(resolution_case.attempt_count, 1),
          "pipeline",
          nil,
          @pipeline_version,
          %{stage: resolution_case.state},
          %{},
          "failed_terminal:interrupted_attempt",
          "interrupted_attempt",
          [],
          at,
          at
        )

        "pipeline"

      nil ->
        "pipeline"

      attempt ->
        attempt
        |> ChangesetStore.update!(%{
          outcome: "failed_terminal:interrupted_attempt",
          error_class: "interrupted_attempt",
          ended_at: at
        })
        |> then(&DurableSoupDb.put_resolution_attempt!(db_path, &1))

        attempt.resolver || attempt.stage
    end
  end

  defp durable_pipeline_crash(db_path, resolution_case_id, tenant_id, at) do
    resolution_case = DurableSoupDb.resolution_case(db_path, tenant_id, resolution_case_id)
    raw_envelope = DurableSoupDb.raw_envelope(db_path, tenant_id, resolution_case.raw_envelope_id)
    registration = DurableSoupDb.source_registration(db_path, tenant_id, raw_envelope.source_key)

    finalize_interrupted_attempt(db_path, resolution_case, at, false)

    begin_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      max(resolution_case.attempt_count, 1),
      "pipeline",
      nil,
      @pipeline_version,
      at
    )

    record_attempt(
      db_path,
      resolution_case,
      raw_envelope,
      max(resolution_case.attempt_count, 1),
      "pipeline",
      nil,
      @pipeline_version,
      %{},
      %{},
      "failed_terminal:pipeline_crash",
      "crash",
      [],
      at,
      at
    )

    finish(
      db_path,
      registration,
      resolution_case,
      "failed_terminal:pipeline_crash",
      "pipeline_crash",
      at,
      "pipeline"
    )
  rescue
    _ ->
      {:outcome, "failed_terminal:pipeline_crash",
       DurableSoupDb.resolution_case(db_path, tenant_id, resolution_case_id)}
  catch
    _, _ ->
      {:outcome, "failed_terminal:pipeline_crash",
       DurableSoupDb.resolution_case(db_path, tenant_id, resolution_case_id)}
  end

  defp merge_consumed(measured, declared) do
    Map.merge(measured, declared, fn key, measured_value, declared_value ->
      if key in [:time_ms, "time_ms"],
        do: max(measured_value, declared_value),
        else: declared_value
    end)
  end

  defp enforce_case_budget(result, registration, resolution_case, ended_at) do
    total_case_ms = integer_budget!(registration.budgets, "total_case_ms")
    elapsed = DateTime.diff(ended_at, resolution_case.inserted_at, :millisecond)

    if elapsed > total_case_ms,
      do: {:budget_exhausted, :total_case_ms},
      else: result
  end

  defp result_consumed({:ok, _, consumed}, _measured), do: consumed
  defp result_consumed({:budget_exhausted, _, consumed}, _measured), do: consumed
  defp result_consumed(_, measured), do: measured

  defp attempt_outcome({:ok, _, _}), do: "ok"
  defp attempt_outcome({:ok, _}), do: "ok"
  defp attempt_outcome(:no_value), do: "no_value"
  defp attempt_outcome({:refused, reason}), do: "refused:" <> reason_code(reason)
  defp attempt_outcome({:outcome, typed}), do: typed_code(typed)
  defp attempt_outcome({:timeout, _}), do: "retry_scheduled:timeout"

  defp attempt_outcome({:budget_exhausted, budget, _}),
    do: "failed_terminal:budget_exhausted:" <> to_string(budget)

  defp attempt_outcome({:budget_exhausted, budget}),
    do: "failed_terminal:budget_exhausted:" <> to_string(budget)

  defp attempt_outcome({:invalid, reason}), do: "quarantined:" <> reason_code(reason)
  defp attempt_outcome({:crash, _}), do: "failed_terminal:crash"
  defp attempt_outcome(_), do: "failed_terminal:invalid_result"

  defp error_class({:timeout, _}), do: "timeout"
  defp error_class({:crash, _}), do: "crash"
  defp error_class({:budget_exhausted, budget, _}), do: "budget_exhausted:" <> to_string(budget)
  defp error_class({:budget_exhausted, budget}), do: "budget_exhausted:" <> to_string(budget)
  defp error_class(_), do: nil

  defp typed_code(code) when is_binary(code) do
    prefix = hd(String.split(code, ":", parts: 2))

    if prefix in outcome_vocab() and prefix != "eligible" do
      code
    else
      "failed_terminal:invalid_result"
    end
  end

  defp typed_code({outcome, reason})
       when outcome in [:unresolved, :quarantined, :refused, :failed_terminal, :retry_scheduled],
       do: Atom.to_string(outcome) <> ":" <> reason_code(reason)

  defp typed_code(reason)
       when reason in [:timeout, :network, :rate_limit, :rate_limited, :transient],
       do: "retry_scheduled:" <> reason_code(reason)

  defp typed_code(reason), do: "failed_terminal:" <> reason_code(reason)

  defp adapter_refusal_code(reason) do
    code = typed_code(reason)

    cond do
      code == "failed_terminal:malformed_envelope" -> "quarantined:malformed_envelope"
      String.starts_with?(code, "failed_terminal:") -> "refused:" <> reason_code(reason)
      true -> code
    end
  end

  defp reason_code({_, reason}), do: reason_code(reason)
  defp reason_code(reason) when is_atom(reason), do: Atom.to_string(reason)

  defp reason_code(reason) when is_binary(reason) do
    case String.split(reason, ":", parts: 2) do
      [_prefix, code] -> code
      [code] -> code
    end
  end

  defp reason_code(reason), do: inspect(reason)

  defp outcome_vocab,
    do: ~w(eligible unresolved retry_scheduled quarantined refused failed_terminal)

  defp outcome_validator_version("eligible"), do: Eligibility.validator_version()

  defp outcome_validator_version(code)
       when code in [
              "unresolved:insufficient_evidence",
              "unresolved:field_conflict",
              "unresolved:policy_not_registered",
              "refused:no_public_article"
            ],
       do: Eligibility.validator_version()

  defp outcome_validator_version(_code), do: @pipeline_version

  defp source_scope(registration, route, circuit_state),
    do: %{
      tenant_id: registration.tenant_id,
      source_key: registration.source_key,
      route: route,
      circuit_state: circuit_state
    }

  defp input_hash(input), do: input |> :erlang.term_to_binary() |> ChangesetStore.hash()

  defp deterministic_id(parts),
    do: parts |> Enum.map(&to_string/1) |> Enum.join("|") |> ChangesetStore.hash()

  defp module(module) when is_atom(module), do: module
  defp module("Elixir." <> _ = module), do: String.to_existing_atom(module)
  defp module(module) when is_binary(module), do: Module.concat([module])

  defp value(map, key, default \\ nil)
  defp value(nil, _key, default), do: default

  defp value(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        case Enum.find(map, fn {existing, _value} -> to_string(existing) == to_string(key) end) do
          nil -> default
          {_existing, value} -> value
        end
    end
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
