defmodule Primeradiant.Ingestion.SourceRegistry do
  @moduledoc false

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    RawEnvelope,
    ResolutionCase,
    ResolutionOutcome,
    SourceGapRecord,
    SourceRegistration
  }

  @digest_conflict "quarantined:source_identity_digest_conflict"
  @registry_version "source_registry_v1"
  @resolver_budget_keys ~w(time_ms requests bytes redirects result_count)

  def register_source(db_path, attrs) do
    with :ok <- validate_registration(attrs) do
      tenant_id = Map.fetch!(attrs, :tenant_id)
      source_key = Map.fetch!(attrs, :source_key)
      policy = Map.fetch!(attrs, :resolution_policy)
      existing = DurableSoupDb.source_registration(db_path, tenant_id, source_key)
      now = now()

      registration =
        ChangesetStore.insert!(SourceRegistration, %{
          id: existing && existing.id,
          tenant_id: tenant_id,
          source_key: source_key,
          adapter_module: attrs |> Map.fetch!(:adapter_module) |> to_string(),
          adapter_version: Map.fetch!(attrs, :adapter_version),
          mode: Map.get(attrs, :mode, "disabled") |> to_string(),
          resolution_policy: policy,
          policy_version: Map.fetch!(attrs, :policy_version),
          policy_hash: policy_hash(policy),
          budgets: Map.get(attrs, :budgets, %{}),
          config: Map.get(attrs, :config, %{}),
          cursor: registration_cursor(attrs, existing),
          last_received_at: existing && existing.last_received_at,
          last_resolution_terminal_at: existing && existing.last_resolution_terminal_at,
          last_admission_at: existing && existing.last_admission_at,
          gap_count: (existing && existing.gap_count) || 0,
          refusal_count: (existing && existing.refusal_count) || 0,
          unresolved_count: (existing && existing.unresolved_count) || 0,
          quarantine_count: (existing && existing.quarantine_count) || 0,
          circuit_state: circuit_states(attrs, existing),
          inserted_at: (existing && existing.inserted_at) || now,
          updated_at: now
        })

      {:ok, DurableSoupDb.put_source_registration!(db_path, registration)}
    end
  end

  def policy_for(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        {:ok,
         %{
           resolution_policy: registration.resolution_policy,
           policy_version: registration.policy_version,
           policy_hash: registration.policy_hash
         }}
    end
  end

  def receive_envelope(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        receive_registered_envelope(db_path, registration, attrs)
    end
  end

  def mark_processed(db_path, %{admission_ref: ref} = attrs) when is_binary(ref),
    do: mark_position_processed(db_path, attrs)

  def mark_processed(db_path, %{outcome_ref: ref} = attrs) when is_binary(ref),
    do: mark_position_processed(db_path, attrs)

  def advance_cursor(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        cursor = normalize_cursor(registration.cursor)
        contiguous = cursor["contiguous_position"]
        processed = MapSet.new(cursor["processed_positions"])
        conflicts = MapSet.new(cursor["conflict_positions"])
        advanced = advance_from(contiguous, processed, conflicts)

        remaining =
          processed
          |> Enum.reject(&(&1 <= advanced))
          |> Enum.sort()

        updated_cursor =
          cursor
          |> Map.put("contiguous_position", advanced)
          |> Map.put("processed_positions", remaining)

        updated = update_registration(registration, %{cursor: updated_cursor})
        {:ok, DurableSoupDb.put_source_registration!(db_path, updated)}
    end
  end

  def record_resolution_terminal(db_path, attrs) do
    record_timestamp(db_path, attrs, :last_resolution_terminal_at)
  end

  def record_admission(db_path, attrs) do
    record_timestamp(db_path, attrs, :last_admission_at)
  end

  def record_resolution_signal(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        route = attrs |> Map.fetch!(:route) |> to_string()
        states = Map.put(registration.circuit_state, route, Map.fetch!(attrs, :circuit_state))
        updated = update_registration(registration, %{circuit_state: states})

        {:ok, DurableSoupDb.put_source_registration!(db_path, updated)}
    end
  end

  def health(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        state = DurableSoupDb.load_tenant(db_path, registration.tenant_id)
        envelope_ids = source_envelope_ids(state, registration.source_key)
        cases = Enum.filter(state.resolution_cases, &(&1.raw_envelope_id in envelope_ids))
        case_ids = MapSet.new(cases, & &1.id)

        outcomes =
          Enum.filter(state.resolution_outcomes, &MapSet.member?(case_ids, &1.resolution_case_id))

        gaps =
          DurableSoupDb.source_gap_records(
            db_path,
            registration.tenant_id,
            registration.source_key
          )

        {:ok,
         %{
           source_key: registration.source_key,
           last_received_at: registration.last_received_at,
           last_resolution_terminal_at: registration.last_resolution_terminal_at,
           last_admission_at: registration.last_admission_at,
           resolution_backlog: Enum.count(cases, &(&1.state in backlog_states())),
           retry_depth: Enum.count(cases, &(&1.state == "retry_scheduled")),
           unresolved_count: outcome_count(outcomes, "unresolved"),
           quarantine_count: outcome_count(outcomes, "quarantined"),
           refusal_count: outcome_count(outcomes, "refused"),
           circuit_states: registration.circuit_state,
           circuit_state: aggregate_circuit_state(registration.circuit_state),
           gap_counts: %{
             open: Enum.count(gaps, &(&1.status == "open")),
             closed: Enum.count(gaps, &(&1.status == "closed"))
           }
         }}
    end
  end

  defp policy_hash(policy) do
    policy
    |> canonicalize()
    |> Jason.encode!()
    |> ChangesetStore.hash()
  end

  defp validate_registration(attrs) do
    budgets = Map.get(attrs, :budgets)
    config = Map.get(attrs, :config)

    with true <- is_map(budgets),
         true <- positive(budgets, "total_case_ms"),
         true <- positive(budgets, "max_attempts"),
         true <- nonnegative(budgets, "retry_backoff_ms"),
         true <- positive(budgets, "per_source_concurrency"),
         true <- stage_budget?(budgets, "adapter"),
         true <- stage_budget?(budgets, "normalizer"),
         true <- is_map(config),
         routes when is_list(routes) <- value(config, "resolvers"),
         true <- Enum.all?(routes, &valid_route?(&1, budgets)),
         true <- valid_inference?(value(config, "inference"), budgets) do
      :ok
    else
      _ -> {:error, :invalid_resolution_registration}
    end
  end

  defp valid_route?(route, budgets) when is_map(route) do
    resolver_budgets = value(budgets, "resolvers")
    id = value(route, "id")

    is_binary(id) and id != "" and valid_module?(value(route, "module")) and
      is_binary(value(route, "version")) and is_list(value(route, "evidence_types")) and
      is_map(resolver_budgets) and
      Enum.all?(@resolver_budget_keys, &nonnegative(value(resolver_budgets, id), &1))
  end

  defp valid_route?(_, _), do: false

  defp valid_inference?(nil, _budgets), do: true

  defp valid_inference?(config, budgets) when is_map(config) do
    is_binary(value(config, "id")) and valid_module?(value(config, "module")) and
      is_binary(value(config, "version")) and stage_budget?(budgets, "inference") and
      nonnegative(value(budgets, "inference"), "tokens")
  end

  defp valid_inference?(_, _), do: false

  defp valid_module?(module), do: is_atom(module) or (is_binary(module) and module != "")
  defp stage_budget?(budgets, stage), do: positive(value(budgets, stage), "time_ms")

  defp positive(map, key), do: is_integer(value(map, key)) and value(map, key) > 0
  defp nonnegative(map, key), do: is_integer(value(map, key)) and value(map, key) >= 0

  defp circuit_states(attrs, existing) do
    case Map.get(attrs, :circuit_state, existing && existing.circuit_state) do
      states when is_map(states) and map_size(states) > 0 -> states
      _ -> configured_circuit_states(Map.get(attrs, :config, %{}))
    end
  end

  defp configured_circuit_states(config) do
    resolver_ids = Enum.map(value(config, "resolvers") || [], &value(&1, "id"))

    inference_ids =
      if value(config, "inference"), do: [value(value(config, "inference"), "id")], else: []

    ["adapter", "normalizer" | resolver_ids ++ inference_ids]
    |> Map.new(&{&1, "closed"})
  end

  defp aggregate_circuit_state(states) do
    priority = %{"closed" => 0, "degraded" => 1, "resolution_stalled" => 2, "disabled" => 3}

    states
    |> Map.values()
    |> Enum.max_by(&Map.get(priority, &1, 0), fn -> "closed" end)
  end

  defp receive_registered_envelope(db_path, registration, attrs) do
    source_identity = Map.fetch!(attrs, :source_event_external_id)
    digest = Map.fetch!(attrs, :content_digest)
    position = Map.fetch!(attrs, :source_position)

    existing_identity =
      db_path
      |> DurableSoupDb.raw_envelopes_for_source(registration.tenant_id, registration.source_key)
      |> Enum.filter(&(&1.source_event_external_id == source_identity))

    same =
      Enum.find(
        existing_identity,
        &(&1.content_digest == digest and &1.adapter_version == registration.adapter_version)
      )

    cond do
      same ->
        resolution_case =
          resolution_case_for(
            db_path,
            registration.tenant_id,
            same.id
          )

        touch_received(db_path, registration, Map.fetch!(attrs, :received_at))

        {:ok, receipt_result(same, resolution_case, true)}

      Enum.any?(existing_identity, &(&1.content_digest != digest)) ->
        receive_digest_conflict(db_path, registration, attrs)

      true ->
        open_discontinuity_gaps(db_path, registration, position)

        {envelope, resolution_case} =
          insert_envelope_and_case(db_path, registration, attrs, "received", nil)

        touch_received(db_path, registration, Map.fetch!(attrs, :received_at))
        {:ok, receipt_result(envelope, resolution_case, false)}
    end
  end

  defp receive_digest_conflict(db_path, registration, attrs) do
    position = Map.fetch!(attrs, :source_position)
    open_discontinuity_gaps(db_path, registration, position)

    {envelope, resolution_case} =
      insert_envelope_and_case(db_path, registration, attrs, "quarantined", @digest_conflict)

    outcome =
      ChangesetStore.insert!(ResolutionOutcome, %{
        tenant_id: registration.tenant_id,
        resolution_case_id: resolution_case.id,
        outcome_code: @digest_conflict,
        reason: "source_identity_digest_conflict",
        retryable: false,
        quarantine_ref: envelope.id,
        validator_version: @registry_version
      })
      |> then(&DurableSoupDb.insert_resolution_outcome!(db_path, &1))

    latest =
      DurableSoupDb.source_registration(db_path, registration.tenant_id, registration.source_key)

    cursor = normalize_cursor(latest.cursor)

    updated =
      update_registration(latest, %{
        cursor:
          Map.update!(cursor, "conflict_positions", fn positions ->
            Enum.sort(Enum.uniq([position | positions]))
          end),
        last_received_at: Map.fetch!(attrs, :received_at),
        quarantine_count: latest.quarantine_count + 1
      })

    DurableSoupDb.put_source_registration!(db_path, updated)

    {:ok,
     receipt_result(envelope, resolution_case, false)
     |> Map.put(:outcome_code, @digest_conflict)
     |> Map.put(:outcome_ref, outcome.id)}
  end

  defp insert_envelope_and_case(db_path, registration, attrs, state, outcome_code) do
    identity = Map.fetch!(attrs, :source_event_external_id)
    digest = Map.fetch!(attrs, :content_digest)
    position = Map.fetch!(attrs, :source_position)

    envelope =
      ChangesetStore.insert!(RawEnvelope, %{
        tenant_id: registration.tenant_id,
        source_key: registration.source_key,
        adapter_version: registration.adapter_version,
        source_event_external_id: identity,
        received_at: Map.fetch!(attrs, :received_at),
        content_digest: digest,
        integrity_metadata:
          attrs
          |> Map.get(:integrity_metadata, %{})
          |> Map.put("source_position", position),
        raw_object_ref: Map.get(attrs, :raw_object_ref),
        retained_bytes: Map.get(attrs, :retained_bytes),
        visibility: Map.fetch!(attrs, :visibility),
        correlation_id: Map.fetch!(attrs, :correlation_id),
        idempotency_key:
          Enum.join(
            [registration.source_key, identity, digest, registration.adapter_version],
            ":"
          )
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :raw_envelopes, &1))

    resolution_case =
      ChangesetStore.insert!(ResolutionCase, %{
        tenant_id: registration.tenant_id,
        raw_envelope_id: envelope.id,
        policy_version: registration.policy_version,
        state: state,
        attempt_count: 0,
        outcome_code: outcome_code,
        config_policy_hash: registration.policy_hash,
        trace_id: Map.get(attrs, :trace_id, Map.fetch!(attrs, :correlation_id))
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_cases, &1))

    {envelope, resolution_case}
  end

  defp mark_position_processed(db_path, attrs) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        position = Map.fetch!(attrs, :source_position)
        cursor = normalize_cursor(registration.cursor)

        if position in cursor["conflict_positions"] do
          {:error, :source_identity_digest_conflict}
        else
          cursor =
            Map.update!(cursor, "processed_positions", fn positions ->
              Enum.sort(Enum.uniq([position | positions]))
            end)

          close_gap(db_path, registration, position)

          gaps =
            DurableSoupDb.source_gap_records(
              db_path,
              registration.tenant_id,
              registration.source_key
            )

          registration
          |> update_registration(%{
            cursor: cursor,
            gap_count: Enum.count(gaps, &(&1.status == "open"))
          })
          |> then(&DurableSoupDb.put_source_registration!(db_path, &1))

          advance_cursor(db_path, attrs)
        end
    end
  end

  defp open_discontinuity_gaps(db_path, registration, observed_position) do
    cursor = normalize_cursor(registration.cursor)
    contiguous = cursor["contiguous_position"]

    if observed_position > contiguous + 1 do
      present_positions =
        db_path
        |> DurableSoupDb.raw_envelopes_for_source(registration.tenant_id, registration.source_key)
        |> Enum.map(&get_in(&1.integrity_metadata, ["source_position"]))
        |> MapSet.new()

      existing_gaps =
        db_path
        |> DurableSoupDb.source_gap_records(registration.tenant_id, registration.source_key)
        |> Map.new(&{&1.source_position, &1})

      (contiguous + 1)..(observed_position - 1)
      |> Enum.reject(&MapSet.member?(present_positions, &1))
      |> Enum.each(fn position ->
        unless Map.has_key?(existing_gaps, to_string(position)) do
          ChangesetStore.insert!(SourceGapRecord, %{
            tenant_id: registration.tenant_id,
            source_key: registration.source_key,
            source_position: to_string(position),
            status: "open",
            opened_at: now()
          })
          |> then(&DurableSoupDb.put_source_gap_record!(db_path, &1))
        end
      end)

      latest =
        DurableSoupDb.source_registration(
          db_path,
          registration.tenant_id,
          registration.source_key
        )

      gaps =
        DurableSoupDb.source_gap_records(db_path, registration.tenant_id, registration.source_key)

      latest
      |> update_registration(%{gap_count: Enum.count(gaps, &(&1.status == "open"))})
      |> then(&DurableSoupDb.put_source_registration!(db_path, &1))
    end
  end

  defp close_gap(db_path, registration, position) do
    db_path
    |> DurableSoupDb.source_gap_records(registration.tenant_id, registration.source_key)
    |> Enum.filter(&(&1.source_position == to_string(position) and &1.status == "open"))
    |> Enum.each(fn gap ->
      gap
      |> ChangesetStore.update!(%{status: "closed", closed_at: now()})
      |> then(&DurableSoupDb.put_source_gap_record!(db_path, &1))
    end)
  end

  defp touch_received(db_path, registration, received_at) do
    db_path
    |> DurableSoupDb.source_registration(registration.tenant_id, registration.source_key)
    |> update_registration(%{last_received_at: received_at})
    |> then(&DurableSoupDb.put_source_registration!(db_path, &1))
  end

  defp record_timestamp(db_path, attrs, field) do
    case registration(db_path, attrs) do
      nil ->
        {:error, :policy_not_registered}

      registration ->
        updated = update_registration(registration, %{field => Map.fetch!(attrs, :at)})
        {:ok, DurableSoupDb.put_source_registration!(db_path, updated)}
    end
  end

  defp update_registration(registration, attrs) do
    ChangesetStore.update!(registration, Map.put(attrs, :updated_at, now()))
  end

  defp registration(db_path, attrs) do
    DurableSoupDb.source_registration(
      db_path,
      Map.fetch!(attrs, :tenant_id),
      Map.fetch!(attrs, :source_key)
    )
  end

  defp registration_cursor(attrs, nil),
    do: attrs |> Map.fetch!(:initial_cursor) |> normalize_cursor()

  defp registration_cursor(attrs, existing),
    do: Map.get(attrs, :initial_cursor, existing.cursor) |> normalize_cursor()

  defp normalize_cursor(position) when is_integer(position) do
    %{
      "contiguous_position" => position,
      "processed_positions" => [],
      "conflict_positions" => []
    }
  end

  defp normalize_cursor(cursor) when is_map(cursor) do
    position =
      cursor["contiguous_position"] || cursor[:contiguous_position] || cursor["position"] ||
        cursor[:position] || 0

    %{
      "contiguous_position" => position,
      "processed_positions" =>
        cursor["processed_positions"] || cursor[:processed_positions] || [],
      "conflict_positions" => cursor["conflict_positions"] || cursor[:conflict_positions] || []
    }
  end

  defp advance_from(position, processed, conflicts) do
    next = position + 1

    if MapSet.member?(processed, next) and not MapSet.member?(conflicts, next) do
      advance_from(next, processed, conflicts)
    else
      position
    end
  end

  defp resolution_case_for(db_path, tenant_id, envelope_id) do
    db_path
    |> DurableSoupDb.load_tenant(tenant_id)
    |> Map.fetch!(:resolution_cases)
    |> Enum.find(&(&1.raw_envelope_id == envelope_id))
  end

  defp receipt_result(envelope, resolution_case, duplicate) do
    %{
      duplicate: duplicate,
      raw_envelope_id: envelope.id,
      resolution_case_id: resolution_case.id,
      state: resolution_case.state
    }
  end

  defp source_envelope_ids(state, source_key) do
    state.raw_envelopes
    |> Enum.filter(&(&1.source_key == source_key))
    |> MapSet.new(& &1.id)
  end

  defp outcome_count(outcomes, prefix) do
    Enum.count(outcomes, &String.starts_with?(&1.outcome_code, prefix))
  end

  defp backlog_states,
    do: ["received", "normalizing", "resolving", "interpreting", "validating"]

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
    |> Enum.map(fn {key, nested} -> {to_string(key), canonicalize(nested)} end)
    |> Jason.OrderedObject.new()
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value), do: value

  defp value(nil, _key), do: nil

  defp value(map, key) do
    Map.get(map, key) || Map.get(map, to_string(key))
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
