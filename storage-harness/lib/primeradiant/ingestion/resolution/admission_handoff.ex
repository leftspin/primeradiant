defmodule Primeradiant.Ingestion.Resolution.AdmissionHandoff do
  @moduledoc false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb, RealIngestion}

  @source_mode "source_evidence_resolution_v1"

  def admit(db_path, resolution_case_id, opts) do
    tenant_id = Keyword.fetch!(opts, :tenant_id)

    case DurableSoupDb.resolution_case(db_path, tenant_id, resolution_case_id) do
      nil ->
        {:error, :resolution_case_not_found}

      %{state: state} when state != "eligible" ->
        {:error, {:resolution_case_not_eligible, state}}

      resolution_case ->
        case current_outcome(db_path, resolution_case) do
          nil -> {:error, :eligible_outcome_pending}
          outcome -> admit_eligible(db_path, resolution_case, outcome, opts)
        end
    end
  end

  defp admit_eligible(db_path, resolution_case, outcome, opts) do
    raw_envelope =
      DurableSoupDb.raw_envelope(
        db_path,
        resolution_case.tenant_id,
        resolution_case.raw_envelope_id
      )

    if outcome.admission_material_ref do
      repair_admission_health(db_path, raw_envelope, outcome, opts)
      {:ok, outcome.admission_material_ref}
    else
      fields =
        db_path
        |> DurableSoupDb.resolved_source_fields_for_case(
          resolution_case.tenant_id,
          resolution_case.id
        )
        |> Enum.filter(& &1.selected)

      case admission_material(resolution_case) do
        nil ->
          {:error, :admission_material_not_declared}

        material ->
          item = admission_item(raw_envelope, resolution_case, fields, material)
          expected_revision = DurableSoupDb.tenant_revision(db_path, resolution_case.tenant_id)
          run_before_state_load(opts)
          prior_state = DurableSoupDb.load_tenant(db_path, resolution_case.tenant_id)
          actor_id = Keyword.get(opts, :actor_id, "flynn")
          {:ok, state, report} = RealIngestion.ingest_items(prior_state, [item], actor_id)
          run_before_persist(opts, item, state)

          DurableSoupDb.persist_delta!(db_path, prior_state, state, %{
            source_kind: @source_mode,
            source_db_path: "resolution-case:#{resolution_case.id}",
            source_row_count: 1,
            expected_tenant_revision: expected_revision
          })

          admission_ref = report.admissions |> List.first() |> Map.fetch!(:source_ref)

          outcome
          |> ChangesetStore.update!(%{admission_material_ref: admission_ref})
          |> then(&DurableSoupDb.insert_resolution_outcome!(db_path, &1))

          SourceRegistry.record_admission(db_path, %{
            tenant_id: resolution_case.tenant_id,
            source_key: raw_envelope.source_key,
            at: admission_at(opts)
          })

          {:ok, admission_ref}
      end
    end
  end

  defp current_outcome(db_path, resolution_case) do
    db_path
    |> DurableSoupDb.resolution_outcomes_for_case(
      resolution_case.tenant_id,
      resolution_case.id
    )
    |> Enum.find(&String.starts_with?(&1.outcome_code, "eligible"))
  end

  defp admission_item(raw_envelope, resolution_case, fields, material) do
    selected = Map.new(fields, &{&1.field_name, &1.normalized_value})
    evidence_refs = fields |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq() |> Enum.sort()
    provenance = fields |> Enum.flat_map(& &1.resolver_provenance) |> Enum.uniq()
    public_url = selected["public_url"]

    metadata =
      Map.merge(selected, %{
        "resolution_case_id" => resolution_case.id,
        "raw_envelope_id" => raw_envelope.id,
        "policy_hash" => resolution_case.config_policy_hash,
        "evidence_ref_ids" => evidence_refs,
        "provenance" => provenance
      })

    %{
      tenant_id: resolution_case.tenant_id,
      source_mode: @source_mode,
      source_type: value(material, "source_type"),
      external_id: raw_envelope.source_event_external_id,
      observed_at: DateTime.to_iso8601(raw_envelope.received_at),
      retrieved_at: DateTime.to_iso8601(raw_envelope.received_at),
      canonical_uri: if(public_url == "no_public_url", do: nil, else: public_url),
      raw_object_uri: raw_envelope.raw_object_ref || "raw-envelope:#{raw_envelope.id}",
      source_name: selected["publisher_label"],
      acl: value(material, "acl"),
      ingestion_run_key: "resolution-case:#{resolution_case.id}",
      metadata: metadata
    }
  end

  defp repair_admission_health(db_path, raw_envelope, outcome, opts) do
    registration =
      DurableSoupDb.source_registration(
        db_path,
        raw_envelope.tenant_id,
        raw_envelope.source_key
      )

    repair_at = outcome.updated_at || admission_at(opts)

    if is_nil(registration.last_admission_at) or
         DateTime.compare(registration.last_admission_at, repair_at) == :lt do
      SourceRegistry.record_admission(db_path, %{
        tenant_id: raw_envelope.tenant_id,
        source_key: raw_envelope.source_key,
        at: repair_at
      })
    end
  end

  defp run_before_state_load(opts) do
    case Keyword.get(opts, :before_state_load) do
      callback when is_function(callback, 0) -> callback.()
      nil -> :ok
    end
  end

  defp run_before_persist(opts, item, state) do
    case Keyword.get(opts, :before_persist) do
      callback when is_function(callback, 2) -> callback.(item, state)
      callback when is_function(callback, 1) -> callback.(item)
      nil -> :ok
    end
  end

  defp admission_material(resolution_case),
    do: resolution_case.policy_snapshot |> value("config") |> value("admission_material")

  defp admission_at(opts),
    do: Keyword.get(opts, :at, DateTime.utc_now() |> DateTime.truncate(:microsecond))

  defp value(nil, _key), do: nil
  defp value(map, key), do: Map.get(map, key) || Map.get(map, String.to_atom(key))
end
