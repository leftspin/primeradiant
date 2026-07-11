defmodule Primeradiant.Ingestion.PackageAcknowledgement do
  @moduledoc false

  alias Primeradiant.Ingestion.SourceRegistry

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    PackageAcknowledgement
  }

  @conflict "refused:package_identity_digest_conflict"
  @terminal_non_admission ~w(unresolved quarantined refused failed_terminal)

  def acknowledge_package(db_path, manifest) when is_map(manifest) do
    with :ok <- validate_manifest(manifest),
         :ok <- validate_digest(manifest) do
      acknowledge_valid(db_path, manifest)
    end
  end

  def acknowledge_package(_db_path, _manifest), do: {:error, :malformed_manifest}

  def manifest_digest(manifest) do
    manifest
    |> manifest_content()
    |> SourceRegistry.policy_hash()
  end

  defp acknowledge_valid(db_path, manifest) do
    existing =
      db_path
      |> DurableSoupDb.package_acknowledgements_for_package(
        value(manifest, :tenant_id),
        value(manifest, :package_id)
      )
      |> Enum.find(&(&1.manifest_digest == value(manifest, :manifest_digest)))

    case existing do
      %{status: "durably_processed"} = acknowledgement ->
        with :ok <-
               revalidate_replay(db_path, manifest, acknowledgement.envelope_disposition_refs),
             :ok <-
               reapply_positions(db_path, manifest, acknowledgement.envelope_disposition_refs) do
          {:ok, result(acknowledgement, true)}
        end

      %{status: @conflict} = refusal ->
        {:error, %{code: @conflict, refusal_ref: refusal.id}}

      nil ->
        build_acknowledgement(db_path, manifest)
    end
  end

  defp build_acknowledgement(db_path, manifest) do
    entries = value(manifest, :envelopes)
    dispositions = Enum.map(entries, &disposition(db_path, manifest, &1))

    failures = Enum.filter(dispositions, &match?({:error, _}, &1))

    if failures == [] do
      refs = Enum.map(dispositions, fn {:ok, ref} -> ref end)
      completed_at = now()

      acknowledgement =
        ChangesetStore.insert!(PackageAcknowledgement, %{
          tenant_id: value(manifest, :tenant_id),
          package_id: value(manifest, :package_id),
          manifest_digest: value(manifest, :manifest_digest),
          source_position_range: position_range(value(manifest, :envelopes)),
          status: "durably_processed",
          envelope_disposition_refs: refs,
          policy_hash: SourceRegistry.policy_hash(Enum.map(refs, & &1["policy_hash"])),
          completed_at: completed_at,
          trace_id: "package:#{value(manifest, :package_id)}"
        })

      refusal = conflict_refusal(manifest)
      claimed = DurableSoupDb.claim_package_identity!(db_path, acknowledgement, refusal)

      if claimed.status == "durably_processed" do
        case reapply_positions(db_path, manifest, claimed.envelope_disposition_refs) do
          :ok -> {:ok, result(claimed, claimed.id != acknowledgement.id)}
          {:error, reason} -> {:error, reason}
        end
      else
        {:error, %{code: @conflict, refusal_ref: claimed.id}}
      end
    else
      {:error, failure_payload(failures)}
    end
  end

  defp disposition(db_path, manifest, envelope_manifest) do
    key = value(envelope_manifest, :envelope_key)

    with raw_envelope when not is_nil(raw_envelope) <-
           find_envelope(db_path, manifest, envelope_manifest),
         resolution_case when not is_nil(resolution_case) <-
           DurableSoupDb.resolution_case_for_envelope(
             db_path,
             value(manifest, :tenant_id),
             raw_envelope.id
           ),
         outcome when not is_nil(outcome) <-
           qualifying_outcome(db_path, resolution_case) do
      {:ok,
       %{
         "envelope_key" => key,
         "raw_envelope_id" => raw_envelope.id,
         "resolution_case_id" => resolution_case.id,
         "outcome_code" => outcome.outcome_code,
         "outcome_ref" => outcome.id,
         "admission_ref" => outcome.admission_material_ref,
         "retry_at" => retry_at(resolution_case, outcome),
         "policy_hash" => resolution_case.config_policy_hash
       }}
    else
      nil -> {:error, key}
    end
  end

  defp find_envelope(db_path, manifest, envelope_manifest) do
    source_key = value(manifest, :source_key)

    position = value(envelope_manifest, :source_position)

    envelope =
      case value(envelope_manifest, :raw_envelope_id) do
        id when is_binary(id) ->
          case DurableSoupDb.raw_envelope(db_path, value(manifest, :tenant_id), id) do
            %{source_key: ^source_key} = envelope ->
              envelope

            _ ->
              nil
          end

        _ ->
          identity = value(envelope_manifest, :source_event_external_id)

          if is_binary(identity) do
            DurableSoupDb.raw_envelope_for_identity(
              db_path,
              value(manifest, :tenant_id),
              source_key,
              identity,
              position
            )
          end
      end

    case envelope do
      %{integrity_metadata: %{"source_position" => ^position}} -> envelope
      _ -> nil
    end
  end

  defp qualifying_outcome(db_path, resolution_case) do
    outcomes =
      DurableSoupDb.resolution_outcomes_for_case(
        db_path,
        resolution_case.tenant_id,
        resolution_case.id
      )

    Enum.find(outcomes, fn outcome ->
      family = outcome_family(outcome.outcome_code)

      outcome.outcome_code == resolution_case.outcome_code and
        family == resolution_case.state and
        case family do
          "eligible" -> is_binary(outcome.admission_material_ref)
          "retry_scheduled" -> not is_nil(resolution_case.next_retry_at)
          terminal -> terminal in @terminal_non_admission
        end
    end)
  end

  defp retry_at(resolution_case, outcome) do
    if String.starts_with?(outcome.outcome_code, "retry_scheduled:"),
      do: DateTime.to_iso8601(resolution_case.next_retry_at)
  end

  defp mark_processed(db_path, manifest, entry, ref) do
    result_ref = ref["admission_ref"] || ref["outcome_ref"]
    ref_key = if ref["admission_ref"], do: :admission_ref, else: :outcome_ref

    attrs = %{
      tenant_id: value(manifest, :tenant_id),
      source_key: value(manifest, :source_key),
      source_position: value(entry, :source_position)
    }

    SourceRegistry.mark_processed(db_path, Map.put(attrs, ref_key, result_ref))
  end

  defp conflict_refusal(manifest) do
    refs = [%{"outcome_code" => @conflict, "envelope_key" => nil}]

    ChangesetStore.insert!(PackageAcknowledgement, %{
      tenant_id: value(manifest, :tenant_id),
      package_id: value(manifest, :package_id),
      manifest_digest: value(manifest, :manifest_digest),
      source_position_range: position_range(value(manifest, :envelopes)),
      status: @conflict,
      envelope_disposition_refs: refs,
      policy_hash: SourceRegistry.policy_hash(manifest_content(manifest)),
      completed_at: now(),
      trace_id: "package:#{value(manifest, :package_id)}:digest-conflict"
    })
  end

  defp reapply_positions(db_path, manifest, refs) do
    value(manifest, :envelopes)
    |> Enum.zip(refs)
    |> Enum.reduce_while(:ok, fn {entry, ref}, :ok ->
      case mark_processed(db_path, manifest, entry, ref) do
        {:ok, _registration} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp revalidate_replay(db_path, manifest, refs) do
    value(manifest, :envelopes)
    |> Enum.zip(refs)
    |> Enum.reduce_while(:ok, fn {entry, ref}, :ok ->
      envelope = find_envelope(db_path, manifest, entry)

      if envelope && envelope.id == ref["raw_envelope_id"] &&
           value(entry, :envelope_key) == ref["envelope_key"] do
        {:cont, :ok}
      else
        {:halt, {:error, failure_payload([{:error, value(entry, :envelope_key)}])}}
      end
    end)
  end

  defp failure_payload(failures) do
    keys = Enum.map(failures, fn {:error, key} -> key end)
    %{code: :package_not_durably_processed, missing_or_pending_envelope_keys: keys}
  end

  defp result(acknowledgement, replay) do
    refs = acknowledgement.envelope_disposition_refs

    %{
      acknowledgement_id: acknowledgement.id,
      status: acknowledgement.status,
      replay: replay,
      envelope_disposition_refs: refs,
      counts: %{
        admitted: Enum.count(refs, &is_binary(&1["admission_ref"])),
        retry_scheduled:
          Enum.count(refs, &String.starts_with?(&1["outcome_code"], "retry_scheduled:")),
        terminal_non_admitted:
          Enum.count(refs, fn ref ->
            family = outcome_family(ref["outcome_code"])
            family in @terminal_non_admission
          end)
      }
    }
  end

  defp validate_manifest(manifest) do
    envelopes = value(manifest, :envelopes)

    if is_binary(value(manifest, :package_id)) and
         is_binary(value(manifest, :manifest_digest)) and
         is_binary(value(manifest, :source_key)) and
         is_binary(value(manifest, :tenant_id)) and is_list(envelopes) and envelopes != [] and
         is_integer(value(manifest, :expected_count)) and
         value(manifest, :expected_count) == length(envelopes) and
         Enum.all?(envelopes, &valid_envelope_manifest?/1) do
      :ok
    else
      {:error, :malformed_manifest}
    end
  end

  defp validate_digest(manifest) do
    if value(manifest, :manifest_digest) == manifest_digest(manifest),
      do: :ok,
      else: {:error, :manifest_digest_mismatch}
  end

  defp valid_envelope_manifest?(envelope) when is_map(envelope) do
    is_binary(value(envelope, :envelope_key)) and
      is_integer(value(envelope, :source_position)) and
      (is_binary(value(envelope, :raw_envelope_id)) or
         is_binary(value(envelope, :source_event_external_id)))
  end

  defp valid_envelope_manifest?(_envelope), do: false

  defp manifest_content(manifest) do
    %{
      "package_id" => value(manifest, :package_id),
      "source_key" => value(manifest, :source_key),
      "tenant_id" => value(manifest, :tenant_id),
      "expected_count" => value(manifest, :expected_count),
      "envelopes" =>
        Enum.map(value(manifest, :envelopes) || [], fn envelope ->
          %{
            "envelope_key" => value(envelope, :envelope_key),
            "source_position" => value(envelope, :source_position),
            "raw_envelope_id" => value(envelope, :raw_envelope_id),
            "source_event_external_id" => value(envelope, :source_event_external_id)
          }
        end)
    }
  end

  defp position_range(envelopes) do
    positions = Enum.map(envelopes, &value(&1, :source_position))
    %{"first" => Enum.min(positions), "last" => Enum.max(positions)}
  end

  defp outcome_family(code), do: code |> String.split(":", parts: 2) |> hd()

  defp value(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))
  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
