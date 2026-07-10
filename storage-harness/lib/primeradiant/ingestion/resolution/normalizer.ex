defmodule Primeradiant.Ingestion.Resolution.Normalizer do
  @moduledoc """
  Deterministically extracts direct source evidence from fields already supplied by
  the candidate. It performs no provider lookup and obtains no external facts.
  """

  alias Primeradiant.StorageHarness.{ChangesetStore, ResolutionEvidence, ResolvedSourceField}

  @roots ~w(raw_fields feed_metadata headers visible_links source_declared_metadata)a
  @max_nodes 10_000
  @field_keys %{
    "publisher_label" => ~w(publisher_label publisher source_label site_name og:site_name),
    "publisher_domain" => ~w(publisher_domain source_domain domain),
    "public_url" => ~w(public_url canonical_url url link href)
  }

  def normalize(candidate, %{resolution_case: resolution_case, raw_envelope: raw_envelope} = ctx),
    do: normalize(resolution_case, raw_envelope, candidate, Map.get(ctx, :budget, %{}))

  def normalize(resolution_case, raw_envelope, candidate)
      when is_map(candidate) and is_map(raw_envelope) do
    normalize(resolution_case, raw_envelope, candidate, %{})
  end

  def normalize(_, _, _), do: {:outcome, "quarantined:malformed_envelope"}

  def normalize(resolution_case, raw_envelope, candidate, budget)
      when is_map(candidate) and is_map(raw_envelope) do
    provenance = value(candidate, :adapter_provenance)

    if is_map(provenance) do
      with {:ok, extracted} <- walk_roots(candidate, budget) do
        pairs =
          for {path, supplied_value} <- extracted,
              is_binary(supplied_value),
              {field, keys} <- @field_keys,
              List.last(path) in keys,
              supplied_value != "" do
            artifact_pair(
              resolution_case,
              raw_envelope,
              field,
              supplied_value,
              path,
              provenance
            )
          end

        {:ok,
         %{
           evidence: Enum.map(pairs, &elem(&1, 0)),
           fields: Enum.map(pairs, &elem(&1, 1))
         }}
      end
    else
      {:outcome, "quarantined:malformed_envelope"}
    end
  end

  defp walk_roots(candidate, _budget) do
    Enum.reduce_while(@roots, {:ok, [], 0}, fn root, {:ok, acc, count} ->
      case value(candidate, root) do
        nil ->
          {:cont, {:ok, acc, count}}

        supplied ->
          case walk(supplied, [Atom.to_string(root)], count, @max_nodes) do
            {:ok, values, next_count} -> {:cont, {:ok, acc ++ values, next_count}}
            {:error, :nodes} -> {:halt, {:budget_exhausted, :nodes}}
          end
      end
    end)
    |> case do
      {:ok, values, _count} -> {:ok, values}
      other -> other
    end
  end

  defp artifact_pair(resolution_case, raw_envelope, field, supplied_value, path, provenance) do
    {normalized, transform} = normalize_value(field, supplied_value)
    locator = %{"attribute_path" => path}

    evidence_id =
      deterministic_id([
        value(resolution_case, :id),
        field,
        Jason.encode!(locator),
        supplied_value
      ])

    evidence =
      ChangesetStore.insert!(ResolutionEvidence, %{
        id: evidence_id,
        tenant_id: value(resolution_case, :tenant_id),
        resolution_case_id: value(resolution_case, :id),
        kind: field,
        value: supplied_value,
        source: "raw_envelope",
        locator: locator,
        digest: ChangesetStore.hash(supplied_value),
        retrieved_at: value(raw_envelope, :received_at),
        visibility: value(raw_envelope, :visibility),
        provenance: provenance,
        transformation_chain: []
      })

    field_candidate =
      ChangesetStore.insert!(ResolvedSourceField, %{
        id: deterministic_id([value(resolution_case, :id), field, normalized, evidence_id]),
        tenant_id: value(resolution_case, :tenant_id),
        resolution_case_id: value(resolution_case, :id),
        field_name: field,
        normalized_value: normalized,
        confidence: 1.0,
        evidence_refs: [evidence_id],
        resolver_provenance: [provenance],
        transform: transform,
        contradiction_status: "none",
        selected: false
      })

    {evidence, field_candidate}
  end

  defp normalize_value("publisher_domain", value),
    do: {value |> String.trim() |> String.downcase(), "lowercase"}

  defp normalize_value("public_url", value) do
    trimmed = String.trim(value)

    normalized =
      case URI.parse(trimmed) do
        %URI{host: host} = uri when is_binary(host) ->
          uri |> Map.put(:host, String.downcase(host)) |> URI.to_string()

        _ ->
          trimmed
      end

    transform =
      cond do
        normalized == value -> "copy"
        normalized == trimmed -> "trim"
        true -> "normalize_url"
      end

    {normalized, transform}
  end

  defp normalize_value(_, value), do: {String.trim(value), "trim"}

  defp walk(_value, _path, count, cap) when is_integer(cap) and count >= cap,
    do: {:error, :nodes}

  defp walk(map, path, count, cap) when is_map(map),
    do: walk_entries(Enum.to_list(map), path, count + 1, cap)

  defp walk(list, path, count, cap) when is_list(list),
    do: walk_entries(Enum.with_index(list), path, count + 1, cap)

  defp walk(value, path, count, _cap), do: {:ok, [{path, value}], count + 1}

  defp walk_entries(entries, path, count, cap) do
    Enum.reduce_while(entries, {:ok, [], count}, fn {key, nested}, {:ok, acc, current} ->
      case walk(nested, path ++ [to_string(key)], current, cap) do
        {:ok, values, next} -> {:cont, {:ok, acc ++ values, next}}
        error -> {:halt, error}
      end
    end)
  end

  defp deterministic_id(parts), do: parts |> Enum.join("|") |> ChangesetStore.hash()
  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
