defmodule Primeradiant.Ingestion.Resolution.Inference do
  @moduledoc """
  Optional bounded interpretation seam over an evidence packet.

  Implementations have no network or tool access. Every returned candidate must cite
  evidence references and declare the deterministic transform that produces its value;
  the case pipeline validates that provenance before persistence.
  """

  @callback interpret(evidence_packet :: map(), budget :: map()) ::
              {:ok, candidates :: [map()], consumed_budget :: map()}
              | :no_value
              | {:outcome, typed_outcome :: term()}

  @fields ~w(publisher_label publisher_domain public_url explanation)

  def validate_candidates(candidates, evidence) when is_list(candidates) do
    evidence_by_id = Map.new(evidence, &{value(&1, :id), &1})

    candidates
    |> Enum.reduce_while({:ok, []}, fn candidate, {:ok, accepted} ->
      with field when field in @fields <- value(candidate, :field_name),
           candidate_value when is_binary(candidate_value) <- value(candidate, :normalized_value),
           confidence when is_number(confidence) <- value(candidate, :confidence),
           refs when is_list(refs) and refs != [] <- value(candidate, :evidence_refs),
           transform when is_binary(transform) <- value(candidate, :transform),
           {:ok, cited} <- cited_evidence(refs, evidence_by_id),
           true <- valid_derivation?(field, candidate_value, transform, cited) do
        normalized = %{
          field_name: field,
          normalized_value: candidate_value,
          confidence: confidence,
          evidence_refs: refs,
          resolver_provenance: provenance(field, value(candidate, :resolver_provenance) || []),
          transform: transform,
          contradiction_status: value(candidate, :contradiction_status) || "none",
          selected: false
        }

        {:cont, {:ok, [normalized | accepted]}}
      else
        _ -> {:halt, {:error, :invented_or_uncited_value}}
      end
    end)
    |> case do
      {:ok, accepted} -> {:ok, Enum.reverse(accepted)}
      error -> error
    end
  end

  def validate_candidates(_, _), do: {:error, :invalid_candidate_shape}

  def derived?(field, candidate, transform, cited) when field in @fields,
    do: valid_derivation?(field, candidate, transform, cited)

  defp cited_evidence(refs, evidence_by_id) do
    cited = Enum.map(refs, &Map.get(evidence_by_id, &1))
    if Enum.all?(cited, & &1), do: {:ok, cited}, else: {:error, :missing_evidence_ref}
  end

  defp valid_derivation?("explanation", candidate, "interpretation", cited),
    do:
      is_binary(candidate) and candidate != "" and cited != [] and
        Enum.all?(cited, &(value(&1, :kind) in ~w(publisher_label publisher_domain public_url)))

  defp valid_derivation?(field, candidate, transform, cited) do
    Enum.any?(cited, fn item ->
      value(item, :kind) == field and
        case apply_transform(value(item, :value), transform) do
          {:ok, ^candidate} -> true
          _ -> false
        end
    end)
  end

  defp provenance("explanation", provenance),
    do: [%{"interpretation" => true} | provenance]

  defp provenance(_, provenance), do: provenance

  defp apply_transform(value, "copy") when is_binary(value), do: {:ok, value}
  defp apply_transform(value, "trim") when is_binary(value), do: {:ok, String.trim(value)}

  defp apply_transform(value, "lowercase") when is_binary(value),
    do: {:ok, value |> String.trim() |> String.downcase()}

  defp apply_transform(value, "uri_host") when is_binary(value) do
    case URI.parse(value) do
      %{host: host} when is_binary(host) -> {:ok, String.downcase(host)}
      _ -> :error
    end
  end

  defp apply_transform(value, "normalize_url") when is_binary(value) do
    trimmed = String.trim(value)

    case URI.parse(trimmed) do
      %URI{host: host} = uri when is_binary(host) ->
        {:ok, uri |> Map.put(:host, String.downcase(host)) |> URI.to_string()}

      _ ->
        :error
    end
  end

  defp apply_transform(_, _), do: :error

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
