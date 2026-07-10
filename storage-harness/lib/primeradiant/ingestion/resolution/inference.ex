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
  alias Primeradiant.Ingestion.Resolution.Transforms

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
           {:ok, derivation} <- derivation_evidence(field, candidate_value, transform, cited) do
        normalized = %{
          field_name: field,
          normalized_value: candidate_value,
          confidence: confidence,
          evidence_refs: refs,
          derivation_evidence_ref: value(derivation, :id),
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
    do: match?({:ok, _}, derivation_evidence(field, candidate, transform, cited))

  defp cited_evidence(refs, evidence_by_id) do
    cited = Enum.map(refs, &Map.get(evidence_by_id, &1))
    if Enum.all?(cited, & &1), do: {:ok, cited}, else: {:error, :missing_evidence_ref}
  end

  defp derivation_evidence("explanation", candidate, "interpretation", cited) do
    if is_binary(candidate) and candidate != "" and cited != [] and
         Enum.all?(cited, fn item ->
           value(item, :kind) in ~w(publisher_label publisher_domain public_url fetched_response_canonical_url fetched_response_accepted_final_url fetched_response_og_site_name non_public_access)
         end),
       do: {:ok, cited |> Enum.sort_by(&value(&1, :id)) |> hd()},
       else: :error
  end

  defp derivation_evidence("public_url", "no_public_url", "explicit_no_public_url", cited) do
    case Enum.find(cited, &(value(&1, :kind) == "non_public_access")) do
      nil -> :error
      item -> {:ok, item}
    end
  end

  defp derivation_evidence(field, candidate, transform, cited) do
    case Enum.find(cited, fn item ->
           evidence_supports_field?(value(item, :kind), field) and
             case Transforms.apply(value(item, :value), transform) do
               {:ok, ^candidate} -> true
               _ -> false
             end
         end) do
      nil -> :error
      item -> {:ok, item}
    end
  end

  defp evidence_supports_field?(kind, "public_url"),
    do: kind in ~w(public_url fetched_response_canonical_url fetched_response_accepted_final_url)

  defp evidence_supports_field?(kind, "publisher_label"),
    do: kind in ~w(publisher_label fetched_response_og_site_name)

  defp evidence_supports_field?(kind, field), do: kind == field

  defp provenance("explanation", provenance),
    do: [%{"interpretation" => true} | provenance]

  defp provenance(_, provenance), do: provenance

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
