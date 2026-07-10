defmodule Primeradiant.Ingestion.Resolution.Eligibility do
  @moduledoc """
  Deterministic T1656-SR-01 eligibility validation.

  Resolver evidence kinds are the precedence vocabulary:

    * `fetched_response_canonical_url` — rank-one article URL. Its locator contains
      `response_url`, the accepted final URL for the bounded fetched response.
    * `fetched_response_accepted_final_url` — rank-two article URL.
    * `public_url` — rank-three raw/feed item link extracted from the raw envelope
      by the normalizer.
    * `fetched_response_og_site_name` — rank-one publisher label. Its locator
      contains `response_url`.
    * `publisher_label` — rank-two raw/feed publisher label extracted from the
      raw envelope by the normalizer.
    * `publisher_domain` — raw/feed supplied publisher domain extracted from the
      raw envelope by the normalizer.
    * `non_public_access` — a raw-envelope source declaration for the explicit
      `no_public_url` enum.

  Registrable-domain normalization is intentionally the initial deterministic
  transform only: lowercase the URI host and remove one leading `www.`. It does
  not use or approximate a public-suffix list.
  """

  alias Primeradiant.Ingestion.Resolution.Transforms
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb, ResolvedSourceField}

  @validator_version "eligibility_sr01_v1"
  @eligibility_constants_version "precedence_thresholds_v1"
  @url_kinds %{
    "fetched_response_canonical_url" => 1,
    "fetched_response_accepted_final_url" => 2,
    "public_url" => 3
  }
  @label_kinds %{"fetched_response_og_site_name" => 1, "publisher_label" => 2}
  @thresholds %{
    "publisher_label" => Decimal.new("0.80"),
    "publisher_domain" => Decimal.new("0.90"),
    "public_url" => Decimal.new("0.90"),
    "explanation" => Decimal.new("0.80")
  }

  def validator_version, do: Enum.join([@validator_version, @eligibility_constants_version], "+")

  def evaluate(db_path, resolution_case, registration) do
    raw_envelope =
      DurableSoupDb.raw_envelope(
        db_path,
        resolution_case.tenant_id,
        resolution_case.raw_envelope_id
      )

    evidence =
      DurableSoupDb.resolution_evidence_for_case(
        db_path,
        resolution_case.tenant_id,
        resolution_case.id
      )

    fields =
      DurableSoupDb.resolved_source_fields_for_case(
        db_path,
        resolution_case.tenant_id,
        resolution_case.id
      )

    evidence_by_id = Map.new(evidence, &{&1.id, &1})
    policy = registration.resolution_policy || %{}

    case value(policy, "source_class") do
      "public_article" ->
        validate_public_article(fields, evidence_by_id, resolution_case, raw_envelope.visibility)

      "source_record_no_public_url" ->
        validate_no_public_url(
          fields,
          evidence_by_id,
          resolution_case,
          policy,
          raw_envelope.visibility
        )

      _ ->
        {:non_eligible, "unresolved:policy_not_registered", []}
    end
  end

  def select!(db_path, fields) do
    winner_ids = MapSet.new(fields, & &1.id)
    winner = hd(fields)

    Enum.each(fields, &DurableSoupDb.put_resolved_source_field!(db_path, &1))

    db_path
    |> DurableSoupDb.resolved_source_fields_for_case(winner.tenant_id, winner.resolution_case_id)
    |> Enum.map(fn field ->
      field
      |> ChangesetStore.update!(%{selected: MapSet.member?(winner_ids, field.id)})
      |> then(&DurableSoupDb.put_resolved_source_field!(db_path, &1))
    end)
  end

  def registrable_domain(url_or_host) when is_binary(url_or_host) do
    Transforms.registrable_domain(url_or_host)
  end

  defp validate_public_article(fields, evidence, resolution_case, visibility) do
    with {:ok, url} <-
           select_precedence(
             fields,
             evidence,
             "public_url",
             @url_kinds,
             &url_relation/2,
             visibility
           ),
         {:ok, domain} <- select_domain(fields, evidence, url, resolution_case, visibility),
         {:ok, label} <- select_label(fields, evidence, url, domain, visibility),
         {:ok, explanation} <-
           select_explanation(fields, evidence, [label, domain, url], resolution_case, visibility) do
      {:eligible, [label, domain, url, explanation]}
    else
      {:error, :field_conflict} -> {:non_eligible, "unresolved:field_conflict", []}
      {:error, :insufficient_evidence} -> {:non_eligible, "unresolved:insufficient_evidence", []}
    end
  end

  defp validate_no_public_url(fields, evidence, resolution_case, policy, visibility) do
    authorized? = value(policy, "allow_no_public_url") == true

    no_url =
      Enum.find(fields, fn field ->
        field.field_name == "public_url" and field.normalized_value == "no_public_url" and
          valid_candidate?(field, evidence, "public_url", visibility) and
          privileged_evidence?(derivation_evidence(field, evidence))
      end)

    cond do
      not authorized? ->
        {:non_eligible, "unresolved:policy_not_registered", []}

      is_nil(no_url) ->
        {:non_eligible, "refused:no_public_article", []}

      true ->
        with {:ok, domain} <- select_required(fields, evidence, "publisher_domain", visibility),
             {:ok, label} <- select_no_url_label(fields, evidence, domain, visibility),
             {:ok, explanation} <-
               select_explanation(
                 fields,
                 evidence,
                 [label, domain, no_url],
                 resolution_case,
                 visibility
               ) do
          {:eligible, [label, domain, no_url, explanation]}
        else
          {:error, :field_conflict} ->
            {:non_eligible, "unresolved:field_conflict", []}

          {:error, :insufficient_evidence} ->
            {:non_eligible, "unresolved:insufficient_evidence", []}
        end
    end
  end

  defp select_precedence(fields, evidence, field_name, ranks, relation, visibility) do
    candidates =
      Enum.filter(fields, fn field ->
        field.field_name == field_name and
          valid_candidate?(field, evidence, field_name, visibility)
      end)

    ranked =
      for candidate <- candidates,
          item = derivation_evidence(candidate, evidence),
          not is_nil(item),
          privileged_evidence?(item),
          rank = Map.get(ranks, item.kind),
          rank do
        {rank, candidate, item}
      end

    case ranked do
      [] ->
        {:error, :insufficient_evidence}

      _ ->
        rank = ranked |> Enum.map(&elem(&1, 0)) |> Enum.min()
        applicable = Enum.filter(ranked, &(elem(&1, 0) == rank))

        cond do
          Enum.any?(applicable, fn {_rank, candidate, item} -> not relation.(candidate, item) end) ->
            {:error, :field_conflict}

          applicable
          |> Enum.map(fn {_, candidate, _} -> candidate.normalized_value end)
          |> Enum.uniq()
          |> length() > 1 ->
            {:error, :field_conflict}

          true ->
            {_rank, candidate, _item} =
              Enum.min_by(applicable, fn {_, candidate, _} -> candidate.id end)

            {:ok, candidate}
        end
    end
  end

  defp select_domain(fields, evidence, url, resolution_case, visibility) do
    selected_domain = registrable_domain(url.normalized_value)

    supplied =
      Enum.filter(fields, fn field ->
        field.field_name == "publisher_domain" and
          valid_candidate?(field, evidence, "publisher_domain", visibility)
      end)

    cond do
      Enum.any?(supplied, &(registrable_domain(&1.normalized_value) != selected_domain)) ->
        {:error, :field_conflict}

      supplied == [] ->
        {:ok, derived_domain(resolution_case, url, selected_domain)}

      true ->
        same_value(supplied, evidence, "publisher_domain", visibility)
    end
  end

  defp select_label(fields, evidence, url, domain, visibility) do
    select_precedence(
      fields,
      evidence,
      "publisher_label",
      @label_kinds,
      fn candidate, item ->
        label_relation(candidate, item, evidence, url, domain)
      end,
      visibility
    )
  end

  defp select_no_url_label(fields, evidence, domain, visibility) do
    candidates = Enum.filter(fields, &(&1.field_name == "publisher_label"))

    case Enum.filter(candidates, &feed_label_domain?(&1, evidence, domain.normalized_value)) do
      [] -> {:error, :insufficient_evidence}
      valid -> same_value(valid, evidence, "publisher_label", visibility)
    end
  end

  defp select_required(fields, evidence, field_name, visibility) do
    fields
    |> Enum.filter(&(&1.field_name == field_name))
    |> same_value(evidence, field_name, visibility)
  end

  defp same_value([], _evidence, _field, _visibility), do: {:error, :insufficient_evidence}

  defp same_value(candidates, evidence, field, visibility) do
    valid = Enum.filter(candidates, &valid_candidate?(&1, evidence, field, visibility))

    cond do
      valid == [] ->
        {:error, :insufficient_evidence}

      valid |> Enum.map(& &1.normalized_value) |> Enum.uniq() |> length() > 1 ->
        {:error, :field_conflict}

      true ->
        {:ok, Enum.min_by(valid, & &1.id)}
    end
  end

  defp select_explanation(fields, evidence, selected, resolution_case, visibility) do
    refs = selected |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq() |> Enum.sort()

    explanations = Enum.filter(fields, &(&1.field_name == "explanation"))

    inferred =
      Enum.find(explanations, fn field ->
        field.transform == "interpretation" and
          valid_candidate?(field, evidence, "explanation", visibility) and
          Enum.sort(field.evidence_refs) == Enum.sort(refs)
      end)

    cond do
      inferred -> {:ok, inferred}
      true -> {:ok, deterministic_explanation(resolution_case, selected, refs)}
    end
  end

  defp deterministic_explanation(resolution_case, selected, refs) do
    provenance = selected |> Enum.flat_map(& &1.resolver_provenance) |> Enum.uniq()

    ChangesetStore.insert!(ResolvedSourceField, %{
      id: deterministic_id([resolution_case.id, "explanation", Enum.join(refs, ",")]),
      tenant_id: resolution_case.tenant_id,
      resolution_case_id: resolution_case.id,
      field_name: "explanation",
      normalized_value:
        "Selected publisher identity and public access from evidence refs: " <>
          Enum.join(refs, ", "),
      confidence: 1.0,
      evidence_refs: refs,
      derivation_evidence_ref: hd(refs),
      resolver_provenance: provenance,
      transform: "deterministic_template",
      contradiction_status: "none",
      selected: false
    })
  end

  defp derived_domain(resolution_case, url, domain) do
    ChangesetStore.insert!(ResolvedSourceField, %{
      id:
        deterministic_id([
          resolution_case.id,
          "publisher_domain",
          domain,
          url.evidence_refs |> Enum.sort() |> Enum.join(",")
        ]),
      tenant_id: resolution_case.tenant_id,
      resolution_case_id: resolution_case.id,
      field_name: "publisher_domain",
      normalized_value: domain,
      confidence: 0.9,
      evidence_refs: url.evidence_refs,
      derivation_evidence_ref: url.derivation_evidence_ref,
      resolver_provenance: url.resolver_provenance,
      transform: "registrable_domain",
      contradiction_status: "none",
      selected: false
    })
  end

  defp valid_candidate?(field, evidence, field_name, visibility) do
    cited = candidate_evidence(field, evidence)
    derivation = derivation_evidence(field, evidence)

    field.normalized_value != "" and normalized?(field_name, field.normalized_value) and
      Decimal.compare(field.confidence, Map.fetch!(@thresholds, field_name)) in [:eq, :gt] and
      confidence_class?(field, evidence) and
      field.evidence_refs != [] and field.resolver_provenance != [] and
      Enum.all?(field.resolver_provenance, &(is_map(&1) and map_size(&1) > 0)) and
      Enum.all?(field.evidence_refs, &Map.has_key?(evidence, &1)) and
      field.derivation_evidence_ref in field.evidence_refs and
      Enum.all?(cited, fn item ->
        is_map(item.provenance) and map_size(item.provenance) > 0 and
          item.visibility == visibility
      end) and
      not is_nil(derivation) and derivation_valid?(field, derivation) and
      provenance_bound?(field, derivation) and
      (field.transform != "interpretation" or
         Enum.any?(field.resolver_provenance, &(value(&1, "interpretation") == true))) and
      is_binary(field.transform) and field.transform != "" and
      field.contradiction_status == "none"
  end

  defp derivation_valid?(%{transform: "interpretation"}, _item), do: true
  defp derivation_valid?(%{transform: "deterministic_template"}, _item), do: true

  defp derivation_valid?(
         %{
           field_name: "public_url",
           normalized_value: "no_public_url",
           transform: "explicit_no_public_url"
         },
         %{kind: "non_public_access"} = item
       ),
       do: privileged_evidence?(item)

  defp derivation_valid?(field, item) do
    evidence_supports_field?(item.kind, field.field_name) and privileged_evidence?(item) and
      match?(
        {:ok, value} when value == field.normalized_value,
        Transforms.apply(item.value, field.transform)
      )
  end

  defp evidence_supports_field?(kind, "public_url"),
    do: kind in ~w(public_url fetched_response_canonical_url fetched_response_accepted_final_url)

  defp evidence_supports_field?(kind, "publisher_label"),
    do: kind in ~w(publisher_label fetched_response_og_site_name)

  defp evidence_supports_field?(kind, field), do: kind == field

  defp provenance_bound?(field, %{source: "raw_envelope", provenance: provenance}),
    do: provenance_identity_in_field?(field, provenance, "adapter")

  defp provenance_bound?(field, %{provenance: provenance}),
    do: provenance_identity_in_field?(field, provenance, "resolver")

  defp provenance_identity_in_field?(field, provenance, key) do
    identity = value(provenance, key)
    is_binary(identity) and Enum.any?(field.resolver_provenance, &(value(&1, key) == identity))
  end

  defp normalized?("publisher_label", value), do: value == String.trim(value)
  defp normalized?("publisher_domain", value), do: value == registrable_domain(value)
  defp normalized?("public_url", "no_public_url"), do: true

  defp normalized?("public_url", value) do
    case URI.parse(value) do
      %URI{scheme: scheme, host: host} when scheme in ["http", "https"] and is_binary(host) ->
        host == String.downcase(host)

      _ ->
        false
    end
  end

  defp normalized?("explanation", value), do: value == String.trim(value)

  defp url_relation(candidate, %{kind: "fetched_response_canonical_url"} = item) do
    response_url = value(item.locator, "response_url")

    is_binary(response_url) and
      registrable_domain(response_url) == registrable_domain(candidate.normalized_value)
  end

  defp url_relation(candidate, %{kind: "fetched_response_accepted_final_url"} = item) do
    response_url = value(item.locator, "response_url")
    is_binary(response_url) and candidate.normalized_value == response_url
  end

  defp url_relation(_candidate, _item), do: true

  defp label_relation(
         candidate,
         %{kind: "fetched_response_og_site_name"} = item,
         _evidence,
         url,
         _domain
       ) do
    response_url = value(item.locator, "response_url")

    candidate.normalized_value == String.trim(item.value || "") and is_binary(response_url) and
      registrable_domain(response_url) == registrable_domain(url.normalized_value)
  end

  defp label_relation(candidate, _item, evidence, _url, domain),
    do: feed_label_domain?(candidate, evidence, domain.normalized_value)

  defp feed_label_domain?(candidate, evidence, domain) do
    Enum.any?(Map.values(evidence), fn item ->
      item.kind == "publisher_domain" and
        registrable_domain(item.value || "") == registrable_domain(domain) and
        same_raw_scope?(candidate_evidence(candidate, evidence), item)
    end)
  end

  defp same_raw_scope?(label_evidence, domain_evidence) do
    Enum.any?(label_evidence, fn label ->
      label.source == domain_evidence.source and label.provenance == domain_evidence.provenance
    end)
  end

  defp candidate_evidence(field, evidence),
    do: Enum.map(field.evidence_refs, &Map.get(evidence, &1)) |> Enum.reject(&is_nil/1)

  defp derivation_evidence(field, evidence), do: Map.get(evidence, field.derivation_evidence_ref)

  defp confidence_class?(%{transform: "interpretation", confidence: confidence}, _evidence),
    do: Decimal.equal?(confidence, Decimal.new("0.80"))

  defp confidence_class?(field, evidence) do
    expected =
      case derivation_evidence(field, evidence) do
        %{kind: kind}
        when kind in ~w(fetched_response_canonical_url fetched_response_accepted_final_url) ->
          Decimal.new("0.90")

        _ ->
          if field.transform == "registrable_domain",
            do: Decimal.new("0.90"),
            else: Decimal.new("1.00")
      end

    Decimal.equal?(field.confidence, expected)
  end

  defp privileged_evidence?(%{kind: kind} = item)
       when kind in ~w(fetched_response_canonical_url fetched_response_accepted_final_url fetched_response_og_site_name) do
    item.source == "resolver_response" and is_binary(value(item.locator, "response_url")) and
      resolver_provenance?(item.provenance)
  end

  defp privileged_evidence?(%{kind: "non_public_access"} = item) do
    item.source == "raw_envelope" and
      value(item.locator, "non_public_reason") == "source_declared" and
      item.value == "declared_non_public"
  end

  defp privileged_evidence?(%{kind: kind, source: "raw_envelope"})
       when kind in ~w(public_url publisher_label publisher_domain),
       do: true

  defp privileged_evidence?(_), do: false

  defp resolver_provenance?(provenance) when is_map(provenance),
    do: is_binary(value(provenance, "resolver"))

  defp resolver_provenance?(_), do: false

  defp deterministic_id(parts),
    do: parts |> Enum.map(&to_string/1) |> Enum.join("|") |> ChangesetStore.hash()

  defp value(nil, _key), do: nil
  defp value(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))
end
