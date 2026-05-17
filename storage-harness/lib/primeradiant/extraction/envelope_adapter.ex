defmodule Primeradiant.Extraction.EnvelopeAdapter do
  @moduledoc false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StorageHarness.Input

  @stopwords MapSet.new(
               ~w(a an and are as at be by for from in is it of on or the to with without before after since into over under this that)
             )

  def to_envelope(%Input{} = input, result) when is_map(result) do
    claims = result.claims || []
    entities = result.entities || []
    framing = result.framing || []
    questions = result.questions || []
    input_ref = Admission.input_ref(input)
    evidence_by_id = Map.new(result.evidence_index || [], &{&1["evidence_id"], &1})

    %{
      input_id: input.id,
      input_ref: input_ref,
      tenant_id: input.tenant_id,
      source_type: input.source_type,
      external_id: input.external_id,
      observed_at: input.observed_at,
      retrieved_at: get_in(result.input_context, ["retrieved_at"]),
      occurred_at: get_in(result.input_context, ["occurred_at"]),
      title: input.title,
      body_text: input.body_text || "",
      acl: input.acl,
      provenance: %{
        ingestion_run_key: get_in(input.normalized, ["ingestion_run_key"]),
        source_mode: get_in(input.normalized, ["source_mode"]),
        source_name: get_in(result.input_context, ["source_name"]),
        source_actor: get_in(result.input_context, ["source_actor"]) || %{},
        canonical_uri: get_in(result.input_context, ["canonical_uri"]),
        raw_object_uri: get_in(input.normalized, ["raw_object_uri"]),
        content_sha256: input.content_sha256,
        normalizer_version: nil,
        extractor_version: get_in(result.extractor_run, ["extractor_version"]),
        contract_version: get_in(result.extractor_run, ["contract_version"]),
        trace_id: get_in(result.extractor_run, ["trace_id"])
      },
      extracted: %{
        claims: Enum.map(claims, &adapt_claim(&1, input_ref, evidence_by_id)),
        entities: Enum.map(entities, &adapt_entity(&1, input_ref, evidence_by_id)),
        events: events(input, result, input_ref, evidence_by_id),
        questions: Enum.map(questions, &adapt_question(&1, input_ref, evidence_by_id)),
        colors: Enum.map(framing, &adapt_framing(&1, input_ref, evidence_by_id)),
        topic_tokens: topic_tokens(input, claims, entities),
        source_posture: source_posture(result),
        source_stance:
          Enum.map(
            result.source_stance || [],
            &adapt_source_stance(&1, input_ref, evidence_by_id)
          ),
        framing: framing,
        quality: result.quality,
        evidence_index: result.evidence_index
      }
    }
  end

  defp adapt_source_stance(stance, input_ref, evidence_by_id) do
    Map.merge(stance, %{
      "evidence_refs" => span_refs(input_ref, stance["evidence_refs"]),
      "span" => span_from_evidence(stance["evidence_refs"], evidence_by_id)
    })
  end

  defp adapt_claim(claim, input_ref, evidence_by_id) do
    %{
      claim_key: claim["claim_id"],
      text: claim["text"],
      fact_key: claim["fact_key"],
      fact_value: claim["fact_value"],
      polarity: claim["polarity"],
      modality: claim["modality"],
      attribution: claim["attribution"],
      uncertainty: claim["uncertainty"],
      downstream_use: claim["downstream_use"],
      structural_candidate: claim["structural_candidate"],
      time_scope: get_in(claim, ["time_scope", "legacy_scope"]) || "current",
      rich_time_scope: claim["time_scope"],
      span: span_from_evidence(claim["evidence_refs"], evidence_by_id),
      evidence_refs: span_refs(input_ref, claim["evidence_refs"])
    }
  end

  defp adapt_entity(entity, input_ref, evidence_by_id) do
    %{
      entity_key: entity["canonical_key"],
      kind: entity["kind"],
      name: entity["name"],
      aliases: entity["aliases"] || [],
      role_in_source: entity["role_in_source"],
      evidence_refs: span_refs(input_ref, entity["evidence_refs"]),
      confidence: entity["confidence"],
      span: span_from_evidence(entity["evidence_refs"], evidence_by_id),
      position: nil
    }
  end

  defp adapt_question(question, input_ref, evidence_by_id) do
    %{
      question_key: question["question_id"],
      text: question["text"],
      status: question["status"] || "open",
      evidence_refs: span_refs(input_ref, question["evidence_refs"]),
      span: span_from_evidence(question["evidence_refs"], evidence_by_id)
    }
  end

  defp adapt_framing(item, input_ref, evidence_by_id) do
    %{
      color_key: item["framing_id"],
      text: item["text"],
      framing_kind: item["framing_kind"],
      promoted_to_structural_fact: false,
      evidence_refs: span_refs(input_ref, item["evidence_refs"]),
      confidence: item["confidence"],
      span: span_from_evidence(item["evidence_refs"], evidence_by_id)
    }
  end

  defp events(_input, result, input_ref, evidence_by_id) do
    (result.claims || [])
    |> Enum.filter(&(&1["structural_candidate"] == true))
    |> Enum.map(fn claim ->
      refs = claim["evidence_refs"] || []

      %{
        event_key: "event:#{claim["claim_id"]}",
        text: claim["text"],
        time_scope: get_in(claim, ["time_scope", "legacy_scope"]) || "current",
        evidence_refs: span_refs(input_ref, refs),
        span: span_from_evidence(refs, evidence_by_id)
      }
    end)
  end

  defp source_posture(result) do
    stance = List.first(result.source_stance || [])

    %{
      kind: (stance && stance["stance_kind"]) || "unknown",
      certainty: (stance && stance["certainty"]) || "low",
      attribution: "production_extractor_v1"
    }
  end

  defp topic_tokens(input, claims, entities) do
    claim_tokens =
      claims
      |> Enum.flat_map(&[&1["fact_key"], &1["fact_value"], &1["normalized_subject"]])

    entity_tokens = Enum.map(entities, & &1["canonical_key"])

    text_tokens =
      [input.title, input.body_text]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" ")
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9_\s=-]/, " ")
      |> String.split()
      |> Enum.map(&String.trim(&1, "_-= "))
      |> Enum.filter(&(String.length(&1) >= 3))
      |> Enum.reject(&MapSet.member?(@stopwords, &1))
      |> Enum.reject(&String.contains?(&1, "="))

    (claim_tokens ++ entity_tokens ++ text_tokens)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&slug/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp span_refs(input_ref, refs) do
    Enum.map(refs || [], &%{"input_ref" => input_ref, "evidence_id" => &1})
  end

  defp span_from_evidence([ref | _], evidence_by_id) do
    case Map.get(evidence_by_id, ref) do
      %{"span_start" => start, "span_end" => finish, "source" => source} ->
        %{start: start, end: finish, source: source}

      _ ->
        %{start: 0, end: 0}
    end
  end

  defp span_from_evidence(_refs, _evidence_by_id), do: %{start: 0, end: 0}

  defp slug(value) do
    value
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
  end
end
