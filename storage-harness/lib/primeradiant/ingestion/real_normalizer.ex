defmodule Primeradiant.Ingestion.RealNormalizer do
  @moduledoc false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StorageHarness.Input

  @normalizer_version "real_normalizer_v1"
  @stopwords MapSet.new(
               ~w(a an and are as at be by for from in is it of on or the to with without before after since into over under)
             )

  def normalize(%Input{} = input) do
    text = input.body_text || ""
    provenance = input.normalized || %{}
    claims = extract_claims(text)
    questions = extract_questions(text)
    colors = extract_colors(input, text)
    entities = extract_entities(text)
    topic_tokens = topic_tokens([input.title, text], claims, entities)

    %{
      input_id: input.id,
      input_ref: Admission.input_ref(input),
      tenant_id: input.tenant_id,
      source_type: input.source_type,
      external_id: input.external_id,
      observed_at: input.observed_at,
      retrieved_at: provenance["retrieved_at"],
      occurred_at: provenance["occurred_at"],
      title: input.title,
      body_text: text,
      acl: input.acl,
      provenance: %{
        ingestion_run_key: provenance["ingestion_run_key"],
        source_mode: provenance["source_mode"],
        source_name: provenance["source_name"],
        source_actor: provenance["source_actor"] || %{},
        canonical_uri: provenance["canonical_uri"],
        raw_object_uri: provenance["raw_object_uri"],
        content_sha256: input.content_sha256,
        normalizer_version: @normalizer_version,
        extractor_version: nil,
        trace_id: provenance["trace_id"]
      },
      extracted: %{
        claims: claims,
        entities: entities,
        events: extract_events(input, text),
        questions: questions,
        colors: colors,
        topic_tokens: topic_tokens,
        source_posture: source_posture(input, text)
      }
    }
  end

  def facts(envelope) do
    envelope.extracted.claims
    |> Enum.filter(&(&1.fact_key && &1.fact_value))
    |> Map.new(&{&1.fact_key, &1.fact_value})
  end

  def fact_scopes(envelope) do
    envelope.extracted.claims
    |> Enum.filter(&(&1.fact_key && &1.time_scope))
    |> Map.new(&{&1.fact_key, &1.time_scope})
  end

  defp extract_claims(text) do
    normalized = normalize_claim_phrases(text)

    Regex.scan(
      ~r/\b([a-z][a-z0-9_]{2,})\s+(?:is|was|remains|stays|became|becomes|moved to|set to)\s+([a-zA-Z0-9_.:-]+)(?:\s+(?:for|during|in|this)\s+([a-zA-Z0-9_.:-]+))?/,
      normalized
    )
    |> Enum.with_index()
    |> Enum.map(fn {[raw, key, value | rest], index} ->
      scope = List.first(rest)

      %{
        claim_key: "claim:#{key}:#{index}",
        text: raw,
        fact_key: key,
        fact_value: String.downcase(value),
        polarity: "positive",
        modality: "asserted",
        time_scope: scope || "current",
        span: span(text, raw)
      }
    end)
  end

  defp normalize_claim_phrases(text) do
    text
    |> String.downcase()
    |> String.replace(
      ~r/\bservice (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "service_state is \\1 for \\2"
    )
    |> String.replace(
      ~r/\broute (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "route is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bcrossings (?:are|were|remain|stayed|became|become) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "crossings is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bterminal staffing (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "terminal_staffing is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bquoted amount (?:is|was|remains|stays|became|becomes) \$?([0-9_.,-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "quoted_amount is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bdeadline (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "deadline is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bstatus (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "status is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bvenue (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "venue is \\1 for \\2"
    )
    |> String.replace(
      ~r/\bspeaker (?:is|was|remains|stays|became|becomes) ([a-z0-9_-]+)(?: (?:for|during|in|this) ([a-z0-9_-]+))?/,
      "speaker is \\1 for \\2"
    )
    |> String.replace(~r/\bBridge Service\b/i, "Bridge Service")
  end

  defp extract_questions(text) do
    text
    |> String.split(~r/(?<=\?)/)
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&String.ends_with?(&1, "?"))
    |> Enum.with_index()
    |> Enum.map(fn {question, index} ->
      %{
        question_key: "question:#{index}",
        text: question,
        status: "open",
        span: span(text, question)
      }
    end)
  end

  defp extract_colors(input, text) do
    posture = source_posture(input, text)

    if posture.kind in ["opinion", "firsthand_message"] do
      [
        %{
          color_key: "color:0",
          text: String.slice(text, 0, 180),
          framing_kind: posture.kind,
          span: %{start: 0, end: min(String.length(text), 180)}
        }
      ]
    else
      []
    end
  end

  defp extract_entities(text) do
    Regex.scan(~r/\b[A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3}/, text)
    |> Enum.map(&List.first/1)
    |> Enum.reject(&String.contains?(&1, "="))
    |> Enum.uniq()
    |> Enum.with_index()
    |> Enum.map(fn {name, index} ->
      %{
        entity_key: slug(name),
        kind: "named",
        name: name,
        aliases: [],
        span: span(text, name),
        position: index
      }
    end)
  end

  defp extract_events(input, text) do
    [
      %{
        event_key: "event:#{input.external_id}",
        text: input.title || String.slice(text, 0, 80),
        time_scope: "current",
        span: %{start: 0, end: min(String.length(text), 80)}
      }
    ]
  end

  defp source_posture(%{source_type: "opinion_column"}, _text),
    do: %{kind: "opinion", certainty: "high", attribution: "source_type"}

  defp source_posture(%{source_type: source_type}, _text)
       when source_type in ["email", "message", "user_note"],
       do: %{kind: "firsthand_message", certainty: "high", attribution: "source_type"}

  defp source_posture(_input, text) do
    cond do
      String.match?(String.downcase(text), ~r/\b(opinion|analysis|column)\b/) ->
        %{kind: "opinion", certainty: "medium", attribution: "text"}

      String.match?(String.downcase(text), ~r/\b(correction|corrected)\b/) ->
        %{kind: "correction", certainty: "medium", attribution: "text"}

      true ->
        %{kind: "reporting", certainty: "medium", attribution: "default"}
    end
  end

  defp topic_tokens(texts, claims, entities) do
    text_tokens =
      texts
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" ")
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9_\s=-]/, " ")
      |> String.split()
      |> Enum.map(&String.trim(&1, "_-= "))
      |> Enum.filter(&(String.length(&1) >= 3))
      |> Enum.reject(&MapSet.member?(@stopwords, &1))
      |> Enum.reject(&String.contains?(&1, "="))

    fact_tokens = Enum.flat_map(claims, &[&1.fact_key, &1.fact_value])
    entity_tokens = Enum.map(entities, & &1.entity_key)

    (text_tokens ++ fact_tokens ++ entity_tokens)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&slug/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp span(text, fragment) do
    start =
      :binary.match(text, fragment)
      |> case do
        {position, _length} -> position
        :nomatch -> 0
      end

    %{start: start, end: start + String.length(fragment)}
  end

  defp slug(value) do
    value
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
  end
end
