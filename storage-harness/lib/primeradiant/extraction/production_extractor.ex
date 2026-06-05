defmodule Primeradiant.Extraction.ProductionExtractor do
  @moduledoc false

  alias Primeradiant.Extraction.Contract
  alias Primeradiant.StorageHarness.{ChangesetStore, Input}
  alias __MODULE__.EvidenceBuilder

  @extractor_version "production_extractor_v1_deterministic_20260517"
  @copulas ~w(is are was were remains remain stayed stays became becomes set)
  @structural_fact_keys ~w(service_state route crossings terminal_staffing venue hours deadline owner amount quoted_amount status battery location triage decision task assignee speaker availability ownership responsibility affected_group group date actor)
  @entity_kinds ~w(person org place product project account agency event document unknown)
  @boilerplate ~r/\b(subscribe|advertisement|share this|related articles|recommended|privacy policy|terms of service|unsubscribe|copyright|all rights reserved|cookie settings|sign up)\b/i
  @unsupported_language ~r/[^\x00-\x7F\x{00A0}\x{2018}\x{2019}\x{201C}\x{201D}\x{2013}\x{2014}\x{2026}]/u

  def extract(%Input{} = input, opts \\ []) do
    result = build_result(input, opts)

    case Contract.validate(input, result) do
      {:ok, result} ->
        {:ok, result}

      {:error, changeset} ->
        fallback =
          input
          |> invalid_extraction_result(now_iso(), "extractor_error", inspect(changeset.errors))

        Contract.validate(input, fallback)
    end
  end

  defp build_result(input, opts) do
    started_at = now_iso()
    text = canonical_text(input)
    content = content_segments(text)
    evidence = EvidenceBuilder.new(input)

    cond do
      String.trim(text) == "" ->
        refused_result(input, evidence, started_at, "no_readable_text")

      String.match?(text, @unsupported_language) ->
        refused_result(input, evidence, started_at, "unsupported_language")

      byte_size(String.trim(text)) < 40 ->
        low_confidence_result(input, evidence, started_at, "text_too_short")

      content == [] ->
        low_confidence_result(input, evidence, started_at, "boilerplate_only")

      true ->
        extracted = extract_content(input, content, evidence)
        quality = quality_for(extracted)

        if quality["status"] in ["low_confidence", "refused"] do
          empty_result(
            input,
            evidence,
            started_at,
            quality["status"],
            quality["confidence"],
            quality["reasons"]
          )
        else
          %{
            "input_id" => input.id,
            "tenant_id" => input.tenant_id,
            "extractor_run" => run(input, opts, started_at),
            "input_context" => input_context(input),
            "quality" => quality,
            "claims" => extracted.claims,
            "entities" => extracted.entities,
            "times" => extracted.times,
            "source_stance" => extracted.source_stance,
            "framing" => extracted.framing,
            "questions" => extracted.questions,
            "evidence_index" => EvidenceBuilder.index(extracted.evidence)
          }
        end
    end
  end

  defp refused_result(input, evidence, started_at, reason) do
    empty_result(input, evidence, started_at, "refused", 0.0, [reason])
  end

  defp low_confidence_result(input, evidence, started_at, reason) do
    empty_result(input, evidence, started_at, "low_confidence", 0.24, [reason])
  end

  defp invalid_extraction_result(input, started_at, reason, warning) do
    input
    |> empty_result(EvidenceBuilder.new(input), started_at, "refused", 0.0, [reason])
    |> put_in(["quality", "warnings"], [warning])
  end

  defp empty_result(input, evidence, started_at, status, confidence, reasons) do
    %{
      "input_id" => input.id,
      "tenant_id" => input.tenant_id,
      "extractor_run" => run(input, [], started_at),
      "input_context" => input_context(input),
      "quality" => %{
        "status" => status,
        "confidence" => confidence,
        "reasons" => reasons,
        "warnings" => []
      },
      "claims" => [],
      "entities" => [],
      "times" => [],
      "source_stance" => [],
      "framing" => [],
      "questions" => [],
      "evidence_index" => EvidenceBuilder.index(evidence)
    }
  end

  defp extract_content(input, segments, evidence) do
    body = Enum.map_join(segments, " ", & &1.text)
    sentences = split_sentences(body)
    source_actor = get_in(input.normalized, ["source_actor"]) || %{}
    source_type = input.source_type

    {claims, evidence} = extract_claims(input, sentences, evidence)
    {questions, evidence} = extract_questions(input, sentences, evidence)
    {framing, evidence} = extract_framing(input, sentences, claims, evidence)
    {entities, evidence} = extract_entities(input, body, claims, evidence)
    {times, evidence} = extract_times(input, sentences, claims, evidence)
    {source_stance, evidence} = extract_stance(input, claims, framing, evidence)

    source_actor_entity =
      if source_actor["name"] do
        actor_entity(input, source_actor, evidence)
      end

    entities =
      [source_actor_entity | entities]
      |> Enum.reject(&is_nil/1)
      |> uniq_by("canonical_key")

    %{
      source_type: source_type,
      claims: claims,
      questions: questions,
      framing: framing,
      entities: entities,
      times: times,
      source_stance: source_stance,
      evidence: evidence,
      stale_check?: stale_check?(body)
    }
  end

  defp extract_claims(input, sentences, evidence) do
    sentences
    |> Enum.reject(&boilerplate?/1)
    |> Enum.reduce({[], evidence}, fn sentence, {claims, evidence} ->
      sentence_claims(sentence)
      |> Enum.reduce({claims, evidence}, fn raw_claim, {claims, evidence} ->
        evidence_ref =
          EvidenceBuilder.ref(evidence, input, Map.get(raw_claim, :evidence_text, sentence))

        evidence = EvidenceBuilder.put(evidence, evidence_ref)
        claim = claim_from(input, raw_claim, sentence, length(claims), evidence_ref)
        {claims ++ [claim], evidence}
      end)
    end)
  end

  defp sentence_claims(sentence) do
    normalized = sentence |> normalize_predicate_phrases() |> normalize_spaces()

    subject_claims =
      Regex.scan(
        ~r/\b(?<subject>[A-Z][A-Za-z0-9&' -]{1,70}?)\s+(?<predicate>[a-z][a-z0-9_]{2,35})\s+(?<copula>is|are|was|were|remains|remain|stays|stayed|became|becomes|set to|moved to)\s+(?<value>[A-Za-z0-9_.:$%-]+)(?:\s+(?:for|during|in|by|since)\s+(?<scope>[A-Za-z0-9_.:-]+))?/,
        normalized,
        capture: :all_names
      )
      |> Enum.map(fn [copula, predicate, scope, subject, value] ->
        %{
          subject: subject,
          predicate: predicate,
          copula: copula,
          value: value,
          scope: empty_to_nil(scope)
        }
      end)

    verb_object_claims =
      Regex.scan(
        ~r/\b(?<subject>[A-Z][A-Za-z0-9&' -]{1,70}?)\s+(?<value>halted|restored|opened|closed|paused|blocked|approved|rejected|resumed)\s+(?<predicate>service|triage|status|route|task|decision)(?:\s+(?:for|during|in|by|since|this)\s+(?<scope>[A-Za-z0-9_.:-]+))?/,
        normalized,
        capture: :all_names
      )
      |> Enum.map(fn [predicate, scope, subject, value] ->
        %{
          subject: subject,
          predicate: predicate,
          copula: nil,
          value: value,
          scope: empty_to_nil(scope)
        }
      end)

    compact_claims =
      Regex.scan(
        ~r/(?:^|[.;]\s+|\s+)(?<predicate>service|route|venue|hours|deadline|owner|amount|status|battery|location|triage|decision|task|assignee|speaker|quoted_amount)\s+(?<value>[A-Za-z0-9_.:$%-]+)(?:\s+(?:for|during|in|by|since|this)\s+(?<scope>[A-Za-z0-9_.:-]+))?/,
        normalized,
        capture: :all_names
      )
      |> Enum.map(fn [predicate, scope, value] ->
        %{
          subject: nil,
          predicate: predicate,
          copula: nil,
          value: value,
          scope: empty_to_nil(scope)
        }
      end)

    launch_date_claims =
      Regex.scan(
        ~r/\b(?<subject>[A-Z][A-Za-z0-9&' :.-]{1,90}?)\s+(?<predicate>launches|launch date(?: is| set for)?|release date(?: is| set for)?)\s+(?<value>(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\.?\s+\d{1,2},?\s+\d{4})\b/i,
        normalized,
        capture: [:subject, :predicate, :value]
      )
      |> Enum.map(fn [subject, predicate, value] ->
        %{
          subject: subject,
          predicate: "date",
          copula: nil,
          value: date_value(value),
          scope: "launch",
          evidence_text: "#{subject} #{predicate} #{value}"
        }
      end)

    standalone_claims =
      Regex.scan(
        ~r/(?:^|[.;]\s+|\s+)(?<predicate>[a-z][a-z0-9_]{2,35})\s+(?<copula>is|are|was|were|remains|remain|stays|stayed|became|becomes|set to|moved to)\s+(?<value>[A-Za-z0-9_.:$%-]+)(?:\s+(?:for|during|in|by|since)\s+(?<scope>[A-Za-z0-9_.:-]+))?/,
        normalized,
        capture: :all_names
      )
      |> Enum.map(fn [copula, predicate, scope, value] ->
        %{
          subject: nil,
          predicate: predicate,
          copula: copula,
          value: value,
          scope: empty_to_nil(scope)
        }
      end)
      |> Enum.reject(fn claim ->
        Enum.any?(
          subject_claims ++ verb_object_claims ++ compact_claims,
          &(&1.predicate == claim.predicate and &1.value == claim.value)
        )
      end)

    (subject_claims ++
       verb_object_claims ++
       compact_claims ++
       launch_date_claims ++
       standalone_claims)
    |> Enum.reject(&bad_predicate?/1)
    |> Enum.uniq_by(&{fact_key(&1.predicate), fact_value(&1.value)})
  end

  defp claim_from(input, raw, sentence, index, evidence_ref) do
    stance = claim_stance(input, sentence)
    key = fact_key(raw.predicate)
    value = fact_value(raw.value)
    time_scope = time_scope(input, raw.scope, sentence)
    structural? = structural_claim?(stance, key, value)

    %{
      "claim_id" => "claim:#{index}",
      "text" => sentence,
      "normalized_subject" => raw.subject && slug(raw.subject),
      "predicate" => key,
      "object_value" => value,
      "fact_key" => if(structural?, do: key),
      "fact_value" => if(structural?, do: value),
      "claim_kind" => claim_kind(key, stance),
      "polarity" => stance.polarity,
      "modality" => stance.modality,
      "attribution" => %{
        "asserted_by" => stance.asserted_by,
        "actor_ref" => nil,
        "attribution_text" => stance.attribution_text,
        "anonymous" => stance.anonymous
      },
      "time_scope" => time_scope,
      "uncertainty" => uncertainty(stance, time_scope),
      "evidence_refs" => [evidence_ref["evidence_id"]],
      "structural_candidate" => structural?,
      "downstream_use" => downstream_use(structural?, stance)
    }
  end

  defp structural_claim?(stance, key, value) do
    key in @structural_fact_keys and value not in [nil, ""] and
      stance.modality in ["asserted", "reported", "corrected", "planned"] and
      stance.polarity == "positive"
  end

  defp downstream_use(true, %{modality: "corrected"}), do: "conflict_candidate"
  defp downstream_use(true, _stance), do: "identity_candidate"

  defp downstream_use(false, %{modality: modality})
       when modality in ["opinion", "alleged", "denied"], do: "framing_only"

  defp downstream_use(false, _stance), do: "background"

  defp claim_stance(input, sentence) do
    down = String.downcase(sentence)

    cond do
      Regex.match?(~r/\b(denied|denies|did not|does not|not approved|no approval)\b/, down) ->
        %{
          polarity: "negative",
          modality: "denied",
          asserted_by: actor_assertion(input),
          anonymous: false,
          attribution_text: matched_text(sentence, ~r/\b(denied|denies|did not|does not)\b/i)
        }

      Regex.match?(~r/\b(may|might|could|possibly|rumor|rumoured|rumored)\b/, down) ->
        %{
          polarity: "unknown",
          modality: "possible",
          asserted_by: quoted_or_unknown(sentence),
          anonymous: Regex.match?(~r/\b(source|sources)\b/, down),
          attribution_text:
            matched_text(sentence, ~r/\b(may|might|could|source said|sources said)\b/i)
        }

      Regex.match?(~r/\b(plans to|planned to|will|set to)\b/, down) ->
        %{
          polarity: "positive",
          modality: "planned",
          asserted_by: quoted_or_unknown(sentence),
          anonymous: false,
          attribution_text: matched_text(sentence, ~r/\b(plans to|planned to|will|set to)\b/i)
        }

      Regex.match?(~r/\b(correction|corrected|revised)\b/, down) ->
        %{
          polarity: "positive",
          modality: "corrected",
          asserted_by: "publisher",
          anonymous: false,
          attribution_text: matched_text(sentence, ~r/\b(correction|corrected|revised)\b/i)
        }

      Regex.match?(~r/\b(said|according to|told|quoted|reported)\b/, down) ->
        %{
          polarity: "positive",
          modality: "reported",
          asserted_by: quoted_or_unknown(sentence),
          anonymous: Regex.match?(~r/\b(anonymous source|source said|sources said)\b/, down),
          attribution_text: matched_text(sentence, ~r/\b(said|according to|told|reported)\b/i)
        }

      true ->
        %{
          polarity: "positive",
          modality: "asserted",
          asserted_by: actor_assertion(input),
          anonymous: false,
          attribution_text: nil
        }
    end
  end

  defp extract_questions(input, sentences, evidence) do
    sentences
    |> Enum.filter(&String.ends_with?(&1, "?"))
    |> Enum.reduce({[], evidence}, fn question, {questions, evidence} ->
      evidence_ref = EvidenceBuilder.ref(evidence, input, question)
      evidence = EvidenceBuilder.put(evidence, evidence_ref)

      item = %{
        "question_id" => "question:#{length(questions)}",
        "text" => question,
        "status" => "open",
        "evidence_refs" => [evidence_ref["evidence_id"]]
      }

      {questions ++ [item], evidence}
    end)
  end

  defp extract_framing(input, sentences, claims, evidence) do
    sentences
    |> Enum.filter(&(opinion_source?(input, &1) or framing_kind(&1) != nil))
    |> Enum.reduce({[], evidence}, fn sentence, {items, evidence} ->
      evidence_ref = EvidenceBuilder.ref(evidence, input, sentence)
      evidence = EvidenceBuilder.put(evidence, evidence_ref)

      item = %{
        "framing_id" => "framing:#{length(items)}",
        "text" => sentence,
        "framing_kind" => framing_kind(sentence) || "other",
        "target_claim_ids" => target_claims(sentence, claims),
        "promoted_to_structural_fact" => false,
        "evidence_refs" => [evidence_ref["evidence_id"]],
        "confidence" => 0.88
      }

      {items ++ [item], evidence}
    end)
  end

  defp extract_entities(input, body, claims, evidence) do
    {entities, evidence} =
      Regex.scan(~r/\b[A-Z][A-Za-z0-9&']+(?:\s+[A-Z][A-Za-z0-9&']+){0,3}/, body)
      |> Enum.map(&List.first/1)
      |> Enum.reject(
        &String.match?(&1, ~r/\b(Advertisement|Related Articles|Subscribe|Share This)\b/i)
      )
      |> Enum.uniq()
      |> Enum.reduce({[], evidence}, fn name, {entities, evidence} ->
        evidence_ref = EvidenceBuilder.ref(evidence, input, name)
        evidence = EvidenceBuilder.put(evidence, evidence_ref)

        entity = %{
          "entity_id" => "entity:#{length(entities)}",
          "name" => name,
          "canonical_key" => slug(name),
          "kind" => entity_kind(name),
          "aliases" => [],
          "role_in_source" => entity_role(name, claims),
          "evidence_refs" => [evidence_ref["evidence_id"]],
          "confidence" => 0.88
        }

        {entities ++ [entity], evidence}
      end)

    {entities, evidence}
  end

  defp extract_times(input, sentences, claims, evidence) do
    time_phrases =
      sentences
      |> Enum.flat_map(fn sentence ->
        Regex.scan(
          ~r/\b(today|tomorrow|yesterday|last week|since [A-Z]?[a-z]+|by tomorrow|morning|afternoon|evening|tonight|friday|monday)\b/i,
          sentence
        )
        |> Enum.map(fn [phrase | _] -> {phrase, sentence} end)
      end)
      |> Enum.uniq()

    time_phrases
    |> Enum.reduce({[], evidence}, fn {phrase, sentence}, {times, evidence} ->
      evidence_ref = EvidenceBuilder.ref(evidence, input, phrase)
      evidence = EvidenceBuilder.put(evidence, evidence_ref)
      scope = time_scope(input, phrase, sentence)

      time =
        scope
        |> Map.merge(%{
          "time_id" => "time:#{length(times)}",
          "text" => phrase,
          "target_claim_ids" => target_claims(sentence, claims),
          "evidence_refs" => [evidence_ref["evidence_id"]]
        })

      {times ++ [time], evidence}
    end)
  end

  defp extract_stance(input, claims, framing, evidence) do
    text = canonical_text(input)
    sentence = List.first(split_sentences(text)) || text
    evidence_ref = EvidenceBuilder.ref(evidence, input, sentence)
    evidence = EvidenceBuilder.put(evidence, evidence_ref)
    stance_kind = source_stance_kind(input, text)

    stance = %{
      "stance_id" => "stance:0",
      "stance_kind" => stance_kind,
      "target_claim_ids" => Enum.map(claims, & &1["claim_id"]),
      "asserted_by" => "publisher",
      "certainty" => if(framing == [], do: "medium", else: "high"),
      "evidence_refs" => [evidence_ref["evidence_id"]]
    }

    {[stance], evidence}
  end

  defp quality_for(%{claims: [], framing: [], questions: [], stale_check?: true}) do
    %{
      "status" => "usable",
      "confidence" => 0.72,
      "reasons" => [],
      "warnings" => ["background_check_without_structural_claim"]
    }
  end

  defp quality_for(%{claims: [], framing: [], questions: []}) do
    %{
      "status" => "low_confidence",
      "confidence" => 0.34,
      "reasons" => ["unsafe_to_extract_structural_facts"],
      "warnings" => []
    }
  end

  defp quality_for(%{claims: claims}) do
    warnings =
      if Enum.any?(claims, &(get_in(&1, ["time_scope", "kind"]) == "relative")),
        do: ["relative_time_normalized_from_source_timestamp"],
        else: []

    %{
      "status" => "usable",
      "confidence" => 0.88,
      "reasons" => [],
      "warnings" => warnings
    }
  end

  defp actor_entity(input, source_actor, evidence) do
    ref =
      case source_actor["name"] do
        nil -> nil
        name -> EvidenceBuilder.find_existing(evidence, name)
      end

    if ref do
      %{
        "entity_id" => "entity:source_actor",
        "name" => source_actor["name"],
        "canonical_key" => source_actor["stable_id"] || slug(source_actor["name"]),
        "kind" => contract_entity_kind(source_actor["kind"]),
        "aliases" => [],
        "role_in_source" =>
          if(input.source_type in ["email", "message", "user_note"],
            do: "source",
            else: "publisher"
          ),
        "evidence_refs" => [ref["evidence_id"]],
        "confidence" => 0.86
      }
    end
  end

  defp input_context(input) do
    provenance = input.normalized || %{}

    %{
      "source_type" => input.source_type,
      "source_name" => provenance["source_name"],
      "source_actor" => provenance["source_actor"] || %{},
      "observed_at" => DateTime.to_iso8601(input.observed_at),
      "retrieved_at" => provenance["retrieved_at"],
      "occurred_at" => provenance["occurred_at"],
      "canonical_uri" => provenance["canonical_uri"],
      "acl" => input.acl
    }
  end

  defp run(input, opts, started_at) do
    %{
      "extractor_version" => @extractor_version,
      "contract_version" => Contract.contract_version(),
      "prompt_version" => opts[:prompt_version],
      "model" => opts[:model],
      "deterministic" => true,
      "trace_id" => get_in(input.normalized, ["trace_id"]),
      "started_at" => started_at,
      "completed_at" => now_iso()
    }
  end

  defp canonical_text(input), do: input.body_text || ""

  defp content_segments(text) do
    text
    |> String.split(~r/\n+/)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == "" or boilerplate?(&1)))
    |> Enum.map(&%{text: &1})
  end

  defp split_sentences(text) do
    text
    |> String.replace(~r/\s+/, " ")
    |> String.split(~r/(?<=[.!?])\s+/)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp boilerplate?(text), do: String.match?(text, @boilerplate)

  defp stale_check?(text),
    do: String.match?(String.downcase(text), ~r/\b(no new activity|no change|background check)\b/)

  defp bad_predicate?(claim) do
    predicate = claim.predicate |> to_string() |> String.downcase()

    value =
      claim.value
      |> to_string()
      |> String.replace(~r/^[.:;,!?"']+|[.:;,!?"']+$/, "")
      |> String.downcase()

    predicate in @copulas or
      predicate in [
        "this",
        "that",
        "there",
        "source",
        "article",
        "communications",
        "officials",
        "leaders"
      ] or
      value in @copulas or
      value in [
        "a",
        "an",
        "the",
        "for",
        "during",
        "in",
        "by",
        "since",
        "this",
        "moved",
        "confusing",
        "spinning",
        "trying",
        "disruption",
        "story"
      ] or
      String.length(predicate) > 32
  end

  defp fact_key("service"), do: "service_state"

  defp fact_key(predicate), do: predicate |> slug() |> String.replace("-", "_")

  defp fact_value(value) do
    value =
      value
      |> to_string()
      |> String.replace(~r/^[.:;,!?"']+|[.:;,!?"']+$/, "")
      |> String.downcase()
      |> String.replace(~r/[$,]/, "")

    case value do
      "opened" -> "open"
      "normally" -> "normal"
      other -> other
    end
  end

  defp claim_kind(key, stance) do
    cond do
      stance.modality == "denied" -> "denial"
      stance.modality == "reported" and stance.anonymous -> "allegation"
      key in ["deadline", "due", "eta"] -> "deadline"
      key in ["venue", "location", "route"] -> "location"
      key in ["amount", "quoted_amount", "price", "cost"] -> "numeric"
      key in ["decision", "approved", "rejected"] -> "decision"
      true -> "state"
    end
  end

  defp contract_entity_kind(kind) when kind in @entity_kinds, do: kind
  defp contract_entity_kind(_kind), do: "unknown"

  defp uncertainty(stance, time_scope) do
    reasons =
      []
      |> maybe_add(stance.modality in ["possible", "alleged"], "hedged_language")
      |> maybe_add(stance.anonymous, "ambiguous_actor")
      |> maybe_add(time_scope["kind"] == "relative", "temporal_ambiguous")
      |> maybe_add(stance.asserted_by == "quoted_actor", "quote_only")

    %{
      "level" => if(reasons == [], do: "low", else: "medium"),
      "reasons" => reasons
    }
  end

  defp maybe_add(list, true, item), do: [item | list]
  defp maybe_add(list, false, _item), do: list

  defp time_scope(input, nil, sentence) do
    case time_phrase(sentence) do
      nil -> current_scope()
      phrase -> time_scope(input, phrase, sentence)
    end
  end

  defp time_scope(input, phrase, _sentence) do
    observed = input.observed_at
    phrase = phrase && String.downcase(phrase)

    case phrase do
      nil ->
        current_scope()

      "today" ->
        relative_scope(observed, phrase, 0)

      "tomorrow" ->
        relative_scope(observed, phrase, 1)

      "yesterday" ->
        relative_scope(observed, phrase, -1)

      "by tomorrow" ->
        relative_scope(observed, phrase, 1)

      "last week" ->
        relative_scope(observed, phrase, -7)

      "tonight" ->
        current_text_scope(phrase)

      other ->
        current_text_scope(other)
    end
  end

  defp relative_scope(observed, phrase, days) do
    date = observed |> DateTime.to_date() |> Date.add(days)
    start = DateTime.new!(date, ~T[00:00:00], "Etc/UTC") |> DateTime.to_iso8601()
    finish = DateTime.new!(date, ~T[23:59:59], "Etc/UTC") |> DateTime.to_iso8601()

    %{
      "kind" => "relative",
      "normalized_start" => start,
      "normalized_end" => finish,
      "relative_text" => phrase,
      "basis" => "observed_at",
      "confidence" => 0.82,
      "legacy_scope" => phrase
    }
  end

  defp current_text_scope(scope) do
    %{
      "kind" => "current",
      "normalized_start" => nil,
      "normalized_end" => nil,
      "relative_text" => scope,
      "basis" => "source_text",
      "confidence" => 0.84,
      "legacy_scope" => scope
    }
  end

  defp current_scope do
    %{
      "kind" => "current",
      "normalized_start" => nil,
      "normalized_end" => nil,
      "relative_text" => nil,
      "basis" => "source_text",
      "confidence" => 0.82,
      "legacy_scope" => "current"
    }
  end

  defp time_phrase(sentence) do
    down = String.downcase(sentence)

    Enum.find(
      [
        "by tomorrow",
        "last week",
        "today",
        "tomorrow",
        "yesterday",
        "morning",
        "afternoon",
        "evening",
        "tonight",
        "friday",
        "monday"
      ],
      &String.contains?(down, &1)
    )
  end

  defp source_stance_kind(%{source_type: "opinion_column"}, _text), do: "opinion"
  defp source_stance_kind(%{source_type: "user_note"}, _text), do: "user_note"

  defp source_stance_kind(%{source_type: source_type}, _text)
       when source_type in ["email", "message"], do: "firsthand_message"

  defp source_stance_kind(_input, text) do
    down = String.downcase(text)

    cond do
      String.match?(down, ~r/\b(correction|corrected|revised)\b/) -> "correction"
      String.match?(down, ~r/\b(denied|denies|did not)\b/) -> "denial"
      String.match?(down, ~r/\b(rumor|rumour|may|might)\b/) -> "rumor"
      String.match?(down, ~r/\b(analysis|opinion|column)\b/) -> "analysis"
      true -> "reporting"
    end
  end

  defp framing_kind(sentence) do
    down = String.downcase(sentence)

    cond do
      String.match?(down, ~r/\b(blamed|blame|fault)\b/) -> "blame"
      String.match?(down, ~r/\b(credited|praised)\b/) -> "credit"
      String.match?(down, ~r/\b(minimized|minor|nothing to see)\b/) -> "minimization"
      String.match?(down, ~r/\b(escalating|crisis|chaos|disaster)\b/) -> "escalation"
      String.match?(down, ~r/\b(reassured|calm|under control)\b/) -> "reassurance"
      String.match?(down, ~r/\b(criticized|avoidable|failed|failure)\b/) -> "criticism"
      String.match?(down, ~r/\b(reframe|spin|campaign)\b/) -> "strategic_spin"
      String.match?(down, ~r/\b(waiting in the rain|human|families|workers)\b/) -> "human_color"
      true -> nil
    end
  end

  defp opinion_source?(input, sentence) do
    input.source_type == "opinion_column" or
      String.match?(String.downcase(sentence), ~r/\b(opinion|analysis|column)\b/)
  end

  defp quoted_or_unknown(sentence) do
    if String.match?(String.downcase(sentence), ~r/\b(said|according to|told|quoted)\b/),
      do: "quoted_actor",
      else: "unknown"
  end

  defp actor_assertion(input) do
    if input.source_type in ["email", "message", "user_note"],
      do: "source_actor",
      else: "publisher"
  end

  defp entity_kind(name) do
    down = String.downcase(name)

    cond do
      String.match?(down, ~r/\b(agency|department|clinic|board|company|corp|office)\b/) ->
        "org"

      String.match?(down, ~r/\b(park|hall|center|centre|street|north|south|east|west)\b/) ->
        "place"

      true ->
        "unknown"
    end
  end

  defp entity_role(name, claims) do
    key = slug(name)

    cond do
      Enum.any?(claims, &(&1["normalized_subject"] == key)) ->
        "subject"

      Enum.any?(claims, &(&1["fact_key"] in ["venue", "location"] and &1["fact_value"] == key)) ->
        "venue"

      true ->
        "unknown"
    end
  end

  defp target_claims(sentence, claims) do
    down = String.downcase(sentence)

    claims
    |> Enum.filter(
      &(String.contains?(down, String.downcase(&1["predicate"] || "")) or
          String.contains?(down, &1["fact_value"] || ""))
    )
    |> Enum.map(& &1["claim_id"])
  end

  defp matched_text(sentence, regex) do
    case Regex.run(regex, sentence) do
      [match | _] -> match
      nil -> nil
    end
  end

  defp normalize_spaces(value), do: String.replace(value, ~r/\s+/, " ")

  defp normalize_predicate_phrases(value) do
    value
    |> String.replace(~r/\bquoted amount\b/i, "quoted_amount")
    |> String.replace(~r/\bterminal staffing\b/i, "terminal_staffing")
  end

  defp date_value(value) do
    [month, day, year] =
      value
      |> String.downcase()
      |> String.replace(~r/[,.]/, "")
      |> String.split(~r/\s+/, trim: true)

    [month_value(month), day, year]
    |> Enum.join("-")
  end

  defp month_value(value) do
    value
    |> String.downcase()
    |> case do
      "january" -> "jan"
      "february" -> "feb"
      "march" -> "mar"
      "april" -> "apr"
      "june" -> "jun"
      "july" -> "jul"
      "august" -> "aug"
      "september" -> "sep"
      "sept" -> "sep"
      "october" -> "oct"
      "november" -> "nov"
      "december" -> "dec"
      month -> month
    end
  end

  defp slug(value) do
    value
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
  end

  defp empty_to_nil(""), do: nil
  defp empty_to_nil(value), do: value

  defp uniq_by(values, key),
    do: values |> Enum.reverse() |> Enum.uniq_by(& &1[key]) |> Enum.reverse()

  defp now_iso, do: DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

  defmodule EvidenceBuilder do
    @moduledoc false

    alias Primeradiant.StorageHarness.ChangesetStore

    def new(input), do: %{input: input, refs: %{}}

    def put(builder, evidence),
      do: %{builder | refs: Map.put(builder.refs, evidence["evidence_id"], evidence)}

    def index(builder), do: builder.refs |> Map.values() |> Enum.sort_by(& &1["evidence_id"])

    def find_existing(builder, text) do
      Enum.find(Map.values(builder.refs), &(&1["quote"] == text))
    end

    def ref(builder, input, fragment) do
      {source, text, start, quote} = locate(input, fragment)
      evidence_id = "ev:#{source}:#{start}:#{ChangesetStore.hash(quote) |> String.slice(0, 8)}"

      Map.get(builder.refs, evidence_id) ||
        %{
          "evidence_id" => evidence_id,
          "source" => source,
          "span_start" => start,
          "span_end" => start + byte_size(quote),
          "text_hash" => ChangesetStore.hash(text),
          "quote" => quote,
          "acl" => input.acl
        }
    end

    defp locate(input, fragment) do
      body = input.body_text || ""
      title = input.title || ""
      trimmed = fragment |> to_string() |> String.trim()

      cond do
        byte_match(body, trimmed) ->
          {start, length} = byte_match(body, trimmed)
          {"body_text", body, start, binary_part(body, start, length)}

        byte_match(title, trimmed) ->
          {start, length} = byte_match(title, trimmed)
          {"title", title, start, binary_part(title, start, length)}

        true ->
          quote = String.slice(body, 0, min(byte_size(body), 80))
          {"body_text", body, 0, quote}
      end
    end

    defp byte_match(text, fragment) do
      case :binary.match(text, fragment) do
        {start, length} -> {start, length}
        :nomatch -> nil
      end
    end
  end
end
