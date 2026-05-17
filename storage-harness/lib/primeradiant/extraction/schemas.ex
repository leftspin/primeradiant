defmodule Primeradiant.Extraction.Result do
  @moduledoc false

  use Ecto.Schema

  @primary_key false
  embedded_schema do
    field(:input_id, :binary_id)
    field(:tenant_id, :binary_id)
    field(:extractor_run, :map)
    field(:input_context, :map)
    field(:quality, :map)
    field(:claims, {:array, :map}, default: [])
    field(:entities, {:array, :map}, default: [])
    field(:times, {:array, :map}, default: [])
    field(:source_stance, {:array, :map}, default: [])
    field(:framing, {:array, :map}, default: [])
    field(:questions, {:array, :map}, default: [])
    field(:evidence_index, {:array, :map}, default: [])
  end
end

defmodule Primeradiant.Extraction.Contract do
  @moduledoc false

  import Ecto.Changeset

  alias Primeradiant.Extraction.Result
  alias Primeradiant.StorageHarness.{ChangesetStore, Input}

  @contract_version "production_extractor_v1"
  @quality_statuses ~w(usable partial low_confidence refused)
  @refusal_reasons ~w(no_readable_text text_too_short boilerplate_only unsupported_language malformed_input ambiguous_scope unsafe_to_extract_structural_facts extractor_error)
  @claim_kinds ~w(event state numeric deadline decision availability location ownership responsibility allegation denial quote other)
  @polarities ~w(positive negative mixed unknown)
  @modalities ~w(asserted reported alleged planned possible denied corrected opinion unknown)
  @asserted_by ~w(publisher quoted_actor source_actor user unknown)
  @time_kinds ~w(instant range relative current unknown)
  @time_bases ~w(occurred_at observed_at retrieved_at source_text unknown)
  @uncertainty_levels ~w(high medium low)
  @uncertainty_reasons ~w(hedged_language ambiguous_actor thin_context quote_only conflicting_within_source temporal_ambiguous extractor_limit other)
  @downstream_uses ~w(identity_candidate conflict_candidate background framing_only ignore)
  @entity_kinds ~w(person org place product project account agency event document unknown)
  @entity_roles ~w(actor affected_party source quoted_source publisher venue subject recipient unknown)
  @stance_kinds ~w(reporting opinion analysis correction denial firsthand_message user_note rumor unknown)
  @framing_kinds ~w(blame credit minimization escalation reassurance criticism strategic_spin human_color other)
  @evidence_sources ~w(title body_text extracted_text metadata artifact_text)

  def contract_version, do: @contract_version

  def validate(%Input{} = input, result) when is_map(result) do
    changeset =
      %Result{}
      |> cast(result, [
        :input_id,
        :tenant_id,
        :extractor_run,
        :input_context,
        :quality,
        :claims,
        :entities,
        :times,
        :source_stance,
        :framing,
        :questions,
        :evidence_index
      ])
      |> validate_required([
        :input_id,
        :tenant_id,
        :extractor_run,
        :input_context,
        :quality,
        :claims,
        :entities,
        :times,
        :source_stance,
        :framing,
        :questions,
        :evidence_index
      ])
      |> validate_top_level(input)

    if changeset.valid? do
      {:ok, apply_changes(changeset)}
    else
      {:error, changeset}
    end
  end

  defp validate_top_level(changeset, input) do
    result = apply_changes(changeset)

    changeset
    |> validate_input_identity(input, result)
    |> validate_extractor_run(result.extractor_run)
    |> validate_input_context(input, result.input_context)
    |> validate_quality(result.quality, result)
    |> validate_evidence_index(input, result.evidence_index)
    |> validate_claims(result.claims, result.evidence_index)
    |> validate_entities(result.entities, result.evidence_index)
    |> validate_times(result.times, result.evidence_index)
    |> validate_source_stance(result.source_stance, result.evidence_index)
    |> validate_framing(result.framing, result.evidence_index)
    |> validate_questions(result.questions, result.evidence_index)
  end

  defp validate_input_identity(changeset, input, result) do
    changeset
    |> require_equal(:input_id, result.input_id, input.id)
    |> require_equal(:tenant_id, result.tenant_id, input.tenant_id)
  end

  defp validate_extractor_run(changeset, run) when is_map(run) do
    changeset
    |> require_in(run, "contract_version", [@contract_version])
    |> require_string(run, "extractor_version")
    |> require_boolean(run, "deterministic")
  end

  defp validate_extractor_run(changeset, _run),
    do: add_error(changeset, :extractor_run, "must be a map")

  defp validate_input_context(changeset, input, context) when is_map(context) do
    changeset
    |> require_equal(:input_context, context["source_type"], input.source_type)
    |> require_equal(
      :input_context,
      context["observed_at"],
      DateTime.to_iso8601(input.observed_at)
    )
    |> require_equal(:input_context, context["acl"], input.acl)
  end

  defp validate_input_context(changeset, _input, _context),
    do: add_error(changeset, :input_context, "must be a map")

  defp validate_quality(changeset, quality, result) when is_map(quality) do
    changeset =
      changeset
      |> require_in(quality, "status", @quality_statuses)
      |> require_confidence(:quality, quality["confidence"])
      |> require_list(:quality, quality["reasons"])
      |> require_list(:quality, quality["warnings"])

    cond do
      quality["status"] in ["low_confidence", "refused"] and
          material_items(result) != [] ->
        add_error(
          changeset,
          :claims,
          "low-confidence/refused results cannot include material extraction items"
        )

      quality["status"] in ["low_confidence", "refused"] and
          Enum.any?(quality["reasons"], &(&1 not in @refusal_reasons)) ->
        add_error(changeset, :quality, "contains unsupported refusal reason")

      true ->
        changeset
    end
  end

  defp validate_quality(changeset, _quality, _result),
    do: add_error(changeset, :quality, "must be a map")

  defp validate_evidence_index(changeset, input, evidence_index) when is_list(evidence_index) do
    Enum.reduce(evidence_index, changeset, fn evidence, acc ->
      acc
      |> require_string(evidence, "evidence_id")
      |> require_in(evidence, "source", @evidence_sources)
      |> require_integer(evidence, "span_start")
      |> require_integer(evidence, "span_end")
      |> require_string(evidence, "text_hash")
      |> require_string(evidence, "quote")
      |> require_equal(:evidence_index, evidence["acl"], input.acl)
      |> validate_span(input, evidence)
    end)
  end

  defp validate_evidence_index(changeset, _input, _evidence_index),
    do: add_error(changeset, :evidence_index, "must be a list")

  defp validate_claims(changeset, claims, evidence_index) when is_list(claims) do
    Enum.reduce(claims, changeset, fn claim, acc ->
      acc
      |> require_string(claim, "claim_id")
      |> require_string(claim, "text")
      |> require_string(claim, "predicate")
      |> require_in(claim, "claim_kind", @claim_kinds)
      |> require_in(claim, "polarity", @polarities)
      |> require_in(claim, "modality", @modalities)
      |> require_in(claim, "downstream_use", @downstream_uses)
      |> require_boolean(claim, "structural_candidate")
      |> validate_attribution(claim["attribution"])
      |> validate_time_scope(claim["time_scope"])
      |> validate_uncertainty(claim["uncertainty"])
      |> validate_refs(:claims, claim["evidence_refs"], evidence_index)
      |> validate_structural_claim(claim)
    end)
  end

  defp validate_claims(changeset, _claims, _evidence_index),
    do: add_error(changeset, :claims, "must be a list")

  defp validate_entities(changeset, entities, evidence_index) when is_list(entities) do
    Enum.reduce(entities, changeset, fn entity, acc ->
      acc
      |> require_string(entity, "entity_id")
      |> require_string(entity, "name")
      |> require_string(entity, "canonical_key")
      |> require_in(entity, "kind", @entity_kinds)
      |> require_in(entity, "role_in_source", @entity_roles)
      |> require_confidence(:entities, entity["confidence"])
      |> validate_refs(:entities, entity["evidence_refs"], evidence_index)
    end)
  end

  defp validate_entities(changeset, _entities, _evidence_index),
    do: add_error(changeset, :entities, "must be a list")

  defp validate_times(changeset, times, evidence_index) when is_list(times) do
    Enum.reduce(times, changeset, fn time, acc ->
      acc
      |> require_string(time, "time_id")
      |> validate_time_scope(time)
      |> validate_refs(:times, time["evidence_refs"], evidence_index)
    end)
  end

  defp validate_times(changeset, _times, _evidence_index),
    do: add_error(changeset, :times, "must be a list")

  defp validate_source_stance(changeset, stances, evidence_index) when is_list(stances) do
    Enum.reduce(stances, changeset, fn stance, acc ->
      acc
      |> require_string(stance, "stance_id")
      |> require_in(stance, "stance_kind", @stance_kinds)
      |> require_list(:source_stance, stance["target_claim_ids"])
      |> require_in(stance, "certainty", ~w(high medium low))
      |> validate_refs(:source_stance, stance["evidence_refs"], evidence_index)
    end)
  end

  defp validate_source_stance(changeset, _stances, _evidence_index),
    do: add_error(changeset, :source_stance, "must be a list")

  defp validate_framing(changeset, framing, evidence_index) when is_list(framing) do
    Enum.reduce(framing, changeset, fn item, acc ->
      acc
      |> require_string(item, "framing_id")
      |> require_string(item, "text")
      |> require_in(item, "framing_kind", @framing_kinds)
      |> require_list(:framing, item["target_claim_ids"])
      |> require_equal(:framing, item["promoted_to_structural_fact"], false)
      |> require_confidence(:framing, item["confidence"])
      |> validate_refs(:framing, item["evidence_refs"], evidence_index)
    end)
  end

  defp validate_framing(changeset, _framing, _evidence_index),
    do: add_error(changeset, :framing, "must be a list")

  defp validate_questions(changeset, questions, evidence_index) when is_list(questions) do
    Enum.reduce(questions, changeset, fn question, acc ->
      acc
      |> require_string(question, "question_id")
      |> require_string(question, "text")
      |> validate_refs(:questions, question["evidence_refs"], evidence_index)
    end)
  end

  defp validate_questions(changeset, _questions, _evidence_index),
    do: add_error(changeset, :questions, "must be a list")

  defp validate_attribution(changeset, attribution) when is_map(attribution) do
    changeset
    |> require_in(attribution, "asserted_by", @asserted_by)
    |> require_boolean(attribution, "anonymous")
  end

  defp validate_attribution(changeset, _attribution),
    do: add_error(changeset, :claims, "claim attribution must be a map")

  defp validate_time_scope(changeset, time_scope) when is_map(time_scope) do
    changeset
    |> require_in(time_scope, "kind", @time_kinds)
    |> require_in(time_scope, "basis", @time_bases)
    |> require_confidence(:claims, time_scope["confidence"])
  end

  defp validate_time_scope(changeset, _time_scope),
    do: add_error(changeset, :claims, "time scope must be a map")

  defp validate_uncertainty(changeset, uncertainty) when is_map(uncertainty) do
    changeset =
      changeset
      |> require_in(uncertainty, "level", @uncertainty_levels)
      |> require_list(:claims, uncertainty["reasons"])

    if Enum.any?(uncertainty["reasons"] || [], &(&1 not in @uncertainty_reasons)) do
      add_error(changeset, :claims, "contains unsupported uncertainty reason")
    else
      changeset
    end
  end

  defp validate_uncertainty(changeset, _uncertainty),
    do: add_error(changeset, :claims, "uncertainty must be a map")

  defp validate_structural_claim(changeset, claim) do
    cond do
      claim["structural_candidate"] == true and blank?(claim["fact_key"]) ->
        add_error(changeset, :claims, "structural claim requires fact_key")

      claim["structural_candidate"] == true and blank?(claim["fact_value"]) ->
        add_error(changeset, :claims, "structural claim requires fact_value")

      claim["downstream_use"] == "framing_only" and claim["structural_candidate"] == true ->
        add_error(changeset, :claims, "framing-only claim cannot be structural")

      true ->
        changeset
    end
  end

  defp validate_refs(changeset, field, refs, evidence_index) when is_list(refs) and refs != [] do
    known_refs = evidence_index |> Enum.map(& &1["evidence_id"]) |> MapSet.new()

    if Enum.all?(refs, &MapSet.member?(known_refs, &1)) do
      changeset
    else
      add_error(changeset, field, "contains unknown evidence ref")
    end
  end

  defp validate_refs(changeset, field, _refs, _evidence_index),
    do: add_error(changeset, field, "requires evidence refs")

  defp validate_span(changeset, input, evidence) do
    source = evidence["source"]
    text = source_text(input, source)
    start = evidence["span_start"]
    finish = evidence["span_end"]
    quote = evidence["quote"]

    cond do
      not is_integer(start) or not is_integer(finish) or start < 0 or finish <= start ->
        add_error(changeset, :evidence_index, "invalid span")

      finish > byte_size(text) ->
        add_error(changeset, :evidence_index, "span outside source text")

      binary_part(text, start, finish - start) != quote ->
        add_error(changeset, :evidence_index, "span quote mismatch")

      evidence["text_hash"] != ChangesetStore.hash(text) ->
        add_error(changeset, :evidence_index, "source text hash mismatch")

      true ->
        changeset
    end
  end

  defp source_text(input, "title"), do: input.title || ""
  defp source_text(input, "body_text"), do: input.body_text || ""

  defp source_text(input, "extracted_text"),
    do: get_in(input.normalized, ["extracted_text"]) || ""

  defp source_text(input, "metadata"),
    do: Jason.encode!(get_in(input.normalized, ["metadata"]) || %{})

  defp source_text(input, "artifact_text"), do: get_in(input.normalized, ["artifact_text"]) || ""

  defp material_items(result) do
    (result.claims || []) ++
      (result.entities || []) ++
      (result.times || []) ++
      (result.source_stance || []) ++
      (result.framing || []) ++
      (result.questions || []) ++
      (result.evidence_index || [])
  end

  defp require_string(changeset, map, key) do
    if is_binary(map[key]) and map[key] != "",
      do: changeset,
      else: add_error(changeset, :base, "#{key} must be a non-empty string")
  end

  defp require_integer(changeset, map, key) do
    if is_integer(map[key]),
      do: changeset,
      else: add_error(changeset, :base, "#{key} must be an integer")
  end

  defp require_boolean(changeset, map, key) do
    if is_boolean(map[key]),
      do: changeset,
      else: add_error(changeset, :base, "#{key} must be a boolean")
  end

  defp require_list(changeset, field, value) do
    if is_list(value),
      do: changeset,
      else: add_error(changeset, field, "must be a list")
  end

  defp require_confidence(changeset, field, value) when is_number(value) do
    if value >= 0 and value <= 1,
      do: changeset,
      else: add_error(changeset, field, "confidence must be between 0 and 1")
  end

  defp require_confidence(changeset, field, _value),
    do: add_error(changeset, field, "confidence must be numeric")

  defp require_equal(changeset, field, actual, expected) do
    if actual == expected,
      do: changeset,
      else: add_error(changeset, field, "mismatch")
  end

  defp require_in(changeset, map, key, values) do
    if map[key] in values,
      do: changeset,
      else: add_error(changeset, :base, "#{key} unsupported")
  end

  defp blank?(value), do: value in [nil, ""]
end
