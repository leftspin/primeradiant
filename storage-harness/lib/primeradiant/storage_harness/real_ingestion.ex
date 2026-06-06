defmodule Primeradiant.StorageHarness.RealIngestion do
  @moduledoc false

  alias Primeradiant.Ingestion.Admission
  alias Primeradiant.StorageHarness.State

  def ingest_items(items, actor_id \\ "flynn") when is_list(items) do
    tenant_id = tenant_id_for_items!(items)

    {state, admissions} =
      items
      |> Enum.sort_by(
        &{to_string(&1[:observed_at] || &1["observed_at"]),
         to_string(&1[:retrieved_at] || &1["retrieved_at"]),
         to_string(&1[:external_id] || &1["external_id"])}
      )
      |> Enum.reduce({State.new(user_id: actor_id, tenant_id: tenant_id), []}, fn item,
                                                                                  {state,
                                                                                   admissions} ->
        {state, admission} = ingest_item(state, item, actor_id)
        {state, admissions ++ [admission]}
      end)

    {:ok, state,
     %{
       source_behavior: :evidence_admission_only,
       admissions: admissions,
       inputs: length(state.inputs),
       ingest_event_activations: count_audit(state, :ingest_event_activation),
       substrate_proof_only: true,
       meaning_proof: :requires_packet_grounded_agent_runs,
       stories: length(state.stories),
       proposals: length(state.proposals),
       graph_commits: length(state.graph_commits)
     }}
  end

  def ingest_item(%State{} = state, item, _actor_id \\ "flynn") do
    {state, input, input_ref} = Admission.admit_source_item(state, item)
    input = persist_admission_fields(input, input_ref)

    admission = admission_record(input, input_ref)

    state =
      state
      |> State.replace(:inputs, input.id, input)
      |> State.audit(admission)
      |> State.audit(activation_record(input, input_ref))

    {state, admission}
  end

  defp persist_admission_fields(input, input_ref) do
    spans = content_span_refs(input, input_ref)
    refs = evidence_refs(spans)

    %{
      input
      | normalized:
          Map.merge(input.normalized || %{}, %{
            "adapter_version" => get_in(input.normalized || %{}, ["source_mode"]),
            "admission_status" => "admitted",
            "source_ref" => input_ref,
            "content_span_refs" => stringify(spans),
            "evidence_refs" => refs,
            "story_identity" => nil,
            "story_classification" => nil,
            "materiality_decision" => nil,
            "relevance_decision" => nil,
            "narrative_dedupe" => nil,
            "meaning_proof" => "not_ingest_owned"
          })
    }
  end

  defp tenant_id_for_items!(items) do
    tenant_ids =
      items
      |> Enum.map(&(&1[:tenant_id] || &1["tenant_id"]))
      |> Enum.uniq()

    case tenant_ids do
      [tenant_id] when is_binary(tenant_id) -> tenant_id
      [nil] -> raise ArgumentError, "real source item requires tenant_id"
      _ -> raise ArgumentError, "real ingestion batch must use one tenant_id"
    end
  end

  defp admission_record(input, input_ref) do
    normalized = input.normalized || %{}

    %{
      event: :source_admitted,
      admission_status: "admitted",
      source_ref: input_ref,
      source_type: input.source_type,
      external_id: input.external_id,
      adapter_version: normalized["source_mode"],
      source_provenance: %{
        ingestion_run_key: normalized["ingestion_run_key"],
        source_name: normalized["source_name"],
        source_actor: normalized["source_actor"] || %{},
        canonical_uri: normalized["canonical_uri"],
        raw_object_uri: normalized["raw_object_uri"]
      },
      visibility: input.acl,
      observed_at: input.observed_at,
      content_sha256: input.content_sha256,
      content_span_refs: content_span_refs(input, input_ref),
      evidence_refs: evidence_refs(content_span_refs(input, input_ref)),
      normalized_evidence: normalized_evidence(input),
      story_identity: nil,
      story_classification: nil,
      materiality_decision: nil,
      relevance_decision: nil,
      narrative_dedupe: nil,
      meaning_proof: :not_ingest_owned
    }
  end

  defp activation_record(input, input_ref) do
    %{
      event: :ingest_event_activation,
      activation_kind: :ingest_event,
      source_ref: input_ref,
      trigger_id: "source-admission:#{input.source_type}",
      scheduler_substrate: true,
      story_meaning_proof: false,
      packet_seed_refs: evidence_refs(content_span_refs(input, input_ref))
    }
  end

  defp content_span_refs(input, input_ref) do
    case input.body_text do
      text when is_binary(text) and text != "" ->
        [
          %{
            source_ref: input_ref,
            source: "body_text",
            span_start: 0,
            span_end: byte_size(text),
            text_hash: input.content_sha256
          }
        ]

      _ ->
        []
    end
  end

  defp evidence_refs(spans) do
    Enum.map(spans, fn span ->
      "evidence:#{span.source_ref}:#{span.source}:#{span.span_start}:#{span.span_end}"
    end)
  end

  defp stringify(value) when is_list(value), do: Enum.map(value, &stringify/1)

  defp stringify(value) when is_map(value),
    do: Map.new(value, fn {key, value} -> {to_string(key), value} end)

  defp normalized_evidence(input) do
    %{
      title: input.title,
      source_type: input.source_type,
      external_id: input.external_id,
      object_uri: input.object_uri,
      content_hash: input.content_sha256
    }
  end

  defp count_audit(state, event) do
    Enum.count(state.audit_events, &(&1.event == event))
  end
end
