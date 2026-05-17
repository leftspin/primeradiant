defmodule Primeradiant.Ingestion.Admission do
  @moduledoc false

  alias Primeradiant.StorageHarness.{ChangesetStore, Input, State}

  @source_mode "manual_real_ingest_v1"
  @source_types ~w(news_article opinion_column email message document user_note web_page)
  @forbidden_keys ~w(fixture_id expected_role story_hint expected_story expected_classification edge_hints)

  def admit_source_item(%State{} = state, attrs) when is_map(attrs) do
    item = normalize_keys(attrs)
    validate_item!(item)

    if item["tenant_id"] != state.tenant_id,
      do: raise(ArgumentError, "real source item tenant_id must match ingestion state")

    content_sha = item["content_sha256"] || content_hash(item)

    existing =
      Enum.find(state.inputs, fn input ->
        input.tenant_id == item["tenant_id"] and
          ((input.source_type == item["source_type"] and input.external_id == item["external_id"]) or
             input.content_sha256 == content_sha)
      end)

    if existing do
      {state, existing, input_ref(existing)}
    else
      body = readable_text(item)

      input =
        ChangesetStore.insert!(Input, %{
          tenant_id: item["tenant_id"],
          fixture_id: nil,
          source_type: item["source_type"],
          external_id: item["external_id"],
          observed_at: ChangesetStore.iso!(item["observed_at"]),
          title: item["title"],
          body_text: body,
          object_uri: item["raw_object_uri"] || item["canonical_uri"],
          content_sha256: content_sha,
          acl: item["acl"],
          normalized: %{
            "source_mode" => @source_mode,
            "ingestion_run_key" => item["ingestion_run_key"],
            "retrieved_at" => item["retrieved_at"],
            "occurred_at" => item["occurred_at"],
            "canonical_uri" => item["canonical_uri"],
            "raw_object_uri" => item["raw_object_uri"],
            "source_name" => item["source_name"],
            "source_actor" => item["source_actor"] || %{},
            "metadata" => item["metadata"] || %{},
            "content_sha256" => content_sha
          },
          facts: %{},
          background: %{},
          questions: %{},
          colors: [],
          topic_tokens: []
        })

      state =
        state
        |> State.append(:inputs, input)
        |> State.put_source_id(:input, input_ref(input), input.id)
        |> State.put_source_id(:input, input.external_id, input.id)

      {state, input, input_ref(input)}
    end
  end

  def input_ref(%Input{} = input), do: "#{input.source_type}:#{input.external_id}"

  def validate_item!(item) do
    reject_forbidden_keys!(item)

    unless item["source_mode"] == @source_mode,
      do: raise(ArgumentError, "source_mode must be #{@source_mode}")

    required = ~w(tenant_id source_mode source_type external_id observed_at retrieved_at acl)

    Enum.each(required, fn key ->
      if blank?(item[key]), do: raise(ArgumentError, "real source item requires #{key}")
    end)

    unless item["source_type"] in @source_types,
      do: raise(ArgumentError, "unsupported source_type")

    if blank?(readable_text(item)) and blank?(item["raw_object_uri"]) do
      raise ArgumentError, "real source item requires readable text or raw object reference"
    end

    acl = item["acl"] || %{}

    unless acl["privacy"] in ["public", "private"],
      do: raise(ArgumentError, "acl privacy must be public or private")

    if acl["privacy"] == "private" and (acl["participants"] || []) == [] do
      raise ArgumentError, "private source items require participants"
    end

    :ok
  end

  def content_hash(item), do: ChangesetStore.hash(canonical_content(item))

  def normalize_keys(value) when is_map(value) do
    Map.new(value, fn {key, value} -> {to_string(key), normalize_keys(value)} end)
  end

  def normalize_keys(value) when is_list(value), do: Enum.map(value, &normalize_keys/1)
  def normalize_keys(value), do: value

  defp reject_forbidden_keys!(value) when is_map(value) do
    Enum.each(value, fn {key, nested} ->
      if key in @forbidden_keys,
        do: raise(ArgumentError, "real admission rejects trusted label #{key}")

      reject_forbidden_keys!(nested)
    end)
  end

  defp reject_forbidden_keys!(value) when is_list(value),
    do: Enum.each(value, &reject_forbidden_keys!/1)

  defp reject_forbidden_keys!(_value), do: :ok

  defp canonical_content(item) do
    [
      item["source_type"],
      item["external_id"],
      item["canonical_uri"],
      readable_text(item),
      inspect(item["metadata"] || %{})
    ]
    |> Enum.reject(&blank?/1)
    |> Enum.join("\n")
  end

  defp readable_text(item), do: item["body_text"] || item["extracted_text"] || ""

  defp blank?(value), do: value in [nil, ""]
end
