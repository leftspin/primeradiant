alias Primeradiant.Ingestion.Admission

alias Primeradiant.StorageHarness.{
  Input,
  LiveStoryAgentLoop,
  State,
  Story,
  StoryEvent
}

fixture_path =
  Path.expand(
    "../../storage-harness/test/fixtures/t1745-production-catalog.json",
    __DIR__
  )

fixture = fixture_path |> File.read!() |> Jason.decode!()

parse_timestamp! = fn value ->
  {:ok, timestamp, 0} = DateTime.from_iso8601(value)
  timestamp
end

message_id = "b9edbc9f-1fe8-437a-bd1f-2f8b2530bddf"

capture_input = %Input{
  id: "row-15164",
  tenant_id: fixture["tenant_id"],
  source_type: "news_article",
  external_id: message_id,
  observed_at: ~U[2026-07-10 21:23:42.501095Z],
  title:
    "Shippers Face Deepening Dilemma as U.S. and Iran Vie for Control of the Strait - The New York Times",
  body_text:
    "Shippers Face Deepening Dilemma as U.S. and Iran Vie for Control of the Strait - The New York Times",
  object_uri: "https://news.google.test/world/#{message_id}",
  content_sha256: :crypto.hash(:sha256, message_id) |> Base.encode16(case: :lower),
  acl: %{"privacy" => "public", "participants" => []},
  normalized: %{},
  facts: %{},
  background: %{},
  questions: %{},
  colors: [],
  topic_tokens: []
}

rows =
  fixture["refs"]
  |> Enum.with_index(1)
  |> Enum.map(fn {ref, index} ->
    story_id = Ecto.UUID.generate()
    linked_input_id = Ecto.UUID.generate()
    updated_at_story = parse_timestamp!.(ref["updated_at_story"])
    last_material_at = parse_timestamp!.(ref["last_material_at"])

    story = %Story{
      id: story_id,
      tenant_id: fixture["tenant_id"],
      story_key: ref["story_key"],
      title: ref["title"],
      state: ref["state"],
      version: ref["version"],
      first_observed_at: updated_at_story,
      updated_at_story: updated_at_story,
      last_material_at: last_material_at,
      structural_facts: %{"captured_production_story" => true}
    }

    linked_input = %Input{
      id: linked_input_id,
      tenant_id: fixture["tenant_id"],
      source_type: "news_article",
      external_id: "captured-story-#{index}",
      observed_at: updated_at_story,
      title: ref["title"],
      body_text: ref["title"],
      content_sha256: :crypto.hash(:sha256, ref["title"] || "") |> Base.encode16(case: :lower),
      acl: %{"privacy" => "public", "participants" => []},
      normalized: %{},
      facts: %{},
      background: %{},
      questions: %{},
      colors: [],
      topic_tokens: []
    }

    story_event = %StoryEvent{
      id: Ecto.UUID.generate(),
      tenant_id: fixture["tenant_id"],
      story_id: story_id,
      input_id: linked_input_id,
      classification: "substantive_update",
      story_version: ref["version"],
      changed_facts: %{},
      observed_at: updated_at_story,
      confidence: Decimal.new("1.0")
    }

    {story, linked_input, story_event}
  end)

stories = Enum.map(rows, &elem(&1, 0))
linked_inputs = Enum.map(rows, &elem(&1, 1))
story_events = Enum.map(rows, &elem(&1, 2))

capture_state = %{
  State.new(tenant_id: fixture["tenant_id"])
  | inputs: [capture_input | linked_inputs],
    stories: stories,
    story_events: story_events
}

capture_admission = %{
  source_ref: Admission.input_ref(capture_input),
  source_type: capture_input.source_type,
  external_id: capture_input.external_id,
  observed_at: capture_input.observed_at,
  content_sha256: capture_input.content_sha256,
  content_span_refs: ["span:row-15164"],
  evidence_refs: ["evidence:row-15164"],
  source_provenance: %{"source_name" => "Google News World"}
}

parent = self()

capture_adapter = fn config, packet, ctx ->
  result = LiveStoryAgentLoop.invoke_live_agent(config, packet, ctx)
  send(parent, {:captured_identity_invocation, config, packet, result})
  raise "identity invocation captured"
end

try do
  LiveStoryAgentLoop.run(capture_state, [capture_admission], "t1745-proof",
    adapter: capture_adapter
  )
rescue
  error in RuntimeError ->
    unless error.message == "identity invocation captured", do: reraise(error, __STACKTRACE__)
end

{config, packet, result} =
  receive do
    {:captured_identity_invocation, config, packet, result} -> {config, packet, result}
  after
    1_000 -> raise "story-identity invocation was not captured"
  end

bounds = result.model_packet.packet_bounds
durable_story_keys = Enum.map(packet.visible_story_refs, & &1.story_key)
model_story_keys = Enum.map(result.model_packet.visible_story_refs, & &1.story_key)

model_ref_keys =
  result.model_packet.visible_story_refs
  |> Enum.map(&(Map.keys(&1) |> Enum.sort()))
  |> Enum.uniq()

unless fixture["tenant_id"] == "00000000-0000-0000-0000-00000000t328" and
         length(stories) == 663 and length(packet.visible_story_refs) == 663 and
         MapSet.size(MapSet.new(durable_story_keys)) == 663 and
         durable_story_keys == model_story_keys and
         model_ref_keys == [~w(last_material_at state story_key title version)a] and
         bounds.eligible_acl_visible_story_count == 663 and
         bounds.included_story_count == 663 and bounds.omitted_story_count == 0 and
         result.prompt_bytes == 164_520 and result.preflight_prompt_tokens <= 130_432 do
  raise "T1745 real Gibson proof assertion failed"
end

IO.puts(
  Jason.encode!(%{
    tenant_id: fixture["tenant_id"],
    captured_at: fixture["captured_at"],
    source_row_id: 15_164,
    source_message_id: message_id,
    packet_path:
      "LiveStoryAgentLoop.run/4 -> ACL eligibility -> quarantine filter -> overlap/material/key rank -> packet -> invoke_live_agent/3",
    config_version: config.config_version,
    prompt_version_hash: config.prompt_version_hash,
    eligible_story_count: bounds.eligible_acl_visible_story_count,
    included_story_count: bounds.included_story_count,
    omitted_story_count: bounds.omitted_story_count,
    truncation_reason: Map.get(bounds, :truncation_reason),
    prompt_bytes: result.prompt_bytes,
    preflight_prompt_tokens: result.preflight_prompt_tokens,
    prompt_byte_limit: 180_000,
    prompt_token_limit: 130_432,
    durable_story_order_sha256:
      :crypto.hash(:sha256, Jason.encode!(durable_story_keys)) |> Base.encode16(case: :lower),
    model_story_order_sha256:
      :crypto.hash(:sha256, Jason.encode!(model_story_keys)) |> Base.encode16(case: :lower),
    model_ref_keys: model_ref_keys,
    template_and_tokenizer: "live_gibson_on_eurisko",
    chat: "canned_after_preflight"
  })
)
