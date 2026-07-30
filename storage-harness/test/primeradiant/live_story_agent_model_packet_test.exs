defmodule Primeradiant.LiveStoryAgentModelPacketTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Agentic.LiveGibson
  alias Primeradiant.Ingestion.Admission

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    Input,
    LiveStoryAgentLoop,
    State,
    Story,
    StoryEvent
  }

  @row_15164_message_id "b9edbc9f-1fe8-437a-bd1f-2f8b2530bddf"
  @row_15164_title "Shippers Face Deepening Dilemma as U.S. and Iran Vie for Control of the Strait - The New York Times"
  @production_catalog_fixture Path.expand(
                                "../fixtures/t1745-production-catalog.json",
                                __DIR__
                              )

  test "production row 15164 includes all 663 current eligible compact refs" do
    refs = production_story_refs()

    packet =
      durable_packet(%{
        external_id: @row_15164_message_id,
        source_ref: "news_article:#{@row_15164_message_id}",
        snippet: @row_15164_title,
        visible_story_refs: refs
      })

    assert packet.external_id == @row_15164_message_id
    assert packet.source_ref == "news_article:#{@row_15164_message_id}"
    assert packet.snippet == @row_15164_title
    assert packet.source_provenance["source_name"] == "Google News World"
    assert packet.source_provenance["published_at"] == "2026-07-10T18:02:56Z"
    assert packet.source_provenance["accepted_at"] == "2026-07-10 21:23:42"
    assert length(packet.visible_story_refs) == 663
    assert Enum.all?(packet.visible_story_refs, &Map.has_key?(&1, :structural_facts))
    assert Enum.all?(packet.visible_story_refs, &Map.has_key?(&1, :evidence_input_refs))

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      assert %{output: %{ok: true}} =
               LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})

      model_packet = chat_model_packet(paths.chat_body)
      assert length(model_packet["visible_story_refs"]) == 663

      assert model_packet["packet_bounds"] == %{
               "eligible_acl_visible_story_count" => 663,
               "included_story_count" => 663,
               "omitted_story_count" => 0
             }

      assert Enum.map(model_packet["visible_story_refs"], & &1["story_key"]) ==
               Enum.map(refs, & &1.story_key)

      assert Enum.all?(model_packet["visible_story_refs"], fn ref ->
               Map.keys(ref) |> Enum.sort() ==
                 ~w(last_material_at state story_key title version)
             end)

      refute Map.has_key?(model_packet["packet_bounds"], "truncation_reason")
    end)
  end

  test "production row 15164 reaches model invocation through ACL ranking and packet construction" do
    {state, admission, fixture_refs} = production_path_state()
    capture_key = {__MODULE__, make_ref()}

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      adapter = fn config, packet, ctx ->
        result = LiveStoryAgentLoop.invoke_live_agent(config, packet, ctx)
        Process.put(capture_key, {packet, result})
        raise "captured production identity invocation"
      end

      assert_raise RuntimeError, "captured production identity invocation", fn ->
        LiveStoryAgentLoop.run(state, [admission], "flynn", adapter: adapter)
      end

      {durable_packet, result} = Process.get(capture_key)
      expected_keys = expected_ranked_story_keys(fixture_refs)

      assert durable_packet.external_id == @row_15164_message_id
      assert durable_packet.source_ref == "news_article:#{@row_15164_message_id}"
      assert durable_packet.snippet == @row_15164_title
      assert length(durable_packet.visible_story_refs) == 663

      assert Enum.map(durable_packet.visible_story_refs, & &1.story_key) == expected_keys

      assert Enum.all?(durable_packet.visible_story_refs, fn ref ->
               Map.has_key?(ref, :structural_facts) and
                 Map.has_key?(ref, :evidence_input_refs) and
                 length(ref.evidence_input_refs) == 1
             end)

      assert length(result.model_packet.visible_story_refs) == 663

      assert result.model_packet.packet_bounds == %{
               eligible_acl_visible_story_count: 663,
               included_story_count: 663,
               omitted_story_count: 0
             }

      assert Enum.map(result.model_packet.visible_story_refs, & &1.story_key) == expected_keys

      assert Enum.all?(result.model_packet.visible_story_refs, fn ref ->
               Map.keys(ref) |> Enum.sort() ==
                 ~w(last_material_at state story_key title version)a
             end)

      assert chat_model_packet(paths.chat_body)["packet_bounds"] == %{
               "eligible_acl_visible_story_count" => 663,
               "included_story_count" => 663,
               "omitted_story_count" => 0
             }
    end)
  end

  test "huge durable identifiers fail unreduced bounds but the zero-ref projection passes without leaks" do
    external_id = "external-full:" <> String.duplicate("x", 190_000)
    source_ref = "source-full:" <> String.duplicate("y", 190_000)
    correlation_id = "correlation:#{source_ref}"
    packet_id = "packet:story_identity:#{correlation_id}"

    packet =
      durable_packet(%{
        external_id: external_id,
        source_ref: source_ref,
        packet_id: packet_id,
        visible_story_refs: []
      })

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      assert {:error, diagnostic} =
               LiveGibson.preflight(:story_identity, identity_config(), packet)

      assert diagnostic.prompt_bytes > 180_000
      assert diagnostic.preflight_prompt_tokens == 130_433
      assert diagnostic.error_class == "prompt_bound_exceeded"

      assert %{model_packet: model_packet} =
               LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})

      chat_prompt = File.read!(paths.chat_body)
      chat_packet = chat_model_packet(paths.chat_body)

      assert model_packet.packet_bounds == %{
               eligible_acl_visible_story_count: 0,
               included_story_count: 0,
               omitted_story_count: 0
             }

      assert chat_packet["packet_id_sha256"] == sha256(Jason.encode!(packet_id))
      refute chat_prompt =~ external_id
      refute chat_prompt =~ source_ref
      refute chat_prompt =~ packet_id
      refute chat_prompt =~ correlation_id
    end)
  end

  test "closed model projection excludes unexpected durable admission and provenance fields" do
    unexpected_value = "durable-admission-provenance-must-not-enter-model"

    packet =
      durable_packet(%{
        admission_provenance: %{
          "unexpected_future_field" => unexpected_value
        }
      })

    assert packet.admission_provenance["unexpected_future_field"] == unexpected_value

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      assert %{model_packet: model_packet} =
               LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})

      refute Map.has_key?(model_packet, :admission_provenance)

      assert model_packet |> Map.keys() |> Enum.sort() ==
               [
                 :bounded_source,
                 :input_id,
                 :output_visibility,
                 :packet_bounds,
                 :packet_contract,
                 :packet_id_sha256,
                 :raw_database_access,
                 :role,
                 :snippet,
                 :soup_candidate_hint,
                 :traversal_depth,
                 :visible_story_refs
               ]
               |> Enum.sort()

      chat_prompt = File.read!(paths.chat_body)
      chat_packet = chat_model_packet(paths.chat_body)

      refute Map.has_key?(chat_packet, "admission_provenance")
      refute chat_prompt =~ unexpected_value
    end)
  end

  test "adversarial rich catalog truncates titles by Unicode grapheme and excludes rich material" do
    grapheme = "e\u0301"
    excluded_marker = "excluded-rich-material-must-not-enter-model"

    refs =
      rich_story_refs(20)
      |> Enum.map(fn ref ->
        %{
          ref
          | title: String.duplicate(grapheme, 260) <> excluded_marker,
            structural_facts: %{
              "marker" => excluded_marker,
              "material" => String.duplicate("structural-fact", 10_000)
            },
            evidence_input_refs: Enum.map(1..1_000, &"#{excluded_marker}:evidence:#{&1}")
        }
      end)

    packet =
      durable_packet(%{
        visible_story_refs: refs,
        source_provenance: %{
          "marker" => excluded_marker,
          "chain" => Enum.map(1..1_000, &"#{excluded_marker}:provenance:#{&1}")
        },
        content_span_refs: Enum.map(1..1_000, &"#{excluded_marker}:span:#{&1}"),
        evidence_refs: Enum.map(1..1_000, &"#{excluded_marker}:source-evidence:#{&1}"),
        soup_candidate_hint: %{
          suggested_story_key: "story-0001",
          suggested_classification: "no_op",
          overlap_count: 11,
          input_token_count: 12,
          rationale:
            "committed story/input token overlap indicates repeated nonmaterial source pressure",
          story_key: "durable-hint-story-key",
          overlap_tokens: Enum.map(1..1_000, &"#{excluded_marker}:token:#{&1}"),
          evidence_input_refs: Enum.map(1..1_000, &"#{excluded_marker}:hint-evidence:#{&1}")
        }
      })

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      assert %{model_packet: model_packet, prompt_bytes: prompt_bytes} =
               LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})

      assert prompt_bytes <= 180_000
      assert length(model_packet.visible_story_refs) == 20

      assert Enum.all?(model_packet.visible_story_refs, fn ref ->
               String.length(ref.title) == 240 and
                 length(String.graphemes(ref.title)) == 240 and
                 ref.title == String.duplicate(grapheme, 240) and
                 Map.keys(ref) |> Enum.sort() ==
                   ~w(last_material_at state story_key title version)a
             end)

      chat_prompt = File.read!(paths.chat_body)
      refute chat_prompt =~ excluded_marker
      refute chat_prompt =~ "durable-hint-story-key"
    end)
  end

  test "oversize ranked catalog packs the largest fitting prefix without skipping" do
    refs = rich_story_refs(20)
    packet = durable_packet(%{visible_story_refs: refs})

    with_fake_gibson([max_story_refs: 7], fn paths ->
      assert %{model_packet: model_packet} =
               LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})

      assert Enum.map(model_packet.visible_story_refs, & &1.story_key) ==
               refs |> Enum.take(7) |> Enum.map(& &1.story_key)

      assert model_packet.packet_bounds == %{
               eligible_acl_visible_story_count: 20,
               included_story_count: 7,
               omitted_story_count: 13,
               truncation_reason: "live_story_agent_prompt_context_bound"
             }

      chat_packet = chat_model_packet(paths.chat_body)

      assert Enum.map(chat_packet["visible_story_refs"], & &1["story_key"]) ==
               refs |> Enum.take(7) |> Enum.map(& &1.story_key)
    end)
  end

  test "meaning bounds count only the role-selected story set" do
    refs = rich_story_refs(20)

    new_story_packet =
      durable_packet(%{
        role: :meaning_update,
        visible_story_refs: refs,
        story_identity: %{
          story_key: "story-0007",
          classification: "new_story",
          confidence: 0.8
        }
      })

    with_fake_gibson([max_story_refs: 1_000], fn paths ->
      assert %{model_packet: model_packet} =
               LiveStoryAgentLoop.invoke_live_agent(
                 meaning_config(),
                 new_story_packet,
                 %{}
               )

      assert model_packet.visible_story_refs == []

      assert model_packet.packet_bounds == %{
               eligible_acl_visible_story_count: 0,
               included_story_count: 0,
               omitted_story_count: 0
             }

      refute Map.has_key?(
               chat_model_packet(paths.chat_body)["packet_bounds"],
               "truncation_reason"
             )
    end)

    existing_story_packet =
      put_in(new_story_packet, [:story_identity, :classification], "substantive_update")

    with_fake_gibson([max_story_refs: 1_000], fn _paths ->
      assert %{model_packet: model_packet} =
               LiveStoryAgentLoop.invoke_live_agent(
                 meaning_config(),
                 existing_story_packet,
                 %{}
               )

      assert Enum.map(model_packet.visible_story_refs, & &1.story_key) == ["story-0007"]

      assert model_packet.packet_bounds == %{
               eligible_acl_visible_story_count: 1,
               included_story_count: 1,
               omitted_story_count: 0
             }
    end)
  end

  test "non-bound preflight errors are raised after one attempt without tokenizer or chat retry" do
    packet = durable_packet(%{visible_story_refs: rich_story_refs(20)})

    with_fake_gibson([max_story_refs: 7, template_failure: true], fn paths ->
      error =
        assert_raise RuntimeError, fn ->
          LiveStoryAgentLoop.invoke_live_agent(identity_config(), packet, %{})
        end

      assert Jason.decode!(error.message)["error_class"] == "template_error"
      assert File.read!(paths.template_calls) == "1"
      refute File.exists?(paths.tokenize_calls)
      refute File.exists?(paths.chat_body)
    end)
  end

  test "terminal model failure occurs before the loop inserts an AgentRun" do
    tenant_id = Ecto.UUID.generate()

    input =
      ChangesetStore.insert!(Input, %{
        tenant_id: tenant_id,
        source_type: "news_article",
        external_id: @row_15164_message_id,
        observed_at: ~U[2026-07-10 21:23:42.501095Z],
        title: @row_15164_title,
        body_text: @row_15164_title,
        object_uri: "https://news.google.test/world/#{@row_15164_message_id}",
        content_sha256: sha256(@row_15164_title),
        acl: %{"privacy" => "public", "participants" => []},
        normalized: %{},
        facts: %{},
        background: %{},
        questions: %{},
        colors: [],
        topic_tokens: []
      })

    state = State.new(tenant_id: tenant_id) |> State.append(:inputs, input)

    admission = %{
      source_ref: Admission.input_ref(input),
      source_type: input.source_type,
      external_id: input.external_id,
      observed_at: input.observed_at,
      content_sha256: input.content_sha256,
      content_span_refs: ["span:row-15164"],
      evidence_refs: ["evidence:row-15164"],
      source_provenance: %{
        "source_name" => "Google News World",
        "published_at" => "2026-07-10T18:02:56Z",
        "accepted_at" => "2026-07-10 21:23:42"
      }
    }

    with_fake_gibson([max_story_refs: 1_000, chat_response: "not json"], fn _paths ->
      assert_raise RuntimeError, fn ->
        LiveStoryAgentLoop.run(state, [admission], "flynn")
      end
    end)

    assert state.agent_runs == []
    assert state.package_acknowledgements == []
  end

  defp identity_config do
    %{
      role: :story_identity,
      system_prompt: "Return JSON.",
      task_prompt: "Use only the bounded packet.",
      output_schema: %{"ok" => "boolean"},
      max_tokens: 640,
      config_version: "story-identity.test",
      prompt_version_hash: String.duplicate("a", 64)
    }
  end

  defp meaning_config, do: %{identity_config() | role: :meaning_update}

  defp durable_packet(overrides) do
    source_ref = Map.get(overrides, :source_ref, "news_article:row-15164")
    role = Map.get(overrides, :role, :story_identity)
    correlation_id = "correlation:#{source_ref}"

    Map.merge(
      %{
        packet_id: "packet:#{role}:#{correlation_id}",
        packet_contract: :acl_scoped_soup_packet,
        role: role,
        output_visibility: "public",
        source_ref: source_ref,
        input_id: Ecto.UUID.generate(),
        external_id: "row-15164",
        evidence_refs: ["evidence:row-15164"],
        content_span_refs: ["span:row-15164"],
        source_provenance: %{
          "source_name" => "Google News World",
          "published_at" => "2026-07-10T18:02:56Z",
          "accepted_at" => "2026-07-10 21:23:42"
        },
        snippet: @row_15164_title,
        visible_story_refs: [],
        soup_candidate_hint: nil,
        traversal_depth: 1,
        raw_database_access: false
      },
      overrides
    )
  end

  defp rich_story_refs(count) do
    Enum.map(1..count, fn index ->
      timestamp = DateTime.add(~U[2026-07-10 21:20:42.000000Z], -index, :second)

      %{
        story_key: "story-#{String.pad_leading(Integer.to_string(index), 4, "0")}",
        title: "Story #{index}",
        version: index,
        state: "active",
        structural_facts: %{"index" => index, "material" => String.duplicate("fact", 8)},
        updated_at_story: DateTime.to_iso8601(timestamp),
        last_material_at: DateTime.to_iso8601(timestamp),
        evidence_input_refs: ["news_article:story-#{index}"]
      }
    end)
  end

  defp production_story_refs do
    fixture = @production_catalog_fixture |> File.read!() |> Jason.decode!()

    assert fixture["tenant_id"] == "00000000-0000-0000-0000-00000000t328"
    assert fixture["captured_at"] == "2026-07-16"
    assert length(fixture["refs"]) == 663

    Enum.map(fixture["refs"], fn ref ->
      %{
        story_key: ref["story_key"],
        title: ref["title"],
        version: ref["version"],
        state: ref["state"],
        structural_facts: %{"captured_production_story" => true},
        updated_at_story: ref["updated_at_story"],
        last_material_at: ref["last_material_at"],
        evidence_input_refs: ["captured:#{ref["story_key"]}"]
      }
    end)
  end

  defp production_path_state do
    fixture = @production_catalog_fixture |> File.read!() |> Jason.decode!()
    tenant_id = fixture["tenant_id"]

    source_input = %Input{
      id: Ecto.UUID.generate(),
      tenant_id: tenant_id,
      source_type: "news_article",
      external_id: @row_15164_message_id,
      observed_at: ~U[2026-07-10 21:23:42.501095Z],
      title: @row_15164_title,
      body_text: @row_15164_title,
      object_uri: "https://news.google.test/world/#{@row_15164_message_id}",
      content_sha256: sha256(@row_15164_title),
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
        input_id = Ecto.UUID.generate()
        title = captured_title(ref["title"])
        updated_at_story = parse_timestamp!(ref["updated_at_story"])
        last_material_at = parse_timestamp!(ref["last_material_at"])

        story = %Story{
          id: story_id,
          tenant_id: tenant_id,
          story_key: ref["story_key"],
          title: title,
          state: ref["state"],
          version: ref["version"],
          first_observed_at: updated_at_story,
          updated_at_story: updated_at_story,
          last_material_at: last_material_at,
          structural_facts: %{"captured_production_story" => true}
        }

        linked_input = %Input{
          id: input_id,
          tenant_id: tenant_id,
          source_type: "news_article",
          external_id: "captured-story-#{index}",
          observed_at: updated_at_story,
          title: title,
          body_text: title,
          content_sha256: sha256(title),
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
          tenant_id: tenant_id,
          story_id: story_id,
          input_id: input_id,
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

    state = %{
      State.new(tenant_id: tenant_id)
      | inputs: [source_input | linked_inputs],
        stories: stories,
        story_events: story_events
    }

    admission = %{
      source_ref: Admission.input_ref(source_input),
      source_type: source_input.source_type,
      external_id: source_input.external_id,
      observed_at: source_input.observed_at,
      content_sha256: source_input.content_sha256,
      content_span_refs: ["span:row-15164"],
      evidence_refs: ["evidence:row-15164"],
      source_provenance: %{
        "source_name" => "Google News World",
        "published_at" => "2026-07-10T18:02:56Z",
        "accepted_at" => "2026-07-10 21:23:42"
      }
    }

    {state, admission, fixture["refs"]}
  end

  defp expected_ranked_story_keys(refs) do
    source_tokens = candidate_tokens([@row_15164_title, @row_15164_title])

    refs
    |> Enum.map(fn ref ->
      title = captured_title(ref["title"])
      story_tokens = candidate_tokens([title, title])
      overlap_count = MapSet.intersection(source_tokens, story_tokens) |> MapSet.size()
      last_material_at = parse_timestamp!(ref["last_material_at"])

      {ref["story_key"], overlap_count, last_material_at}
    end)
    |> Enum.sort_by(fn {story_key, overlap_count, last_material_at} ->
      {-overlap_count, -DateTime.to_unix(last_material_at, :microsecond), story_key}
    end)
    |> Enum.map(&elem(&1, 0))
  end

  defp candidate_tokens(values) do
    stopwords =
      MapSet.new(
        ~w(a an and are as at be by for from has have in into is it its of on or the this to with without)
      )

    values
    |> Enum.join(" ")
    |> String.downcase()
    |> String.split(~r/[^a-z0-9]+/, trim: true)
    |> Enum.reject(&(String.length(&1) < 3 or MapSet.member?(stopwords, &1)))
    |> MapSet.new()
  end

  defp captured_title(value) when is_binary(value), do: value
  defp captured_title(value) when is_list(value), do: Enum.join(value, " ")

  defp parse_timestamp!(value) do
    {:ok, timestamp, 0} = DateTime.from_iso8601(value)
    timestamp
  end

  defp chat_model_packet(path) do
    path
    |> File.read!()
    |> Jason.decode!()
    |> get_in(["messages", Access.at(1), "content"])
    |> Jason.decode!()
    |> Map.fetch!("bounded_soup_packet")
  end

  defp with_fake_gibson(opts, fun) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-story-model-packet-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    paths = %{
      chat_body: Path.join(tmp, "chat-body"),
      max_story_refs: Path.join(tmp, "max-story-refs"),
      chat_response: Path.join(tmp, "chat-response"),
      template_calls: Path.join(tmp, "template-calls"),
      tokenize_calls: Path.join(tmp, "tokenize-calls"),
      template_failure: Path.join(tmp, "template-failure")
    }

    File.write!(paths.max_story_refs, Integer.to_string(Keyword.fetch!(opts, :max_story_refs)))
    File.write!(paths.template_failure, to_string(Keyword.get(opts, :template_failure, false)))

    File.write!(
      paths.chat_response,
      Keyword.get(
        opts,
        :chat_response,
        Jason.encode!(%{
          "id" => "chatcmpl-storage-test",
          "model" => "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf",
          "choices" => [
            %{"finish_reason" => "stop", "message" => %{"content" => "{\"ok\":true}"}}
          ],
          "usage" => %{
            "prompt_tokens" => 2,
            "completion_tokens" => 3,
            "total_tokens" => 5
          }
        })
      )
    )

    curl = Path.join(tmp, "curl")

    File.write!(curl, """
    #!/usr/bin/env bash
    url=""
    request=""
    for arg in "$@"; do
      case "$arg" in
        http://*) url="$arg" ;;
        @*) request="${arg#@}" ;;
      esac
    done
    case "$url" in
      */apply-template)
        calls=0
        [ -f "#{paths.template_calls}" ] && calls=$(cat "#{paths.template_calls}")
        printf '%s' "$((calls + 1))" > "#{paths.template_calls}"
        if [ "$(cat "#{paths.template_failure}")" = "true" ]; then
          printf '%s\n' 'template unavailable'
          exit 7
        fi
        jq -c '{prompt: .messages[1].content}' "$request"
        ;;
      */tokenize)
        calls=0
        [ -f "#{paths.tokenize_calls}" ] && calls=$(cat "#{paths.tokenize_calls}")
        printf '%s' "$((calls + 1))" > "#{paths.tokenize_calls}"
        prompt_bytes=$(jq -r '.content' "$request" | wc -c | tr -d ' ')
        story_refs=$(jq -r '.content | fromjson | .bounded_soup_packet.visible_story_refs | length' "$request")
        max_story_refs=$(cat "#{paths.max_story_refs}")
        if [ "$prompt_bytes" -gt 180000 ] || [ "$story_refs" -gt "$max_story_refs" ]; then
          jq -nc '{tokens: [range(0; 130433)]}'
        else
          printf '%s\n' '{"tokens":[1,2]}'
        fi
        ;;
      */v1/chat/completions)
        cp "$request" "#{paths.chat_body}"
        cat "#{paths.chat_response}"
        ;;
      *) exit 99 ;;
    esac
    """)

    File.chmod!(curl, 0o755)
    old_path = System.get_env("PATH") || ""
    System.put_env("PATH", tmp <> ":" <> old_path)

    try do
      fun.(paths)
    after
      System.put_env("PATH", old_path)
      File.rm_rf!(tmp)
    end
  end

  defp sha256(content) do
    :sha256
    |> :crypto.hash(content)
    |> Base.encode16(case: :lower)
  end
end
