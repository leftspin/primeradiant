defmodule Primeradiant.Agentic.LiveGibsonTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Agentic.LiveGibson

  defmodule LoadedStoryCard do
    defstruct [:__meta__, :id, :status, :inserted_at, :source_refs]
  end

  test "sends large chat completion payloads from a request file instead of argv" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-gibson-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    curl = Path.join(tmp, "curl")
    args_path = Path.join(tmp, "curl-args")
    copied_body_path = Path.join(tmp, "curl-body")

    File.write!(curl, """
    #!/usr/bin/env bash
    printf '%s\\n' "$@" > "#{args_path}"
    for arg in "$@"; do
      case "$arg" in
        @*) cp "${arg#@}" "#{copied_body_path}" ;;
      esac
    done
    printf '%s\\n' '{"id":"chatcmpl-test","model":"Qwen3.6-35B-A3B-UD-Q4_K_M.gguf","choices":[{"finish_reason":"stop","message":{"content":"{\\"ok\\":true}"}}]}'
    """)

    File.chmod!(curl, 0o755)

    old_path = System.get_env("PATH") || ""
    System.put_env("PATH", tmp <> ":" <> old_path)
    on_exit(fn -> System.put_env("PATH", old_path) end)

    config = %{
      role: :story_synthesis,
      system_prompt: "Return JSON.",
      task_prompt: "Synthesize the bounded packet.",
      output_schema: %{"ok" => "boolean"},
      max_tokens: 32
    }

    packet = %{body: String.duplicate("large packet ", 10_000)}

    assert %{output: %{ok: true}, response_id: "chatcmpl-test"} =
             LiveGibson.invoke(:story_synthesis, config, packet)

    args = File.read!(args_path)
    body = File.read!(copied_body_path)

    assert args =~ "--data-binary"
    assert args =~ ~r/@.*primeradiant-gibson-\d+\.json/
    refute args =~ "large packet"
    assert body =~ "large packet"
    assert byte_size(body) > 100_000
  end

  test "normalizes loaded schema structs in bounded packets before prompt encoding" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-gibson-struct-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    curl = Path.join(tmp, "curl")
    copied_body_path = Path.join(tmp, "curl-body")

    File.write!(curl, """
    #!/usr/bin/env bash
    for arg in "$@"; do
      case "$arg" in
        @*) cp "${arg#@}" "#{copied_body_path}" ;;
      esac
    done
    printf '%s\\n' '{"id":"chatcmpl-test","model":"Qwen3.6-35B-A3B-UD-Q4_K_M.gguf","choices":[{"finish_reason":"stop","message":{"content":"{\\"ok\\":true}"}}]}'
    """)

    File.chmod!(curl, 0o755)

    old_path = System.get_env("PATH") || ""
    System.put_env("PATH", tmp <> ":" <> old_path)
    on_exit(fn -> System.put_env("PATH", old_path) end)

    config = %{
      role: :story_synthesis,
      system_prompt: "Return JSON.",
      task_prompt: "Synthesize the bounded packet.",
      output_schema: %{"ok" => "boolean"},
      max_tokens: 32
    }

    inserted_at = ~U[2026-06-24 00:00:00Z]

    packet = %{
      role: :story_synthesis,
      prior_story_card_version: %LoadedStoryCard{
        __meta__: %{state: :loaded, source: "story_card_versions"},
        id: "card-1",
        status: "incomplete",
        inserted_at: inserted_at,
        source_refs: MapSet.new(["news_article:1"])
      }
    }

    assert %{output: %{ok: true}, response_id: "chatcmpl-test"} =
             LiveGibson.invoke(:story_synthesis, config, packet)

    body = copied_body_path |> File.read!() |> Jason.decode!()
    prompt = body["messages"] |> Enum.at(1) |> Map.fetch!("content") |> Jason.decode!()
    card = prompt["bounded_soup_packet"]["prior_story_card_version"]

    assert card["id"] == "card-1"
    assert card["status"] == "incomplete"
    assert card["inserted_at"] == DateTime.to_iso8601(inserted_at)
    assert card["source_refs"] == ["news_article:1"]
    refute Map.has_key?(card, "__meta__")
  end

  test "preflights bounded roles with the exact template and tokenized prompt" do
    responses = %{
      template: {Jason.encode!(%{prompt: "templated exact prompt"}), 0},
      tokenize: {Jason.encode!(%{tokens: [10, 20, 30]}), 0},
      chat:
        {completion_response("<think>private</think>{\"ok\":true}", %{
           "prompt_tokens" => 3,
           "completion_tokens" => 7
         }), 0}
    }

    with_fake_curl(responses, fn paths ->
      config = bounded_config()
      packet = %{packet_id_sha256: String.duplicate("a", 64), visible_story_refs: []}

      assert {:ok,
              preflight = %{
                prompt_body: prompt_body,
                prompt_bytes: prompt_bytes,
                preflight_prompt_tokens: 3
              }} = LiveGibson.preflight(:story_identity, config, packet)

      assert prompt_bytes == byte_size(prompt_body)

      template_body = paths.template_body |> File.read!() |> Jason.decode!()
      assert template_body["add_generation_prompt"] == true

      assert template_body["messages"] == [
               %{"role" => "system", "content" => config.system_prompt},
               %{"role" => "user", "content" => prompt_body}
             ]

      tokenize_body = paths.tokenize_body |> File.read!() |> Jason.decode!()
      assert tokenize_body == %{"content" => "templated exact prompt", "add_special" => false}

      assert result =
               LiveGibson.invoke_preflighted(:story_identity, config, packet, preflight)

      assert result.output == %{ok: true}
      assert result.model_packet == packet
      assert result.preflight_prompt_tokens == 3
      assert result.prompt_bytes == byte_size(result.prompt_body)
      assert result.finish_reason == "stop"

      assert result.provider_usage == %{
               prompt_tokens: 3,
               completion_tokens: 7,
               total_tokens: nil
             }

      raw_content = "<think>private</think>{\"ok\":true}"
      assert result.content_bytes == byte_size(raw_content)
      assert result.content_sha256 == sha256(raw_content)

      chat_body = paths.chat_body |> File.read!() |> Jason.decode!()
      assert chat_body["max_tokens"] == 640

      calls = File.read!(paths.call_log)
      assert length(Regex.scan(~r{/apply-template}, calls)) == 1
      assert length(Regex.scan(~r{/tokenize}, calls)) == 1
      assert length(Regex.scan(~r{/v1/chat/completions}, calls)) == 1
    end)
  end

  test "preflighted invocation rejects changed role, prompt, or bounds without chat completion" do
    responses = %{
      template: {Jason.encode!(%{prompt: "templated"}), 0},
      tokenize: {Jason.encode!(%{tokens: [1, 2]}), 0},
      chat: {completion_response("{\"ok\":true}"), 0}
    }

    with_fake_curl(responses, fn paths ->
      config = bounded_config()
      packet = %{visible_story_refs: []}
      assert {:ok, preflight} = LiveGibson.preflight(:story_identity, config, packet)

      diagnostic =
        terminal_diagnostic(fn ->
          LiveGibson.invoke_preflighted(
            :story_identity,
            config,
            packet,
            %{preflight | preflight_prompt_tokens: 130_433}
          )
        end)

      assert diagnostic["error_class"] == "invalid_preflight"
      refute File.exists?(paths.chat_body)
    end)
  end

  test "rejects bounded inference when exact token count exceeds 130432" do
    responses = %{
      template: {Jason.encode!(%{prompt: "templated"}), 0},
      tokenize: {Jason.encode!(%{tokens: List.duplicate(1, 130_433)}), 0},
      chat: {completion_response("{\"ok\":true}"), 0}
    }

    with_fake_curl(responses, fn paths ->
      assert {:error, diagnostic} =
               LiveGibson.preflight(:meaning_update, bounded_config(), %{visible_story_refs: []})

      assert diagnostic.error_class == "prompt_bound_exceeded"
      assert diagnostic.preflight_prompt_tokens == 130_433
      assert diagnostic.provider_usage == nil
      refute File.exists?(paths.chat_body)
    end)
  end

  test "keeps story synthesis byte-only and rejects prompts over 180000 bytes" do
    with_fake_curl(%{chat: {completion_response("{\"ok\":true}"), 0}}, fn paths ->
      assert_raise RuntimeError, fn ->
        LiveGibson.invoke(
          :story_synthesis,
          bounded_config(),
          %{body: String.duplicate("x", 180_001)}
        )
      end

      diagnostic =
        terminal_diagnostic(fn ->
          LiveGibson.invoke(
            :story_synthesis,
            bounded_config(),
            %{body: String.duplicate("x", 180_001)}
          )
        end)

      assert diagnostic["stage"] == "prompt_preflight"
      assert diagnostic["error_class"] == "prompt_bound_exceeded"
      assert diagnostic["prompt_bytes"] > 180_000
      assert diagnostic["preflight_prompt_tokens"] == nil
      refute File.exists?(paths.template_body)
      refute File.exists?(paths.tokenize_body)
      refute File.exists?(paths.chat_body)
    end)
  end

  test "does not apply the story prompt byte bound to unrelated LiveGibson roles" do
    with_fake_curl(%{chat: {completion_response("{\"ok\":true}"), 0}}, fn paths ->
      assert %{output: %{ok: true}, prompt_bytes: prompt_bytes} =
               LiveGibson.invoke(
                 :follow_on_review,
                 bounded_config(),
                 %{body: String.duplicate("x", 180_001)}
               )

      assert prompt_bytes > 180_000
      refute File.exists?(paths.template_body)
      refute File.exists?(paths.tokenize_body)
      assert File.exists?(paths.chat_body)
    end)
  end

  test "returns typed template, tokenizer, and transport terminal diagnostics" do
    cases = [
      {%{template: {"template unavailable", 7}}, "template_preflight", "template_error"},
      {%{
         template: {Jason.encode!(%{prompt: "templated"}), 0},
         tokenize: {"not json", 0}
       }, "tokenizer_preflight", "tokenizer_error"},
      {%{
         template: {Jason.encode!(%{prompt: "templated"}), 0},
         tokenize: {Jason.encode!(%{tokens: [1, 2]}), 0},
         chat: {"transport unavailable", 7}
       }, "chat_transport", "transport_error"}
    ]

    Enum.each(cases, fn {responses, stage, error_class} ->
      with_fake_curl(responses, fn _paths ->
        diagnostic =
          terminal_diagnostic(fn ->
            LiveGibson.invoke(:story_identity, bounded_config(), %{visible_story_refs: []})
          end)

        assert diagnostic["stage"] == stage
        assert diagnostic["error_class"] == error_class
        assert diagnostic["prompt_bytes"] > 0
        assert diagnostic["provider_usage"] == nil
        assert diagnostic["response_id"] == nil
        assert diagnostic["finish_reason"] == nil
        assert diagnostic["content_bytes"] == nil
        assert diagnostic["content_sha256"] == nil
      end)
    end)
  end

  test "returns a typed outer-decode terminal diagnostic" do
    with_bounded_responses({"not json", 0}, fn ->
      diagnostic = invoke_diagnostic()

      assert diagnostic["stage"] == "outer_response_decode"
      assert diagnostic["error_class"] == "outer_decode_error"
      assert diagnostic["provider_usage"] == nil
      assert diagnostic["preflight_prompt_tokens"] == 2
    end)
  end

  for {name, response, error_class} <- [
        {"id", %{"model" => "Qwen", "choices" => []}, "missing_id"},
        {"model", %{"id" => "chatcmpl-test", "choices" => []}, "missing_model"},
        {"choice", %{"id" => "chatcmpl-test", "model" => "Qwen", "choices" => []},
         "missing_choice"},
        {"content",
         %{
           "id" => "chatcmpl-test",
           "model" => "Qwen",
           "choices" => [%{"finish_reason" => "stop", "message" => %{}}]
         }, "missing_content"},
        {"finish_reason",
         %{
           "id" => "chatcmpl-test",
           "model" => "Qwen",
           "choices" => [%{"message" => %{"content" => "{\"ok\":true}"}}]
         }, "missing_finish_reason"}
      ] do
    test "returns a typed missing #{name} terminal diagnostic" do
      response = unquote(Macro.escape(response))
      error_class = unquote(error_class)

      response =
        Map.put(response, "usage", %{
          "prompt_tokens" => 11,
          "completion_tokens" => 12
        })

      with_bounded_responses({Jason.encode!(response), 0}, fn ->
        diagnostic = invoke_diagnostic()
        assert diagnostic["stage"] == "completion_response"
        assert diagnostic["error_class"] == error_class

        if error_class == "missing_model" do
          assert diagnostic["response_id"] == "chatcmpl-test"
        end

        assert diagnostic["provider_usage"] == %{
                 "prompt_tokens" => 11,
                 "completion_tokens" => 12,
                 "total_tokens" => nil
               }
      end)
    end
  end

  test "returns raw-content diagnostics for non-stop and malformed inner responses" do
    cases = [
      {completion_response("truncated", nil, "length"), "completion_response", "non_stop_finish",
       "length"},
      {completion_response("not json"), "inner_response_decode", "malformed_inner_json", "stop"}
    ]

    Enum.each(cases, fn {response, stage, error_class, finish_reason} ->
      with_bounded_responses({response, 0}, fn ->
        diagnostic = invoke_diagnostic()
        content = if error_class == "non_stop_finish", do: "truncated", else: "not json"

        assert diagnostic["stage"] == stage
        assert diagnostic["error_class"] == error_class
        assert diagnostic["response_id"] == "chatcmpl-test"
        assert diagnostic["finish_reason"] == finish_reason
        assert diagnostic["content_bytes"] == byte_size(content)
        assert diagnostic["content_sha256"] == sha256(content)
      end)
    end)
  end

  defp bounded_config do
    %{
      role: :story_identity,
      system_prompt: "Return JSON.",
      task_prompt: "Use only the bounded packet.",
      output_schema: %{"ok" => "boolean"},
      max_tokens: 32
    }
  end

  defp completion_response(content, usage \\ nil, finish_reason \\ "stop") do
    %{
      "id" => "chatcmpl-test",
      "model" => "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf",
      "choices" => [
        %{"finish_reason" => finish_reason, "message" => %{"content" => content}}
      ]
    }
    |> then(fn response -> if usage, do: Map.put(response, "usage", usage), else: response end)
    |> Jason.encode!()
  end

  defp with_bounded_responses(chat_response, fun) do
    with_fake_curl(
      %{
        template: {Jason.encode!(%{prompt: "templated"}), 0},
        tokenize: {Jason.encode!(%{tokens: [1, 2]}), 0},
        chat: chat_response
      },
      fn _paths -> fun.() end
    )
  end

  defp invoke_diagnostic do
    terminal_diagnostic(fn ->
      LiveGibson.invoke(:story_identity, bounded_config(), %{visible_story_refs: []})
    end)
  end

  defp terminal_diagnostic(fun) do
    error = assert_raise RuntimeError, fun
    Jason.decode!(error.message)
  end

  defp with_fake_curl(responses, fun) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-gibson-fake-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    paths = %{
      template_body: Path.join(tmp, "template-body"),
      tokenize_body: Path.join(tmp, "tokenize-body"),
      chat_body: Path.join(tmp, "chat-body"),
      call_log: Path.join(tmp, "call-log")
    }

    response_paths =
      Map.new([:template, :tokenize, :chat], fn key ->
        response_path = Path.join(tmp, "#{key}-response")
        {body, _status} = Map.get(responses, key, {"missing fake response", 98})
        File.write!(response_path, body)
        {key, response_path}
      end)

    statuses =
      Map.new([:template, :tokenize, :chat], fn key ->
        {_body, status} = Map.get(responses, key, {"missing fake response", 98})
        {key, status}
      end)

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
    printf '%s\\n' "$url" >> "#{paths.call_log}"
    case "$url" in
      */apply-template)
        cp "$request" "#{paths.template_body}"
        cat "#{response_paths.template}"
        exit #{statuses.template}
        ;;
      */tokenize)
        cp "$request" "#{paths.tokenize_body}"
        cat "#{response_paths.tokenize}"
        exit #{statuses.tokenize}
        ;;
      */v1/chat/completions)
        cp "$request" "#{paths.chat_body}"
        cat "#{response_paths.chat}"
        exit #{statuses.chat}
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
