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

    packet = %{body: String.duplicate("large packet ", 20_000)}

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

  test "raises a typed error when the completion does not stop" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-gibson-non-stop-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    curl = Path.join(tmp, "curl")

    File.write!(curl, """
    #!/usr/bin/env bash
    printf '%s\\n' '{"id":"chatcmpl-test","model":"Qwen3.6-35B-A3B-UD-Q4_K_M.gguf","choices":[{"finish_reason":"length","message":{"content":"truncated"}}]}'
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

    assert_raise LiveGibson.NonStopFinishError, fn ->
      LiveGibson.invoke(:story_synthesis, config, %{body: "bounded packet"})
    end
  end

  test "raises a typed error when the completion omits a required response field" do
    tmp = Path.join(System.tmp_dir!(), "primeradiant-live-gibson-response-shape-#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    curl = Path.join(tmp, "curl")
    File.write!(curl, """
    #!/usr/bin/env bash
    printf '%s\\n' '{"id":"chatcmpl-test","choices":[{"finish_reason":"stop","message":{"content":"{\\"ok\\":true}"}}]}'
    """)
    File.chmod!(curl, 0o755)

    old_path = System.get_env("PATH") || ""
    System.put_env("PATH", tmp <> ":" <> old_path)
    on_exit(fn -> System.put_env("PATH", old_path) end)

    config = %{role: :story_synthesis, system_prompt: "Return JSON.", task_prompt: "Synthesize.", output_schema: %{"ok" => "boolean"}, max_tokens: 32}

    assert_raise LiveGibson.ResponseShapeError, fn ->
      LiveGibson.invoke(:story_synthesis, config, %{body: "bounded packet"})
    end
  end
end
