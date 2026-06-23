defmodule Primeradiant.Agentic.LiveGibsonTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Agentic.LiveGibson

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
    printf '%s\\n' '{"id":"chatcmpl-test","model":"Qwen3.6-35B-A3B-UD-Q4_K_M.gguf","choices":[{"message":{"content":"{\\"ok\\":true}"}}]}'
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
end
