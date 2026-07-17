defmodule Primeradiant.LiveStoryAgentFailureMatrixTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 120_000

  alias Primeradiant.Ingestion.Admission

  alias Primeradiant.StorageHarness.{
    AgentRun,
    ChangesetStore,
    Input,
    LiveStoryAgentLoop,
    State
  }

  @tenant "00000000-0000-0000-0000-00000000f174"
  @initial_cursor "2026-06-03 05:00:00|1"

  test "AgentRun insertion observer detects a known insertion attempt" do
    {_run, attempts} =
      trace_agent_run_insert_attempts(fn ->
        ChangesetStore.insert!(AgentRun, %{
          tenant_id: @tenant,
          agent_run_key: "agent-run:failure-matrix-positive-control",
          agent_type: "story_identity",
          scope: %{},
          status: "succeeded"
        })
      end)

    assert [%{agent_run_key: "agent-run:failure-matrix-positive-control"}] = attempts
  end

  test "every terminal LiveGibson failure retains the durable package and watcher cursor" do
    Enum.with_index(failure_cases(), 1)
    |> Enum.each(fn {failure, index} ->
      diagnostic = assert_loop_failure(failure)

      assert diagnostic["stage"] == failure.stage
      assert diagnostic["error_class"] == failure.error_class

      assert Map.keys(diagnostic) |> Enum.sort() ==
               ~w(content_bytes content_sha256 error_class finish_reason preflight_prompt_tokens prompt_bytes provider_usage response_id stage)

      assert_watcher_retains_failure(diagnostic, failure.name, index)
    end)
  end

  defp assert_loop_failure(failure) do
    state = state_with_input()
    admission = admission(hd(state.inputs))

    with_fake_gibson(failure, fn ->
      {error, agent_run_insert_attempts} =
        trace_agent_run_insert_attempts(fn ->
          assert_raise RuntimeError, fn ->
            LiveStoryAgentLoop.run(state, [admission], "flynn")
          end
        end)

      refute error.message =~ "\n"
      assert agent_run_insert_attempts == []
      assert state.agent_runs == []
      assert state.package_acknowledgements == []

      Jason.decode!(error.message)
    end)
  end

  defp trace_agent_run_insert_attempts(fun) do
    tracee = self()
    tracer = spawn(fn -> collect_agent_run_insert_attempts([]) end)

    :erlang.trace_pattern({ChangesetStore, :insert!, 2}, true, [])
    :erlang.trace(tracee, true, [:call, {:tracer, tracer}])

    try do
      result = fun.()
      :erlang.trace(tracee, false, [:call])

      delivered_ref = :erlang.trace_delivered(tracee)
      assert_receive {:trace_delivered, ^tracee, ^delivered_ref}, 1_000

      send(tracer, {:read, tracee})
      assert_receive {:agent_run_insert_attempts, attempts}, 1_000

      {result, attempts}
    after
      :erlang.trace(tracee, false, [:call])
      :erlang.trace_pattern({ChangesetStore, :insert!, 2}, false, [])
      send(tracer, :stop)
    end
  end

  defp collect_agent_run_insert_attempts(attempts) do
    receive do
      {:trace, _pid, :call, {ChangesetStore, :insert!, [AgentRun, attrs]}} ->
        collect_agent_run_insert_attempts([attrs | attempts])

      {:read, caller} ->
        send(caller, {:agent_run_insert_attempts, Enum.reverse(attempts)})
        collect_agent_run_insert_attempts([])

      :stop ->
        :ok
    end
  end

  defp assert_watcher_retains_failure(diagnostic, name, index) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-failure-matrix-#{name}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)

    try do
      source_db = Path.join(tmp, "daemon.sqlite3")
      state_root = Path.join(tmp, "state")
      stub_state = Path.join(tmp, "stub-state")
      bin_dir = Path.join(tmp, "bin")
      ssh_key = Path.join(tmp, "ssh-key")
      cursor_file = Path.join(state_root, "cursor.txt")
      run_id = "failure-matrix-#{index}-#{name}"
      diagnostic_line = Jason.encode!(diagnostic)

      File.mkdir_p!(state_root)
      File.mkdir_p!(stub_state)
      File.write!(ssh_key, "stub-key")
      File.write!(cursor_file, @initial_cursor <> "\n")
      File.write!(Path.join(stub_state, "fail-message"), diagnostic_line)

      create_source_db!(source_db)
      write_failing_ssh!(bin_dir, stub_state)

      watcher_script = Path.expand("scripts/r1/live_subspace_daemon_watcher_once.sh")

      {output, status} =
        System.cmd(
          watcher_script,
          [
            "--source-db",
            source_db,
            "--tenant",
            @tenant,
            "--state-root",
            state_root,
            "--eurisko-target",
            "clu@eurisko-test",
            "--eurisko-repo",
            "/home/clu/src/primeradiant",
            "--ssh-key",
            ssh_key,
            "--limit",
            "10",
            "--run-id",
            run_id
          ],
          stderr_to_stdout: true,
          env: [{"PATH", bin_dir <> ":" <> System.get_env("PATH")}]
        )

      assert status == 17
      assert output =~ "live watcher package consumption failed"
      assert File.read!(cursor_file) == @initial_cursor <> "\n"
      assert File.exists?(Path.join(stub_state, "consume-attempted"))
      refute File.exists?(Path.join(stub_state, "ack-emitted"))

      failed_report =
        Path.join(state_root, "#{run_id}-failed-report.json")
        |> File.read!()
        |> Jason.decode!()

      assert failed_report["status"] == "package_consume_failed"
      assert failed_report["failed_stage"] == "package_consume"
      assert failed_report["package_cursor"] == "2026-06-03 05:01:00|2"
      assert failed_report["last_acked_cursor"] == @initial_cursor
      assert failed_report["cursor_checkpointing"] == "durable_remote_ack_per_package"
      assert failed_report["unconsumed_range_retained"] == true
      assert get_in(failed_report, ["error", "exit_status"]) == 17
      assert get_in(failed_report, ["error", "message"]) =~ diagnostic_line
    after
      File.rm_rf!(tmp)
    end
  end

  defp failure_cases do
    usage = %{"prompt_tokens" => 2, "completion_tokens" => 3, "total_tokens" => 5}

    [
      %{
        name: "template",
        mode: :template_failure,
        stage: "template_preflight",
        error_class: "template_error"
      },
      %{
        name: "tokenizer",
        mode: :tokenizer_failure,
        stage: "tokenizer_preflight",
        error_class: "tokenizer_error"
      },
      %{
        name: "prompt-bound",
        mode: :prompt_bound,
        stage: "prompt_preflight",
        error_class: "prompt_bound_exceeded"
      },
      %{
        name: "chat-transport",
        mode: :chat_transport,
        stage: "chat_transport",
        error_class: "transport_error"
      },
      %{
        name: "outer-decode",
        response: "not json",
        stage: "outer_response_decode",
        error_class: "outer_decode_error"
      },
      %{
        name: "missing-id",
        response: completion(%{"model" => model(), "choices" => [choice()]}, usage),
        stage: "completion_response",
        error_class: "missing_id"
      },
      %{
        name: "missing-model",
        response: completion(%{"id" => "chatcmpl-test", "choices" => [choice()]}, usage),
        stage: "completion_response",
        error_class: "missing_model"
      },
      %{
        name: "missing-choice",
        response:
          completion(%{"id" => "chatcmpl-test", "model" => model(), "choices" => []}, usage),
        stage: "completion_response",
        error_class: "missing_choice"
      },
      %{
        name: "missing-content",
        response:
          completion(
            %{
              "id" => "chatcmpl-test",
              "model" => model(),
              "choices" => [%{"finish_reason" => "stop", "message" => %{}}]
            },
            usage
          ),
        stage: "completion_response",
        error_class: "missing_content"
      },
      %{
        name: "missing-finish",
        response:
          completion(
            %{
              "id" => "chatcmpl-test",
              "model" => model(),
              "choices" => [%{"message" => %{"content" => "{}"}}]
            },
            usage
          ),
        stage: "completion_response",
        error_class: "missing_finish_reason"
      },
      %{
        name: "non-stop",
        response:
          completion(
            %{"id" => "chatcmpl-test", "model" => model(), "choices" => [choice("length")]},
            usage
          ),
        stage: "completion_response",
        error_class: "non_stop_finish"
      },
      %{
        name: "malformed-inner",
        response:
          completion(
            %{"id" => "chatcmpl-test", "model" => model(), "choices" => [choice("stop", "{")]},
            usage
          ),
        stage: "inner_response_decode",
        error_class: "malformed_inner_json"
      }
    ]
  end

  defp state_with_input do
    input =
      ChangesetStore.insert!(Input, %{
        tenant_id: @tenant,
        source_type: "news_article",
        external_id: "failure-matrix-input",
        observed_at: ~U[2026-07-10 21:23:42.501095Z],
        title: "Failure matrix input",
        body_text: "Failure matrix input body.",
        object_uri: "https://news.google.test/failure-matrix-input",
        content_sha256: sha256("Failure matrix input body."),
        acl: %{"privacy" => "public", "participants" => []},
        normalized: %{},
        facts: %{},
        background: %{},
        questions: %{},
        colors: [],
        topic_tokens: []
      })

    State.new(tenant_id: @tenant) |> State.append(:inputs, input)
  end

  defp admission(input) do
    %{
      source_ref: Admission.input_ref(input),
      source_type: input.source_type,
      external_id: input.external_id,
      observed_at: input.observed_at,
      content_sha256: input.content_sha256,
      content_span_refs: ["span:failure-matrix"],
      evidence_refs: ["evidence:failure-matrix"],
      source_provenance: %{"source_name" => "Failure Matrix"}
    }
  end

  defp with_fake_gibson(failure, fun) do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-live-gibson-failure-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    response_path = Path.join(tmp, "chat-response")
    mode_path = Path.join(tmp, "mode")
    File.write!(response_path, Map.get(failure, :response, valid_completion()))
    File.write!(mode_path, failure |> Map.get(:mode, :response) |> Atom.to_string())
    curl = Path.join(tmp, "curl")

    File.write!(curl, """
    #!/usr/bin/env bash
    url=""
    for arg in "$@"; do
      case "$arg" in
        http://*) url="$arg" ;;
      esac
    done
    mode=$(cat "#{mode_path}")
    case "$url" in
      */apply-template)
        if [ "$mode" = "template_failure" ]; then exit 7; fi
        printf '%s\n' '{"prompt":"bounded exact prompt"}'
        ;;
      */tokenize)
        if [ "$mode" = "tokenizer_failure" ]; then printf 'not json'; exit 0; fi
        if [ "$mode" = "prompt_bound" ]; then
          jq -nc '{tokens: [range(0; 130433)]}'
        else
          printf '%s\n' '{"tokens":[1,2]}'
        fi
        ;;
      */v1/chat/completions)
        if [ "$mode" = "chat_transport" ]; then exit 7; fi
        cat "#{response_path}"
        ;;
      *) exit 99 ;;
    esac
    """)

    File.chmod!(curl, 0o755)
    old_path = System.get_env("PATH") || ""
    System.put_env("PATH", tmp <> ":" <> old_path)

    try do
      fun.()
    after
      System.put_env("PATH", old_path)
      File.rm_rf!(tmp)
    end
  end

  defp create_source_db!(db_path) do
    sql = """
    CREATE TABLE daemon_event (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ingress_source_id INTEGER NOT NULL,
      message_id TEXT NOT NULL,
      message_timestamp TEXT NOT NULL,
      inbound_event TEXT NOT NULL,
      author_id TEXT NOT NULL,
      author_name TEXT NOT NULL,
      text TEXT NOT NULL,
      sender_embeddings_json TEXT,
      attention_space_id TEXT,
      attention_fallback INTEGER NOT NULL,
      payload_json TEXT,
      raw_body TEXT,
      accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      attention_disposition TEXT NOT NULL DEFAULT 'deliver',
      attention_delivery_mode TEXT NOT NULL DEFAULT 'unknown_legacy'
    );
    INSERT INTO daemon_event
      (id, ingress_source_id, message_id, message_timestamp, inbound_event, author_id, author_name, text, attention_fallback, accepted_at)
    VALUES
      (1, 1, 'failure-matrix-1', '2026-06-03T05:00:00Z', 'new_message', 'sender', 'argus-racter-publisher', '{"body":{"title":"first"}}', 0, '2026-06-03 05:00:00'),
      (2, 1, 'failure-matrix-2', '2026-06-03T05:01:00Z', 'new_message', 'sender', 'argus-racter-publisher', '{"body":{"title":"second"}}', 0, '2026-06-03 05:01:00');
    """

    {_output, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp write_failing_ssh!(bin_dir, state_dir) do
    File.mkdir_p!(bin_dir)
    stub = Path.join(bin_dir, "ssh")

    File.write!(stub, """
    #!/usr/bin/env bash
    set -uo pipefail
    cmd="${@: -1}"
    if [[ "$cmd" == *"tar -C"* && "$cmd" == *"-xf -"* ]]; then
      cat > /dev/null
      exit 0
    fi
    if [[ "$cmd" == *consume_event_package.sh* ]]; then
      touch "#{state_dir}/consume-attempted"
      cat "#{state_dir}/fail-message" >&2
      exit 17
    fi
    exit 0
    """)

    File.chmod!(stub, 0o755)
  end

  defp completion(fields, usage), do: fields |> Map.put("usage", usage) |> Jason.encode!()

  defp valid_completion do
    completion(
      %{"id" => "chatcmpl-test", "model" => model(), "choices" => [choice()]},
      %{"prompt_tokens" => 2, "completion_tokens" => 3, "total_tokens" => 5}
    )
  end

  defp choice(finish_reason \\ "stop", content \\ "{}") do
    %{"finish_reason" => finish_reason, "message" => %{"content" => content}}
  end

  defp model, do: "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf"

  defp sha256(content) do
    :sha256 |> :crypto.hash(content) |> Base.encode16(case: :lower)
  end
end
