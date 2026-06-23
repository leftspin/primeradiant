defmodule Primeradiant.Agentic.LiveGibson do
  @moduledoc false

  @endpoint "http://gibson:8080/v1/chat/completions"
  @model "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf"

  def invoke(role, config, packet) do
    prompt = prompt(config, packet)
    started = monotonic_ms()

    request_body =
      Jason.encode!(%{
        model: model(),
        messages: [
          %{role: "system", content: config.system_prompt},
          %{role: "user", content: prompt}
        ],
        temperature: 0.1,
        max_tokens: Map.get(config, :max_tokens, 640)
      })

    request_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-gibson-#{System.unique_integer([:positive])}.json"
      )

    File.write!(request_path, request_body)

    {body, 0} =
      try do
        System.cmd(
          "curl",
          [
            "-sS",
            "--max-time",
            "120",
            endpoint(),
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            "@#{request_path}"
          ],
          stderr_to_stdout: true
        )
      after
        File.rm(request_path)
      end

    elapsed = monotonic_ms() - started
    decoded = Jason.decode!(body)
    model = required_string(decoded, "model")
    response_id = required_string(decoded, "id")
    content = get_in(decoded, ["choices", Access.at(0), "message", "content"])
    output = content |> json_object!() |> normalize_keys()

    unless String.contains?(String.downcase(model), "qwen") do
      raise "live Gibson response did not identify a Qwen model"
    end

    %{
      role: role,
      output: output,
      raw_model_content: content,
      response_id: response_id,
      model: model,
      model_family: "qwen",
      runtime_target: :gibson_llama_server,
      model_route: endpoint(),
      producer_kind: :live_model_inference,
      producer_id: "gibson:qwen3.6:llama-server",
      decision_source: :live_gibson_qwen_inference,
      deterministic_product_logic: false,
      prompt_body: prompt,
      prompt_body_hash: hash(prompt),
      system_prompt_hash: hash(config.system_prompt),
      agent_output_hash: hash(%{content: content, response_id: response_id, role: role}),
      duration_ms: elapsed
    }
  end

  defp prompt(config, packet) do
    Jason.encode!(%{
      instruction: config.task_prompt,
      output_schema: config.output_schema,
      bounded_soup_packet: json_safe(packet)
    })
  end

  defp endpoint, do: @endpoint
  defp model, do: @model

  defp required_string(map, key) do
    case map[key] do
      value when is_binary(value) and value != "" -> value
      _ -> raise "live Gibson response omitted #{key}"
    end
  end

  defp json_object!(content) when is_binary(content) do
    content
    |> String.replace(~r/<think>.*?<\/think>/s, "")
    |> then(fn stripped ->
      case Regex.run(~r/\{.*\}/s, stripped) do
        [json] -> Jason.decode!(json)
        nil -> raise "live Gibson output did not contain a JSON object"
      end
    end)
  end

  defp normalize_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {String.to_atom(to_string(key)), normalize_keys(value)} end)
  end

  defp normalize_keys(list) when is_list(list), do: Enum.map(list, &normalize_keys/1)
  defp normalize_keys(value), do: value

  defp json_safe(%MapSet{} = set), do: set |> MapSet.to_list() |> Enum.map(&json_safe/1)

  defp json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(value), do: value

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  def hash(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(value))
    |> Base.encode16(case: :lower)
  end
end
