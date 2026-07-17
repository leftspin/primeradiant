defmodule Primeradiant.Agentic.LiveGibson do
  @moduledoc false

  @base_endpoint "http://gibson:8080"
  @endpoint "#{@base_endpoint}/v1/chat/completions"
  @apply_template_endpoint "#{@base_endpoint}/apply-template"
  @tokenize_endpoint "#{@base_endpoint}/tokenize"
  @model "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf"
  @prompt_byte_limit 180_000
  @prompt_token_limit 130_432
  @bounded_roles [:story_identity, :meaning_update]

  def invoke(role, config, packet) do
    case preflight(role, config, packet) do
      {:ok, result} -> invoke_preflighted(role, config, packet, result)
      {:error, diagnostic} -> raise_terminal(diagnostic)
    end
  end

  def invoke_preflighted(role, config, packet, preflight) do
    preflight = validate_preflight!(role, config, packet, preflight)

    started = monotonic_ms()

    request_body =
      Jason.encode!(%{
        model: model(),
        messages: messages(config, preflight.prompt_body),
        temperature: 0.1,
        max_tokens: max_tokens(role, config)
      })

    case curl_json(endpoint(), request_body) do
      {:ok, body} ->
        elapsed = monotonic_ms() - started
        decode_completion!(role, config, packet, preflight, body, elapsed)

      {:error, _body} ->
        raise_terminal(diagnostic("chat_transport", "transport_error", preflight))
    end
  end

  def preflight(role, config, packet) do
    prompt_body = prompt(config, packet)

    base = %{
      role: role,
      prompt_body: prompt_body,
      prompt_bytes: byte_size(prompt_body),
      preflight_prompt_tokens: nil
    }

    if role in @bounded_roles do
      with {:ok, templated_prompt} <- apply_template(config, prompt_body, base),
           {:ok, prompt_tokens} <- tokenize(templated_prompt, base),
           result = %{base | preflight_prompt_tokens: prompt_tokens},
           :ok <- within_bounds(result) do
        {:ok, result}
      else
        {:error, diagnostic} -> {:error, diagnostic}
      end
    else
      if role == :story_synthesis do
        case within_byte_bound(base) do
          :ok -> {:ok, base}
          {:error, diagnostic} -> {:error, diagnostic}
        end
      else
        {:ok, base}
      end
    end
  end

  defp validate_preflight!(role, config, packet, preflight) when is_map(preflight) do
    prompt_body = prompt(config, packet)

    valid =
      preflight[:role] == role and preflight[:prompt_body] == prompt_body and
        preflight[:prompt_bytes] == byte_size(prompt_body) and
        valid_preflight_bound?(role, preflight)

    if valid do
      preflight
    else
      diagnostic_base = %{
        role: role,
        prompt_body: prompt_body,
        prompt_bytes: byte_size(prompt_body),
        preflight_prompt_tokens: Map.get(preflight, :preflight_prompt_tokens)
      }

      raise_terminal(diagnostic("prompt_preflight", "invalid_preflight", diagnostic_base))
    end
  end

  defp validate_preflight!(role, config, packet, _preflight) do
    prompt_body = prompt(config, packet)

    raise_terminal(
      diagnostic("prompt_preflight", "invalid_preflight", %{
        role: role,
        prompt_body: prompt_body,
        prompt_bytes: byte_size(prompt_body),
        preflight_prompt_tokens: nil
      })
    )
  end

  defp valid_preflight_bound?(role, preflight) when role in @bounded_roles do
    is_integer(preflight[:preflight_prompt_tokens]) and preflight[:preflight_prompt_tokens] >= 0 and
      preflight[:prompt_bytes] <= @prompt_byte_limit and
      preflight[:preflight_prompt_tokens] <= @prompt_token_limit
  end

  defp valid_preflight_bound?(:story_synthesis, preflight) do
    preflight[:preflight_prompt_tokens] == nil and
      preflight[:prompt_bytes] <= @prompt_byte_limit
  end

  defp valid_preflight_bound?(_role, preflight) do
    preflight[:preflight_prompt_tokens] == nil
  end

  defp apply_template(config, prompt_body, preflight) do
    body =
      Jason.encode!(%{
        messages: messages(config, prompt_body),
        add_generation_prompt: true
      })

    with {:ok, response} <- curl_json(@apply_template_endpoint, body),
         {:ok, decoded} <- decode_object(response),
         prompt when is_binary(prompt) <- decoded["prompt"] do
      {:ok, prompt}
    else
      _ -> {:error, diagnostic("template_preflight", "template_error", preflight)}
    end
  end

  defp tokenize(templated_prompt, preflight) do
    body = Jason.encode!(%{content: templated_prompt, add_special: false})

    with {:ok, response} <- curl_json(@tokenize_endpoint, body),
         {:ok, decoded} <- decode_object(response),
         tokens when is_list(tokens) <- decoded["tokens"] do
      {:ok, length(tokens)}
    else
      _ -> {:error, diagnostic("tokenizer_preflight", "tokenizer_error", preflight)}
    end
  end

  defp within_bounds(%{prompt_bytes: bytes, preflight_prompt_tokens: tokens} = preflight) do
    if bytes <= @prompt_byte_limit and tokens <= @prompt_token_limit do
      :ok
    else
      {:error, diagnostic("prompt_preflight", "prompt_bound_exceeded", preflight)}
    end
  end

  defp within_byte_bound(%{prompt_bytes: bytes} = preflight) do
    if bytes <= @prompt_byte_limit do
      :ok
    else
      {:error, diagnostic("prompt_preflight", "prompt_bound_exceeded", preflight)}
    end
  end

  defp decode_completion!(role, config, packet, preflight, body, elapsed) do
    decoded =
      case decode_object(body) do
        {:ok, object} ->
          object

        :error ->
          raise_terminal(diagnostic("outer_response_decode", "outer_decode_error", preflight))
      end

    provider_usage = provider_usage(decoded)
    diagnostic_base = Map.put(preflight, :provider_usage, provider_usage)
    response_id = required_response_string!(decoded, "id", "missing_id", diagnostic_base)

    model =
      required_response_string!(decoded, "model", "missing_model", diagnostic_base, response_id)

    choice =
      case decoded["choices"] do
        [choice | _] when is_map(choice) ->
          choice

        _ ->
          raise_terminal(
            diagnostic("completion_response", "missing_choice", diagnostic_base, response_id)
          )
      end

    content =
      case get_in(choice, ["message", "content"]) do
        value when is_binary(value) ->
          value

        _ ->
          raise_terminal(
            diagnostic("completion_response", "missing_content", diagnostic_base, response_id)
          )
      end

    finish_reason =
      case choice["finish_reason"] do
        value when is_binary(value) ->
          value

        _ ->
          raise_terminal(
            diagnostic(
              "completion_response",
              "missing_finish_reason",
              diagnostic_base,
              response_id,
              nil,
              content
            )
          )
      end

    unless finish_reason == "stop" do
      raise_terminal(
        diagnostic(
          "completion_response",
          "non_stop_finish",
          diagnostic_base,
          response_id,
          finish_reason,
          content
        )
      )
    end

    output =
      case json_object(content) do
        {:ok, object} ->
          normalize_keys(object)

        :error ->
          raise_terminal(
            diagnostic(
              "inner_response_decode",
              "malformed_inner_json",
              diagnostic_base,
              response_id,
              finish_reason,
              content
            )
          )
      end

    unless String.contains?(String.downcase(model), "qwen") do
      raise "live Gibson response did not identify a Qwen model"
    end

    %{
      role: role,
      output: output,
      model_packet: packet,
      raw_model_content: content,
      response_id: response_id,
      finish_reason: finish_reason,
      provider_usage: provider_usage,
      preflight_prompt_tokens: preflight.preflight_prompt_tokens,
      prompt_bytes: preflight.prompt_bytes,
      content_bytes: byte_size(content),
      content_sha256: binary_sha256(content),
      model: model,
      model_family: "qwen",
      runtime_target: :gibson_llama_server,
      model_route: endpoint(),
      producer_kind: :live_model_inference,
      producer_id: "gibson:qwen3.6:llama-server",
      decision_source: :live_gibson_qwen_inference,
      deterministic_product_logic: false,
      prompt_body: preflight.prompt_body,
      prompt_body_hash: hash(preflight.prompt_body),
      system_prompt_hash: hash(config.system_prompt),
      agent_output_hash: hash(%{content: content, response_id: response_id, role: role}),
      duration_ms: elapsed
    }
  end

  defp required_response_string!(decoded, key, error_class, preflight, response_id \\ nil) do
    case decoded[key] do
      value when is_binary(value) and value != "" ->
        value

      _ ->
        raise_terminal(diagnostic("completion_response", error_class, preflight, response_id))
    end
  end

  defp provider_usage(%{"usage" => usage}) when is_map(usage) do
    %{
      prompt_tokens: Map.get(usage, "prompt_tokens"),
      completion_tokens: Map.get(usage, "completion_tokens"),
      total_tokens: Map.get(usage, "total_tokens")
    }
  end

  defp provider_usage(_decoded), do: nil

  defp diagnostic(
         stage,
         error_class,
         preflight,
         response_id \\ nil,
         finish_reason \\ nil,
         content \\ nil
       ) do
    %{
      stage: stage,
      response_id: response_id,
      finish_reason: finish_reason,
      provider_usage: Map.get(preflight, :provider_usage),
      preflight_prompt_tokens: preflight.preflight_prompt_tokens,
      prompt_bytes: preflight.prompt_bytes,
      content_bytes: content && byte_size(content),
      content_sha256: content && binary_sha256(content),
      error_class: error_class
    }
  end

  defp raise_terminal(diagnostic), do: raise(Jason.encode!(diagnostic))

  defp prompt(config, packet) do
    Jason.encode!(%{
      instruction: config.task_prompt,
      output_schema: config.output_schema,
      bounded_soup_packet: json_safe(packet)
    })
  end

  defp messages(config, prompt_body) do
    [
      %{role: "system", content: config.system_prompt},
      %{role: "user", content: prompt_body}
    ]
  end

  defp curl_json(url, request_body) do
    request_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-gibson-#{System.unique_integer([:positive])}.json"
      )

    File.write!(request_path, request_body)

    try do
      case System.cmd(
             "curl",
             [
               "-sS",
               "--max-time",
               "120",
               url,
               "-H",
               "Content-Type: application/json",
               "--data-binary",
               "@#{request_path}"
             ],
             stderr_to_stdout: true
           ) do
        {body, 0} -> {:ok, body}
        {body, _status} -> {:error, body}
      end
    after
      File.rm(request_path)
    end
  end

  defp decode_object(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      _ -> :error
    end
  end

  defp endpoint, do: @endpoint
  defp model, do: @model

  defp max_tokens(role, _config) when role in @bounded_roles, do: 640
  defp max_tokens(_role, config), do: Map.get(config, :max_tokens, 640)

  defp json_object(content) when is_binary(content) do
    content
    |> String.replace(~r/<think>.*?<\/think>/s, "")
    |> then(fn stripped ->
      case Regex.run(~r/\{.*\}/s, stripped) do
        [json] -> decode_object(json)
        nil -> :error
      end
    end)
  end

  defp normalize_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {String.to_atom(to_string(key)), normalize_keys(value)} end)
  end

  defp normalize_keys(list) when is_list(list), do: Enum.map(list, &normalize_keys/1)
  defp normalize_keys(value), do: value

  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_safe(%MapSet{} = set), do: set |> MapSet.to_list() |> Enum.map(&json_safe/1)

  defp json_safe(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> Map.drop([:__meta__])
    |> json_safe()
  end

  defp json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(value), do: value

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  defp binary_sha256(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower)
  end

  def hash(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(value))
    |> Base.encode16(case: :lower)
  end
end
