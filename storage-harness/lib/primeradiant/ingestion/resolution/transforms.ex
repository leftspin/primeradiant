defmodule Primeradiant.Ingestion.Resolution.Transforms do
  @moduledoc false

  def apply(value, "copy") when is_binary(value), do: {:ok, value}
  def apply(value, "trim") when is_binary(value), do: {:ok, String.trim(value)}

  def apply(value, "lowercase") when is_binary(value),
    do: {:ok, value |> String.trim() |> String.downcase()}

  def apply(value, "uri_host") when is_binary(value) do
    case URI.parse(value) do
      %URI{host: host} when is_binary(host) -> {:ok, String.downcase(host)}
      _ -> :error
    end
  end

  def apply(value, "registrable_domain") when is_binary(value),
    do: {:ok, registrable_domain(value)}

  def apply(value, "normalize_url") when is_binary(value) do
    trimmed = String.trim(value)

    case URI.parse(trimmed) do
      %URI{host: host} = uri when is_binary(host) ->
        {:ok, uri |> Map.put(:host, String.downcase(host)) |> URI.to_string()}

      _ ->
        :error
    end
  end

  def apply(_, _), do: :error

  def registrable_domain(url_or_host) when is_binary(url_or_host) do
    host =
      case URI.parse(url_or_host) do
        %URI{host: host} when is_binary(host) -> host
        _ -> url_or_host
      end

    host
    |> String.trim()
    |> String.downcase()
    |> String.trim_trailing(".")
    |> then(fn
      "www." <> rest -> rest
      other -> other
    end)
  end
end
