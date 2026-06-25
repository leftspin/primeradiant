defmodule Primeradiant.StorageHarness.DaemonNewsSourceIdentity do
  @moduledoc false

  @original_url_keys ~w(original_url original_article_url canonical_url)
  @canonical_url_keys ~w(url canonical_url)
  @original_name_keys ~w(original_source_name original_publication publication)
  @source_name_keys ~w(source_name)

  def source_fields(body, provenance) when is_map(body) and is_map(provenance) do
    aggregator_url = first_string(body, @canonical_url_keys)

    aggregator_name =
      first_string(body, @source_name_keys) || first_string(provenance, @source_name_keys)

    cond do
      google_news_rss_url?(aggregator_url) ->
        google_news_fields(body, provenance, aggregator_url, aggregator_name)

      true ->
        %{
          canonical_uri: aggregator_url,
          source_name: aggregator_name,
          source_actor_name: aggregator_name,
          aggregator: nil
        }
    end
  end

  defp google_news_fields(body, provenance, aggregator_url, aggregator_name) do
    original_url =
      first_string(body, @original_url_keys) || first_string(provenance, @original_url_keys)

    original_name =
      first_string(body, @original_name_keys) || first_string(provenance, @original_name_keys)

    if original_url && not google_news_rss_url?(original_url) do
      original_fields(original_url, original_name, aggregator_url, aggregator_name)
    else
      aggregator_only(aggregator_url, aggregator_name)
    end
  end

  defp original_fields(original_url, source_name, aggregator_url, aggregator_name) do
    %{
      canonical_uri: original_url,
      source_name: source_name,
      source_actor_name: source_name,
      aggregator: aggregator_metadata(aggregator_url, aggregator_name)
    }
  end

  defp aggregator_only(aggregator_url, aggregator_name) do
    %{
      canonical_uri: nil,
      source_name: nil,
      source_actor_name: nil,
      aggregator: aggregator_metadata(aggregator_url, aggregator_name)
    }
  end

  defp aggregator_metadata(nil, nil), do: nil

  defp aggregator_metadata(url, name) do
    %{
      "kind" => "aggregator",
      "name" => name,
      "url" => url,
      "reason" => "google_news_rss_is_not_original_publisher_identity"
    }
  end

  defp google_news_rss_url?(url) when is_binary(url) do
    uri = URI.parse(url)

    uri.scheme in ["http", "https"] and uri.host == "news.google.com" and
      String.starts_with?(uri.path || "", "/rss/articles/")
  end

  defp google_news_rss_url?(_), do: false

  defp first_string(map, keys) do
    Enum.find_value(keys, fn key ->
      case Map.get(map, key) do
        value when is_binary(value) and value != "" -> value
        _ -> nil
      end
    end)
  end
end
