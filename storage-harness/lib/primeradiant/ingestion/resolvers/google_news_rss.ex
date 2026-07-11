defmodule Primeradiant.Ingestion.Resolvers.GoogleNewsRss do
  @moduledoc false
  @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

  @id "google_news_rss_v1"
  @redirect_statuses [301, 302, 303, 307, 308]
  @google_paths ["/rss", "/articles/", "/rss/articles/"]

  def id, do: @id

  @impl true
  def resolve(packet, budget) do
    started = System.monotonic_time(:millisecond)

    with {:ok, transport} <- transport(),
         {:ok, article_url} <- evidenced_google_url(packet),
         :ok <- validate_google_url(article_url),
         {:ok, final_url, redirect_consumed} <-
           follow_google(article_url, transport, budget, zero(), started),
         {:ok, response, consumed} <-
           metadata_fetch(final_url, transport, budget, redirect_consumed, started),
         {:ok, canonical, site_name} <- metadata(response, final_url) do
      evidence = evidence(final_url, canonical, site_name)

      case exhausted(consumed, budget, length(evidence), started) do
        nil -> {:ok, evidence, consumed}
        key -> budget_outcome(key)
      end
    else
      {:outcome, _} = outcome -> outcome
      {:error, reason} -> {:outcome, {:retry_scheduled, reason}}
    end
  end

  defp evidenced_google_url(packet) do
    packet
    |> value(:evidence, [])
    |> Enum.find_value(fn item ->
      if value(item, :kind) == "public_url", do: value(item, :value)
    end)
    |> case do
      url when is_binary(url) -> {:ok, url}
      _ -> outcome(:google_news_rss_link_missing)
    end
  end

  defp follow_google(url, transport, budget, consumed, started) do
    with :ok <- check_before_request(consumed, budget, started),
         :ok <- validate_google_url(url),
         :ok <- public_addresses(transport, URI.parse(url).host),
         {:ok, response} <-
           transport.request(:get, url, request_headers(), remaining(budget, consumed, started)),
         {:ok, consumed} <- consume_response(consumed, response, budget, started) do
      if response.status in @redirect_statuses do
        with {:ok, location} <- location(response.headers, url),
             :ok <- consume_redirect(consumed, budget) do
          next = %{consumed | redirects: consumed.redirects + 1}
          uri = URI.parse(location)

          cond do
            uri.scheme != "https" ->
              outcome(:google_news_rss_final_https_required)

            ip_literal?(uri.host) ->
              outcome(:google_news_rss_ip_literal_forbidden)

            uri.host == "news.google.com" ->
              follow_google(location, transport, budget, next, started)

            true ->
              {:ok, location, next}
          end
        end
      else
        outcome(:google_news_rss_publisher_url_not_observed)
      end
    end
  end

  defp metadata_fetch(final_url, transport, budget, consumed, started) do
    with :ok <- validate_public_final(final_url),
         :ok <- check_before_request(consumed, budget, started),
         :ok <- public_addresses(transport, URI.parse(final_url).host),
         {:ok, response} <-
           transport.request(
             :get,
             final_url,
             request_headers(),
             remaining(budget, consumed, started)
           ),
         {:ok, consumed} <- consume_response(consumed, response, budget, started) do
      cond do
        response.status in @redirect_statuses ->
          outcome(:google_news_rss_metadata_redirect_forbidden)

        response.status < 200 or response.status >= 300 ->
          outcome(:google_news_rss_metadata_status)

        not html?(response.headers) ->
          outcome(:google_news_rss_non_html_response)

        true ->
          {:ok, response, consumed}
      end
    end
  end

  defp metadata(response, final_url) do
    with {:ok, canonical} <- canonical_url(response.body, final_url),
         :ok <- same_public_origin(canonical, final_url),
         {:ok, site_name} <- og_site_name(response.body) do
      {:ok, canonical, site_name}
    end
  end

  defp evidence(final_url, canonical, site_name) do
    [
      %{
        kind: "fetched_response_accepted_final_url",
        value: final_url,
        normalized_value: final_url,
        field_name: "public_url",
        confidence: 0.90,
        transform: "copy",
        locator: %{"response_url" => final_url, "selector" => "redirect_location"},
        provenance: %{"authority" => @id},
        transformation_chain: []
      },
      %{
        kind: "fetched_response_canonical_url",
        value: canonical,
        normalized_value: canonical,
        field_name: "public_url",
        confidence: 0.90,
        transform: "copy",
        locator: %{
          "response_url" => final_url,
          "selector" => "link[rel=canonical]",
          "attribute" => "href"
        },
        provenance: %{"authority" => @id},
        transformation_chain: []
      },
      %{
        kind: "fetched_response_og_site_name",
        value: site_name,
        normalized_value: String.trim(site_name),
        field_name: "publisher_label",
        confidence: 1.0,
        transform: "trim",
        locator: %{
          "response_url" => final_url,
          "selector" => "meta[property=og:site_name]",
          "attribute" => "content"
        },
        provenance: %{"authority" => @id},
        transformation_chain: []
      }
    ]
  end

  defp validate_google_url(url) do
    uri = URI.parse(url)

    cond do
      uri.scheme != "https" ->
        outcome(:google_news_rss_https_required)

      not is_nil(uri.userinfo) ->
        outcome(:google_news_rss_credentials_forbidden)

      ip_literal?(uri.host) ->
        outcome(:google_news_rss_ip_literal_forbidden)

      uri.host != "news.google.com" ->
        outcome(:google_news_rss_host_forbidden)

      uri.port not in [nil, 443] ->
        outcome(:google_news_rss_port_forbidden)

      not Enum.any?(@google_paths, &String.starts_with?(uri.path || "", &1)) ->
        outcome(:google_news_rss_path_forbidden)

      true ->
        :ok
    end
  end

  defp validate_public_final(url) do
    uri = URI.parse(url)

    cond do
      uri.scheme != "https" -> outcome(:google_news_rss_final_https_required)
      not is_nil(uri.userinfo) -> outcome(:google_news_rss_credentials_forbidden)
      not is_binary(uri.host) -> outcome(:google_news_rss_final_host_invalid)
      uri.port not in [nil, 443] -> outcome(:google_news_rss_final_port_forbidden)
      ip_literal?(uri.host) -> outcome(:google_news_rss_ip_literal_forbidden)
      uri.host == "news.google.com" -> outcome(:google_news_rss_publisher_url_not_observed)
      true -> :ok
    end
  end

  defp public_addresses(transport, host) do
    case transport.resolve_addresses(host) do
      {:ok, addresses} when addresses != [] ->
        if Enum.all?(addresses, &public_address?/1),
          do: :ok,
          else: outcome(:google_news_rss_address_forbidden)

      {:ok, []} ->
        {:error, :dns_no_addresses}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp public_address?({a, b, c, _d}) do
    not (a in [0, 10, 127] or a >= 224 or (a == 100 and b in 64..127) or
           (a == 169 and b == 254) or (a == 172 and b in 16..31) or
           (a == 192 and b == 168) or (a == 192 and b == 0 and c in [0, 2]) or
           (a == 198 and b in [18, 19]) or (a == 198 and b == 51 and c == 100) or
           (a == 203 and b == 0 and c == 113))
  end

  defp public_address?({a, b, _c, _d, _e, _f, _g, _h}) do
    not (a == 0 or a in 0xFC00..0xFDFF or a in 0xFE80..0xFEBF or a in 0xFF00..0xFFFF or
           (a == 0x2001 and b == 0x0DB8))
  end

  defp public_address?(_), do: false

  defp ip_literal?(host) when is_binary(host),
    do: match?({:ok, _}, :inet.parse_address(String.to_charlist(host)))

  defp ip_literal?(_), do: false

  defp canonical_url(body, final_url) do
    case Regex.run(
           ~r/<link\b[^>]*\brel=["']canonical["'][^>]*\bhref=["']([^"']+)["'][^>]*>/i,
           body,
           capture: :all_but_first
         ) ||
           Regex.run(
             ~r/<link\b[^>]*\bhref=["']([^"']+)["'][^>]*\brel=["']canonical["'][^>]*>/i,
             body,
             capture: :all_but_first
           ) do
      [href] -> {:ok, URI.merge(final_url, href) |> URI.to_string()}
      _ -> outcome(:google_news_rss_canonical_missing)
    end
  end

  defp og_site_name(body) do
    case Regex.run(
           ~r/<meta\b[^>]*\bproperty=["']og:site_name["'][^>]*\bcontent=["']([^"']+)["'][^>]*>/i,
           body,
           capture: :all_but_first
         ) ||
           Regex.run(
             ~r/<meta\b[^>]*\bcontent=["']([^"']+)["'][^>]*\bproperty=["']og:site_name["'][^>]*>/i,
             body,
             capture: :all_but_first
           ) do
      [name] when name != "" -> {:ok, name}
      _ -> outcome(:google_news_rss_site_name_missing)
    end
  end

  defp same_public_origin(canonical, final_url) do
    canonical_uri = URI.parse(canonical)
    final_uri = URI.parse(final_url)

    if canonical_uri.scheme == "https" and same_registrable_domain?(canonical_uri, final_uri) and
         canonical_uri.port in [nil, 443] and not ip_literal?(canonical_uri.host),
       do: :ok,
       else: outcome(:google_news_rss_cross_host_canonical)
  end

  defp html?(headers) do
    headers
    |> header("content-type")
    |> case do
      value when is_binary(value) -> String.starts_with?(String.downcase(value), "text/html")
      _ -> false
    end
  end

  defp location(headers, base) do
    case header(headers, "location") do
      value when is_binary(value) and value != "" ->
        {:ok, URI.merge(base, value) |> URI.to_string()}

      _ ->
        outcome(:google_news_rss_redirect_location_missing)
    end
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(to_string(key)) == name, do: to_string(value)
    end)
  end

  defp consume_response(consumed, response, budget, started) do
    next = %{
      consumed
      | requests: consumed.requests + 1,
        bytes: consumed.bytes + byte_size(response.body)
    }

    case exhausted(next, budget, 0, started) do
      nil -> {:ok, next}
      key -> budget_outcome(key)
    end
  end

  defp consume_redirect(consumed, budget) do
    limit = min(value(budget, :redirects, 3), 3)

    if consumed.redirects + 1 > limit,
      do: outcome(:google_news_rss_redirect_budget_exhausted),
      else: :ok
  end

  defp check_before_request(consumed, budget, started) do
    cond do
      is_number(value(budget, :time_ms)) and remaining_time(budget, started) <= 0 ->
        budget_outcome(:time_ms)

      true ->
        case exhausted(%{consumed | requests: consumed.requests + 1}, budget, 0, started) do
          nil -> :ok
          key -> budget_outcome(key)
        end
    end
  end

  defp exhausted(consumed, budget, result_count, started) do
    elapsed = max(System.monotonic_time(:millisecond) - started, 0)

    cond do
      is_number(value(budget, :requests)) and consumed.requests > value(budget, :requests) ->
        :requests

      is_number(value(budget, :bytes)) and consumed.bytes > value(budget, :bytes) ->
        :bytes

      consumed.redirects > min(value(budget, :redirects, 3), 3) ->
        :redirects

      is_number(value(budget, :result_count)) and result_count > value(budget, :result_count) ->
        :result_count

      is_number(value(budget, :time_ms)) and elapsed > value(budget, :time_ms) ->
        :time_ms

      true ->
        nil
    end
  end

  defp budget_outcome(key), do: outcome(String.to_atom("google_news_rss_#{key}_budget_exhausted"))

  defp remaining(budget, consumed, started),
    do: %{
      time_ms: remaining_time(budget, started),
      bytes: max((value(budget, :bytes) || 0) - consumed.bytes, 0)
    }

  defp remaining_time(budget, started) do
    case value(budget, :time_ms) do
      limit when is_number(limit) ->
        elapsed = max(System.monotonic_time(:millisecond) - started, 0)
        max(limit - elapsed, 0)

      _ ->
        nil
    end
  end

  defp same_registrable_domain?(%URI{host: left}, %URI{host: right})
       when is_binary(left) and is_binary(right) do
    alias Primeradiant.Ingestion.Resolution.Eligibility

    Eligibility.registrable_domain(left) == Eligibility.registrable_domain(right)
  end

  defp same_registrable_domain?(_, _), do: false

  defp transport do
    case Application.fetch_env(
           :primeradiant_storage_harness,
           :google_news_rss_http_transport
         ) do
      {:ok, module} -> {:ok, module}
      :error -> outcome(:google_news_rss_transport_not_configured)
    end
  end

  defp request_headers,
    do: [
      {"accept", "text/html,application/xhtml+xml"},
      {"user-agent", "PrimeRadiantEvidenceResolver/1"}
    ]

  defp zero, do: %{requests: 0, bytes: 0, redirects: 0}
  defp outcome(reason), do: {:outcome, {:refused, reason}}

  defp value(map, key, default \\ nil),
    do: Map.get(map, key) || Map.get(map, to_string(key)) || default
end
