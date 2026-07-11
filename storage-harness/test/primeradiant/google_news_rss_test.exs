defmodule Primeradiant.GoogleNewsRssTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.Ingestion.Resolution.Case
  alias Primeradiant.Ingestion.Resolvers.GoogleNewsRss
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb}

  @tenant "90000000-0000-0000-0000-000000001656"
  @google_source "google-rss-fixture"
  @publisher_source "publisher-rss-fixture"
  @article "https://news.google.com/rss/articles/item"
  @hop "https://news.google.com/articles/hop"
  @final "https://publisher.example/article"
  @canonical "https://publisher.example/canonical"
  @at ~U[2026-07-11 18:00:00.000000Z]
  @budget %{time_ms: 5_000, requests: 4, bytes: 20_000, redirects: 3, result_count: 3}

  defmodule FixtureTransport do
    @behaviour Primeradiant.Ingestion.Resolvers.HttpTransport

    def request(method, url, headers, limits) do
      Agent.get_and_update(:google_rss_transport_fixture, fn state ->
        capture = %{method: method, url: url, headers: headers, limits: limits}
        response = Map.get(state.responses, url, {:error, :unexpected_request})
        {response, %{state | captures: state.captures ++ [capture]}}
      end)
    end

    def resolve_addresses(host) do
      Agent.get_and_update(:google_rss_transport_fixture, fn state ->
        case Map.get(state.addresses, host, {:ok, [{93, 184, 216, 34}]}) do
          {:sequence, [next | rest]} ->
            {next, put_in(state.addresses[host], {:sequence, rest})}

          response ->
            {response, state}
        end
      end)
    end
  end

  setup do
    start_supervised!(%{
      id: :google_rss_transport_fixture,
      start:
        {Agent, :start_link,
         [fn -> fixture_state(%{}) end, [name: :google_rss_transport_fixture]]}
    })

    prior = Application.get_env(:primeradiant_storage_harness, :google_news_rss_http_transport)

    Application.put_env(
      :primeradiant_storage_harness,
      :google_news_rss_http_transport,
      FixtureTransport
    )

    on_exit(fn ->
      if prior,
        do:
          Application.put_env(
            :primeradiant_storage_harness,
            :google_news_rss_http_transport,
            prior
          ),
        else:
          Application.delete_env(:primeradiant_storage_harness, :google_news_rss_http_transport)
    end)

    root = Path.join(System.tmp_dir!(), "google-rss-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "SER-V6 registered Google resolver and non-Google RSS use the generic pipeline", %{
    db_path: db
  } do
    configure(happy_responses())
    register_google(db, "enabled", %{"api_key" => "ignored", "oauth_token" => "ignored"})

    google =
      receive_item(db, @google_source, 1, "google-item", %{
        "title" => "Item",
        "link" => @article,
        "source_label" => "Feed Label"
      })

    assert {:outcome, "eligible", _} = run_case(db, google)

    google_case = DurableSoupDb.resolution_case(db, @tenant, google.resolution_case_id)

    selected =
      DurableSoupDb.resolved_source_fields_for_case(db, @tenant, google_case.id)
      |> Enum.filter(& &1.selected)

    assert Enum.find(selected, &(&1.field_name == "public_url")).normalized_value == @canonical

    assert Enum.find(selected, &(&1.field_name == "publisher_label")).normalized_value ==
             "Publisher Site"

    assert Decimal.equal?(
             Enum.find(selected, &(&1.field_name == "public_url")).confidence,
             Decimal.new("0.90")
           )

    evidence = DurableSoupDb.resolution_evidence_for_case(db, @tenant, google_case.id)

    assert Enum.any?(
             evidence,
             &(&1.kind == "fetched_response_accepted_final_url" and
                 &1.locator["response_url"] == @final)
           )

    assert Enum.any?(
             evidence,
             &(&1.kind == "fetched_response_canonical_url" and
                 &1.locator["response_url"] == @final)
           )

    assert Enum.any?(
             evidence,
             &(&1.kind == "fetched_response_og_site_name" and &1.locator["response_url"] == @final)
           )

    request_time_limits = Enum.map(captures(), & &1.limits.time_ms)
    assert Enum.all?(request_time_limits, &(&1 <= @budget.time_ms))
    assert request_time_limits == Enum.sort(request_time_limits, :desc)

    register_publisher(db)

    publisher =
      receive_item(db, @publisher_source, 1, "publisher-item", %{
        "title" => "Publisher item",
        "link" => "https://plain.example/article",
        "source_label" => "Plain Publisher",
        "source_domain" => "plain.example"
      })

    assert {:outcome, "eligible", _} = run_case(db, publisher)

    captures = captures()

    refute Enum.any?(captures, fn capture ->
             Enum.any?(capture.headers, fn {key, _} ->
               String.downcase(key) in ["authorization", "cookie", "proxy-authorization"]
             end)
           end)
  end

  test "shadow registration records eligibility without admission", %{db_path: db} do
    configure(happy_responses())
    register_google(db, "shadow")

    receipt =
      receive_item(db, @google_source, 1, "shadow-item", %{
        "title" => "Item",
        "link" => @article,
        "source_label" => "Feed"
      })

    assert {:outcome, "eligible", _} = run_case(db, receipt)

    assert DurableSoupDb.table_count(db, "inputs", @tenant) == 0

    assert DurableSoupDb.resolution_outcomes_for_case(db, @tenant, receipt.resolution_case_id)
           |> Enum.any?(&(&1.outcome_code == "eligible"))
  end

  test "SER-V13 rejects disallowed initial authority shapes without requests" do
    cases = [
      {"http://news.google.com/rss/articles/item", :google_news_rss_https_required},
      {"https://google.com/rss/articles/item", :google_news_rss_host_forbidden},
      {"https://news.google.com/search/item", :google_news_rss_path_forbidden},
      {"https://news.google.com:444/rss/articles/item", :google_news_rss_port_forbidden},
      {"https://127.0.0.1/rss/articles/item", :google_news_rss_ip_literal_forbidden},
      {"https://user:secret@news.google.com/rss/articles/item",
       :google_news_rss_credentials_forbidden}
    ]

    Enum.each(cases, fn {url, reason} ->
      configure(%{})
      assert GoogleNewsRss.resolve(packet(url), @budget) == refused(reason)
      assert captures() == []
    end)
  end

  test "SER-V13 checks private loopback and link-local addresses on Google hops and metadata" do
    forbidden = [
      {10, 0, 0, 1},
      {127, 0, 0, 1},
      {169, 254, 1, 1},
      {0, 0, 0, 0, 0, 0, 0, 1},
      {0xFE80, 0, 0, 0, 0, 0, 0, 1}
    ]

    Enum.each(forbidden, fn address ->
      configure(happy_responses(), %{"news.google.com" => {:ok, [address]}})

      assert GoogleNewsRss.resolve(packet(@article), @budget) ==
               refused(:google_news_rss_address_forbidden)

      configure(happy_responses(), %{"publisher.example" => {:ok, [address]}})

      assert GoogleNewsRss.resolve(packet(@article), @budget) ==
               refused(:google_news_rss_address_forbidden)
    end)

    configure(happy_responses(), %{
      "news.google.com" =>
        {:sequence,
         [
           {:ok, [{8, 8, 8, 8}]},
           {:ok, [{10, 0, 0, 1}]}
         ]}
    })

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_address_forbidden)

    assert Enum.map(captures(), & &1.url) == [@article]
  end

  test "SER-V13 redirect and final URL authority failures are typed" do
    four_hops = %{
      @article => redirect("https://news.google.com/articles/1"),
      "https://news.google.com/articles/1" => redirect("https://news.google.com/articles/2"),
      "https://news.google.com/articles/2" => redirect("https://news.google.com/articles/3"),
      "https://news.google.com/articles/3" => redirect("https://news.google.com/articles/4")
    }

    configure(four_hops)

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_redirect_budget_exhausted)

    configure(%{@article => redirect("http://publisher.example/article")})

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_final_https_required)

    configure(%{
      @article => redirect(@final),
      @final => redirect("https://other.example/article")
    })

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_metadata_redirect_forbidden)

    configure(%{
      @article => redirect("https://cross.example/intermediate"),
      "https://cross.example/intermediate" => redirect(@final)
    })

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_metadata_redirect_forbidden)

    refute Enum.any?(captures(), &(&1.url == @final))
  end

  test "SER-V13 metadata response and canonical authority failures are typed" do
    configure(%{@article => redirect(@final), @final => ok("application/json", "{}")})

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_non_html_response)

    cross =
      "<html><head><link rel=\"canonical\" href=\"https://other.example/canonical\"><meta property=\"og:site_name\" content=\"Publisher\"></head></html>"

    configure(%{@article => redirect(@final), @final => ok("text/html", cross)})

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_cross_host_canonical)

    same_registrable_domain =
      "<html><head><link rel=\"canonical\" href=\"https://www.publisher.example/canonical\"><meta property=\"og:site_name\" content=\"Publisher\"></head></html>"

    configure(%{@article => redirect(@final), @final => ok("text/html", same_registrable_domain)})

    assert {:ok, evidence, _consumed} = GoogleNewsRss.resolve(packet(@article), @budget)

    assert Enum.any?(
             evidence,
             &(&1.kind == "fetched_response_canonical_url" and
                 &1.value == "https://www.publisher.example/canonical")
           )

    configure(%{
      @article => redirect(@final),
      @final => ok("text/html", String.duplicate("x", 101))
    })

    assert GoogleNewsRss.resolve(packet(@article), %{@budget | bytes: 100}) ==
             refused(:google_news_rss_bytes_budget_exhausted)
  end

  test "SER-V13 request byte redirect and time budget exhaustion are typed" do
    configure(happy_responses())

    assert GoogleNewsRss.resolve(packet(@article), %{@budget | requests: 1}) ==
             refused(:google_news_rss_requests_budget_exhausted)

    configure(happy_responses())

    assert GoogleNewsRss.resolve(packet(@article), %{@budget | bytes: 10}) ==
             refused(:google_news_rss_bytes_budget_exhausted)

    configure(happy_responses())

    assert GoogleNewsRss.resolve(packet(@article), %{@budget | redirects: 1}) ==
             refused(:google_news_rss_redirect_budget_exhausted)

    configure(happy_responses())

    assert GoogleNewsRss.resolve(packet(@article), %{@budget | time_ms: -1}) ==
             refused(:google_news_rss_time_ms_budget_exhausted)
  end

  test "real transport is absent until an explicitly reviewed bounded TLS transport is configured" do
    Application.delete_env(:primeradiant_storage_harness, :google_news_rss_http_transport)

    assert GoogleNewsRss.resolve(packet(@article), @budget) ==
             refused(:google_news_rss_transport_not_configured)

    assert captures() == []
  end

  test "SER-R10 common ingestion modules contain no Google branch" do
    root = Path.expand("../../lib/primeradiant/ingestion", __DIR__)

    files =
      Path.wildcard(Path.join(root, "resolution/*.ex")) ++
        [Path.join(root, "source_registry.ex"), Path.join(root, "package_acknowledgement.ex")]

    Enum.each(files, fn file ->
      text = File.read!(file)
      refute String.contains?(String.downcase(text), "google"), file
    end)
  end

  defp register_google(db, mode, extra_config \\ %{}) do
    config =
      Map.merge(
        %{
          "admission_material" => admission_material(),
          "evidence_types" => ["public_url"],
          "resolvers" => [
            %{
              "id" => GoogleNewsRss.id(),
              "module" => GoogleNewsRss,
              "version" => "v1",
              "evidence_types" => ["public_url"]
            }
          ]
        },
        extra_config
      )

    register(db, @google_source, mode, config)
  end

  defp register_publisher(db),
    do:
      register(db, @publisher_source, "enabled", %{
        "admission_material" => admission_material(),
        "resolvers" => []
      })

  defp register(db, source_key, mode, config) do
    {:ok, _} =
      SourceRegistry.register_source(db, %{
        tenant_id: @tenant,
        source_key: source_key,
        adapter_module: Primeradiant.Ingestion.Adapters.GoogleNewsRssFeedAdapter,
        adapter_version: "v1",
        mode: mode,
        resolution_policy: %{"version" => "v1", "source_class" => "public_article"},
        policy_version: "v1",
        budgets: %{
          "max_attempts" => 2,
          "total_case_ms" => 10_000,
          "retry_backoff_ms" => 10,
          "per_source_concurrency" => 1,
          "adapter" => %{"time_ms" => 1_000},
          "normalizer" => %{"time_ms" => 1_000},
          "resolvers" => %{
            GoogleNewsRss.id() => Map.new(@budget, fn {k, v} -> {to_string(k), v} end)
          }
        },
        config: config,
        initial_cursor: 0
      })
  end

  defp receive_item(db, source_key, position, identity, item) do
    bytes = Jason.encode!(item)

    {:ok, receipt} =
      SourceRegistry.receive_envelope(db, %{
        tenant_id: @tenant,
        source_key: source_key,
        source_event_external_id: identity,
        source_position: position,
        received_at: DateTime.add(@at, position, :second),
        content_digest: ChangesetStore.hash(bytes),
        retained_bytes: bytes,
        visibility: "public",
        correlation_id: "correlation-#{identity}"
      })

    receipt
  end

  defp packet(url), do: %{evidence: [%{kind: "public_url", value: url}]}

  defp run_case(db, receipt) do
    inserted_at =
      DurableSoupDb.resolution_case(db, @tenant, receipt.resolution_case_id).inserted_at

    Case.run(db, receipt.resolution_case_id, tenant_id: @tenant, at: inserted_at)
  end

  defp refused(reason), do: {:outcome, {:refused, reason}}
  defp redirect(location), do: {:ok, %{status: 302, headers: [{"location", location}], body: ""}}
  defp ok(type, body), do: {:ok, %{status: 200, headers: [{"content-type", type}], body: body}}

  defp happy_responses do
    html =
      "<html><head><link rel=\"canonical\" href=\"#{@canonical}\"><meta property=\"og:site_name\" content=\"Publisher Site\"></head></html>"

    %{
      @article => redirect(@hop),
      @hop => redirect(@final),
      @final => ok("text/html; charset=utf-8", html)
    }
  end

  defp fixture_state(responses, addresses \\ %{}),
    do: %{responses: responses, addresses: addresses, captures: []}

  defp configure(responses, addresses \\ %{}) do
    Agent.update(:google_rss_transport_fixture, fn _ -> fixture_state(responses, addresses) end)
  end

  defp captures, do: Agent.get(:google_rss_transport_fixture, & &1.captures)

  defp admission_material,
    do: %{
      "source_type" => "news_article",
      "acl" => %{"privacy" => "public", "participants" => []}
    }
end
