defmodule Primeradiant.StorageHarness.UvealResearchSoupIngest do
  @moduledoc false

  alias Primeradiant.StorageHarness.{DurableSoupDb, RealIngestion}

  @source_mode "manual_real_ingest_v1"
  @category "cancer"

  def ingest_meta(meta_path, opts \\ []) do
    tenant_id = Keyword.fetch!(opts, :tenant_id)
    actor_id = Keyword.get(opts, :actor_id, "flynn")
    soup_db_path = Keyword.get(opts, :soup_db_path, default_soup_db_path())

    with {:ok, meta} <- read_meta(meta_path),
         {:ok, items} <- meta_to_items(meta, meta_path, tenant_id),
         {:ok, state, ingestion_report} <- RealIngestion.ingest_items(items, actor_id) do
      source_summary = source_summary(meta, meta_path, items)

      DurableSoupDb.persist!(soup_db_path, state, %{
        source_kind: "uveal_research_morning_meta",
        source_db_path: meta_path,
        source_row_count: length(items)
      })

      {:ok, state,
       DurableSoupDb.source_admission_report(
         soup_db_path,
         tenant_id,
         source_summary,
         ingestion_report
       )}
    end
  end

  def default_soup_db_path do
    System.get_env("PRIMERADIANT_SOUP_DB") ||
      Path.join(System.tmp_dir!(), "primeradiant-uveal-research-soup.sqlite3")
  end

  def meta_to_items(meta, meta_path, tenant_id) when is_map(meta) do
    items =
      meta
      |> Map.get("items", [])
      |> Enum.with_index()
      |> Enum.filter(fn {item, _index} -> item["category"] == @category end)
      |> Enum.map(fn {item, index} ->
        item_to_source_item(meta, meta_path, tenant_id, item, index)
      end)

    if items == [] do
      {:error, {:no_uveal_research_items, meta_path}}
    else
      {:ok, items}
    end
  end

  defp read_meta(meta_path) do
    case File.read(meta_path) do
      {:ok, body} -> Jason.decode(body)
      {:error, reason} -> {:error, {:meta_not_readable, meta_path, reason}}
    end
  end

  defp item_to_source_item(meta, meta_path, tenant_id, item, index) do
    date = required_string(meta, "date")
    headline = required_string(item, "headline")
    summary = required_string(item, "summary")
    sources = Map.get(item, "sources", [])
    external_id = "uveal-research:#{date}:#{hash(headline)}"
    text = Enum.reject([headline, summary, sources_text(sources)], &blank?/1) |> Enum.join("\n\n")

    %{
      tenant_id: tenant_id,
      ingestion_run_key: "uveal-research-morning-meta-v1",
      source_type: "news_article",
      source_mode: @source_mode,
      external_id: external_id,
      observed_at: observed_at(meta),
      retrieved_at: observed_at(meta),
      occurred_at: nil,
      canonical_uri: Map.get(meta, "url"),
      raw_object_uri: meta_path,
      source_name: "vr-ai-news-morning",
      source_actor: %{
        kind: "gibson_morning_research",
        name: "vr-ai-news-morning",
        stable_id: "vr-ai-news-morning:cancer"
      },
      title: headline,
      body_text: text,
      extracted_text: text,
      metadata: %{
        uveal_research_soup_ingest: true,
        source_artifact: Path.basename(meta_path),
        source_category: @category,
        source_item_index: index,
        sources: sources
      },
      acl: %{"privacy" => "public"},
      content_sha256: nil
    }
  end

  defp source_summary(meta, meta_path, items) do
    %{
      kind: "uveal_research_morning_meta",
      mode: "read_only",
      meta_path: meta_path,
      date: Map.get(meta, "date"),
      title: Map.get(meta, "title"),
      source_row_count: length(items),
      source_category: @category
    }
  end

  defp observed_at(meta) do
    case Map.get(meta, "date") do
      date when is_binary(date) and date != "" -> "#{date}T10:00:00Z"
      _ -> raise ArgumentError, "uveal research meta requires date"
    end
  end

  defp sources_text([]), do: nil

  defp sources_text(sources) when is_list(sources) do
    "Sources: " <> Enum.join(Enum.map(sources, &to_string/1), ", ")
  end

  defp required_string(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "uveal research meta item requires #{key}"
    end
  end

  defp hash(value) do
    :sha256
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower)
    |> binary_part(0, 12)
  end

  defp blank?(value), do: value in [nil, ""]
end
