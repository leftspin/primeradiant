defmodule Primeradiant.StorageHarness.DaemonNewsEvent do
  @moduledoc false

  alias Primeradiant.StorageHarness.{
    DaemonNewsAdapter,
    DurableSoupDb,
    LiveStoryAgentLoop,
    RealIngestion
  }

  @source_mode "daemon_news_event_r1"

  def default_soup_db_path do
    System.get_env("PRIMERADIANT_SOUP_DB") ||
      Path.join(System.tmp_dir!(), "primeradiant-daemon-news-event-soup.sqlite3")
  end

  def consume_event(event, opts) when is_map(event) do
    raw_root = Keyword.get(opts, :raw_root)
    tenant_id = Keyword.fetch!(opts, :tenant_id)
    actor_id = Keyword.get(opts, :actor_id, "flynn")
    soup_db_path = Keyword.get(opts, :soup_db_path, default_soup_db_path())

    story_agent_loop? = Keyword.get(opts, :story_agent_loop?, false)
    story_agent_opts = Keyword.get(opts, :story_agent_opts, [])

    with {:ok, item, summary} <- event_to_item(event, tenant_id, raw_root),
         {:ok, state, ingestion_report} <- RealIngestion.ingest_items([item], actor_id) do
      admissions = Map.get(ingestion_report, :admissions, [])

      {state, report} =
        if story_agent_loop? and admissions != [] do
          {state, story_agent_report} =
            LiveStoryAgentLoop.run(
              state,
              admissions,
              actor_id,
              story_agent_opts
            )

          {state, story_agent_report(state, summary, ingestion_report, story_agent_report)}
        else
          {state, source_admission_report(state, summary, ingestion_report)}
        end

      DurableSoupDb.persist!(soup_db_path, state, %{
        source_kind: DaemonNewsAdapter.source_adapter(),
        source_db_path: "event:#{summary.event_id}",
        source_row_count: 1
      })

      report =
        report.(soup_db_path, tenant_id)
        |> Map.put(:event_driven_r1, %{
          event_type: DaemonNewsAdapter.event_type(),
          adapter: DaemonNewsAdapter.source_adapter(),
          production_source_event_emitter_present: false,
          persistent_service_installed: false
        })

      {:ok, state, report}
    end
  end

  defp source_admission_report(_state, summary, ingestion_report) do
    fn soup_db_path, tenant_id ->
      DurableSoupDb.source_admission_report(soup_db_path, tenant_id, summary, ingestion_report)
    end
  end

  defp story_agent_report(_state, summary, ingestion_report, story_agent_report) do
    fn soup_db_path, tenant_id ->
      DurableSoupDb.changed_stories_report(
        soup_db_path,
        tenant_id,
        summary,
        Map.merge(ingestion_report, story_agent_report)
      )
      |> Map.merge(%{
        story_meaning_proof: story_agent_report.story_meaning_proof,
        substrate_proof_only: story_agent_report.substrate_proof_only,
        live_story_agent_loop: story_agent_report
      })
    end
  end

  def event_to_item(event, tenant_id, raw_root \\ nil) when is_map(event) do
    with {:ok, envelope, summary} <-
           DaemonNewsAdapter.resolve_source_ref(event, %{"tenant_id" => tenant_id}, raw_root) do
      body = Map.get(envelope, "body") || %{}
      provenance = Map.get(envelope, "provenance") || %{}
      source = event["source"] || %{}
      ref = event["raw_ref"] || %{}
      title = string_field(body, "title") || "Daemon news #{source["item_id"]}"
      summary_text = string_field(body, "summary") || string_field(body, "description")
      text = Enum.reject([title, summary_text], &blank?/1) |> Enum.join("\n\n")

      if blank?(text) do
        {:error, {:unreadable_source_item, source["item_id"]}}
      else
        item = %{
          tenant_id: tenant_id,
          ingestion_run_key: "daemon-news-event-r1",
          source_type: "news_article",
          source_mode: @source_mode,
          external_id: source["item_id"],
          observed_at: source["created_at"] || event["emitted_at"],
          retrieved_at: source["committed_at"] || event["emitted_at"],
          occurred_at: string_field(body, "published_at"),
          canonical_uri: string_field(body, "url") || string_field(body, "canonical_url"),
          raw_object_uri: raw_uri(ref),
          source_name:
            string_field(body, "source_name") || string_field(provenance, "source_name"),
          source_actor: %{
            kind: "daemon_news_source",
            name: string_field(body, "source_name") || string_field(provenance, "source_name"),
            stable_id: source["source_space"] || source["sender_id"]
          },
          title: title,
          body_text: text,
          extracted_text: text,
          metadata: %{
            event_driven_r1: true,
            event_id: event["event_id"],
            source_adapter: DaemonNewsAdapter.source_adapter(),
            source_cursor: event["cursor"],
            source_db_message_id: source["item_id"],
            source_space: source["source_space"],
            receptor_id: source["receptor_id"],
            sender_id: source["sender_id"],
            raw_sha256: ref["sha256"],
            provenance: provenance,
            dedupe: Map.get(envelope, "dedupe") || %{}
          },
          acl: event["acl"],
          content_sha256: nil
        }

        {:ok, item, summary}
      end
    end
  end

  defp raw_uri(ref) do
    "#{ref["path"]}#offset=#{ref["offset"] || 0}&length=#{ref["length"]}"
  end

  defp string_field(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp blank?(value), do: value in [nil, ""]
end
