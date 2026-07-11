defmodule Primeradiant.Ingestion.Adapters.GoogleNewsRssFeedAdapter do
  @moduledoc false
  @behaviour Primeradiant.Ingestion.SourceAdapter

  @impl true
  def to_candidate(raw_envelope, _ctx) do
    case Jason.decode(raw_envelope.retained_bytes || "") do
      {:ok, item} when is_map(item) ->
        {:ok,
         %{
           raw_refs: [raw_envelope.id],
           declared_identity: raw_envelope.source_event_external_id,
           declared_cursor: raw_envelope.integrity_metadata["source_position"],
           feed_metadata: %{
             "title" => item["title"],
             "link" => item["link"],
             "publisher_label" => item["source_label"],
             "publisher_domain" => item["source_domain"]
           },
           visibility: raw_envelope.visibility,
           adapter_provenance: %{"adapter" => "rss_item_v1", "version" => "v1"}
         }}

      _ ->
        {:outcome, {:quarantined, :malformed_envelope}}
    end
  end
end
