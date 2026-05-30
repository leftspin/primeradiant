defmodule Primeradiant.StorageHarness.DaemonNewsAdapter do
  @moduledoc false

  @event_type "primeradiant.source.committed_item.v1"
  @message_type "swarm.channel.news.report.v0"
  @source_adapter "daemon-news"

  def event_type, do: @event_type
  def source_adapter, do: @source_adapter

  def resolve_source_ref(event, scope, raw_root \\ nil) when is_map(event) and is_map(scope) do
    with :ok <- validate_event(event, scope),
         {:ok, envelope, raw_bytes} <- read_raw_envelope(event, raw_root),
         :ok <- verify_raw_digest(event, raw_bytes) do
      {:ok, envelope, source_summary(event)}
    end
  end

  defp source_summary(event) do
    %{
      kind: @source_adapter,
      mode: "event_envelope",
      event_id: event["event_id"],
      cursor: event["cursor"],
      message_type: get_in(event, ["source", "message_type"]),
      source_item_id: get_in(event, ["source", "item_id"]),
      raw_ref: event["raw_ref"],
      acl: event["acl"]
    }
  end

  defp validate_event(event, scope) do
    cond do
      event["event_type"] != @event_type ->
        {:error, {:unsupported_event_type, event["event_type"]}}

      get_in(event, ["source", "adapter"]) != @source_adapter ->
        {:error, {:unsupported_source_adapter, get_in(event, ["source", "adapter"])}}

      get_in(event, ["source", "message_type"]) != @message_type ->
        {:error, {:unsupported_message_type, get_in(event, ["source", "message_type"])}}

      get_in(event, ["source", "tenant_id"]) != scope["tenant_id"] ->
        {:error, :source_tenant_scope_mismatch}

      blank?(event["event_id"]) or blank?(event["cursor"]) or blank?(event["emitted_at"]) ->
        {:error, :missing_event_identity}

      blank?(get_in(event, ["source", "item_id"])) ->
        {:error, :missing_source_item_id}

      blank?(get_in(event, ["raw_ref", "path"])) or blank?(get_in(event, ["raw_ref", "sha256"])) ->
        {:error, :missing_raw_reference}

      get_in(event, ["acl", "privacy"]) not in ["public", "private"] ->
        {:error, :missing_acl}

      true ->
        :ok
    end
  end

  defp read_raw_envelope(event, raw_root) do
    ref = event["raw_ref"]
    path = resolve_raw_path(ref["path"], raw_root)
    offset = int!(ref["offset"] || 0)
    length = int!(ref["length"])

    with {:ok, file} <- File.open(path, [:read, :binary]) do
      try do
        case :file.pread(file, offset, length) do
          {:ok, bytes} ->
            with {:ok, envelope} <- Jason.decode(bytes), do: {:ok, envelope, bytes}

          :eof ->
            {:error, {:raw_archive_short_read, path, get_in(event, ["source", "item_id"])}}

          {:error, reason} ->
            {:error, {:raw_archive_read_failed, path, reason}}
        end
      after
        File.close(file)
      end
    else
      {:error, reason} -> {:error, {:raw_archive_not_readable, path, reason}}
    end
  end

  defp verify_raw_digest(event, raw_bytes) do
    expected = get_in(event, ["raw_ref", "sha256"])
    actual = :crypto.hash(:sha256, raw_bytes) |> Base.encode16(case: :lower)

    if expected == actual do
      :ok
    else
      {:error, {:raw_digest_mismatch, get_in(event, ["source", "item_id"])}}
    end
  end

  defp resolve_raw_path(path, nil), do: path

  defp resolve_raw_path(path, raw_root) do
    if Path.type(path) == :absolute, do: path, else: Path.join(raw_root, path)
  end

  defp int!(value) when is_integer(value), do: value
  defp int!(value) when is_binary(value), do: String.to_integer(value)

  defp blank?(value), do: value in [nil, ""]
end
