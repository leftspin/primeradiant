defmodule Primeradiant.StorageHarness.DaemonNewsReplay do
  @moduledoc false

  alias Primeradiant.StorageHarness.{DurableSoupDb, RealIngestion}

  @default_db_path "/Volumes/BlipsAndChitz/news-storage/news.db"
  @message_type "swarm.channel.news.report.v0"
  @source_mode "manual_real_ingest_v1"
  @required_columns ~w(message_id message_type created_at received_at raw_archive_path raw_archive_offset raw_archive_length raw_sha256)

  def default_db_path, do: @default_db_path

  def replay(opts \\ []) do
    db_path = Keyword.get(opts, :db_path, @default_db_path)
    raw_root = Keyword.get(opts, :raw_root)
    tenant_id = Keyword.fetch!(opts, :tenant_id)
    actor_id = Keyword.get(opts, :actor_id, "flynn")
    limit = Keyword.get(opts, :limit)
    soup_db_path = Keyword.get(opts, :soup_db_path, default_soup_db_path())

    with :ok <- validate_source_db(db_path),
         {:ok, rows} <- read_rows(db_path, limit),
         {:ok, items} <- rows_to_items(rows, tenant_id, raw_root),
         {:ok, state, ingestion_report} <- RealIngestion.ingest_items(items, actor_id) do
      source_summary = source_summary(db_path, rows)

      DurableSoupDb.persist!(soup_db_path, state, %{
        source_kind: "daemon_news_sqlite",
        source_db_path: db_path,
        source_row_count: length(rows)
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
      Path.join(System.tmp_dir!(), "primeradiant-daemon-news-soup.sqlite3")
  end

  def validate_source_db(db_path) do
    with :ok <- readable_file(db_path),
         {:ok, columns} <- table_columns(db_path, "messages") do
      missing = @required_columns -- columns

      if missing == [] do
        :ok
      else
        {:error, {:unsupported_source_schema, db_path, missing}}
      end
    end
  end

  defp readable_file(path) do
    if File.regular?(path) do
      :ok
    else
      {:error, {:source_db_not_found, path}}
    end
  end

  defp table_columns(db_path, table) do
    case sqlite_readonly(db_path, "PRAGMA table_info(#{table});") do
      {:ok, output} ->
        columns =
          output
          |> String.split("\n", trim: true)
          |> Enum.map(fn line ->
            line
            |> String.split("|")
            |> Enum.at(1)
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, columns}

      error ->
        error
    end
  end

  defp read_rows(db_path, limit) do
    sql = """
    SELECT json_object(
      'message_id', message_id,
      'source_space', source_space,
      'receptor_id', receptor_id,
      'sender_id', sender_id,
      'message_type', message_type,
      'created_at', created_at,
      'received_at', received_at,
      'raw_archive_path', raw_archive_path,
      'raw_archive_offset', raw_archive_offset,
      'raw_archive_length', raw_archive_length,
      'raw_sha256', raw_sha256
    )
    FROM messages
    WHERE message_type = '#{@message_type}'
    ORDER BY COALESCE(created_at, received_at), message_id
    #{limit_clause(limit)};
    """

    with {:ok, output} <- sqlite_readonly(db_path, sql) do
      rows =
        output
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode!/1)

      {:ok, rows}
    end
  end

  defp limit_clause(nil), do: ""
  defp limit_clause(limit) when is_integer(limit) and limit > 0, do: "LIMIT #{limit}"

  defp rows_to_items(rows, tenant_id, raw_root) do
    rows
    |> Enum.map(&row_to_item(&1, tenant_id, raw_root))
    |> collect_results()
  end

  defp row_to_item(row, tenant_id, raw_root) do
    with {:ok, envelope} <- read_raw_envelope(row, raw_root) do
      body = Map.get(envelope, "body") || %{}
      provenance = Map.get(envelope, "provenance") || %{}
      title = string_field(body, "title") || "Daemon news #{row["message_id"]}"
      summary = string_field(body, "summary") || string_field(body, "description")
      text = Enum.reject([title, summary], &blank?/1) |> Enum.join("\n\n")

      if blank?(text) do
        {:error, {:unreadable_source_item, row["message_id"]}}
      else
        {:ok,
         %{
           tenant_id: tenant_id,
           ingestion_run_key: "daemon-news-replay-v1",
           source_type: "news_article",
           source_mode: @source_mode,
           external_id: row["message_id"],
           observed_at: row["created_at"] || row["received_at"],
           retrieved_at: row["received_at"],
           occurred_at: string_field(body, "published_at"),
           canonical_uri: string_field(body, "url") || string_field(body, "canonical_url"),
           raw_object_uri: raw_uri(row),
           source_name:
             string_field(body, "source_name") || string_field(provenance, "source_name"),
           source_actor: %{
             kind: "daemon_news_source",
             name: string_field(body, "source_name") || string_field(provenance, "source_name"),
             stable_id: row["source_space"] || row["sender_id"]
           },
           title: title,
           body_text: text,
           extracted_text: text,
           metadata: %{
             daemon_news_replay: true,
             source_db_message_id: row["message_id"],
             source_space: row["source_space"],
             receptor_id: row["receptor_id"],
             sender_id: row["sender_id"],
             raw_sha256: row["raw_sha256"],
             provenance: provenance,
             dedupe: Map.get(envelope, "dedupe") || %{}
           },
           acl: %{"privacy" => "public"},
           content_sha256: nil
         }}
      end
    end
  end

  defp read_raw_envelope(row, raw_root) do
    path = resolve_raw_path(row["raw_archive_path"], raw_root)
    offset = int!(row["raw_archive_offset"])
    length = int!(row["raw_archive_length"])

    with {:ok, file} <- File.open(path, [:read, :binary]) do
      try do
        case :file.pread(file, offset, length) do
          {:ok, bytes} -> Jason.decode(bytes)
          :eof -> {:error, {:raw_archive_short_read, path, row["message_id"]}}
          {:error, reason} -> {:error, {:raw_archive_read_failed, path, reason}}
        end
      after
        File.close(file)
      end
    else
      {:error, reason} -> {:error, {:raw_archive_not_readable, path, reason}}
    end
  end

  defp resolve_raw_path(path, nil), do: path

  defp resolve_raw_path(path, raw_root) do
    if Path.type(path) == :absolute do
      path
    else
      Path.join(raw_root, path)
    end
  end

  defp raw_uri(row) do
    "#{row["raw_archive_path"]}#offset=#{row["raw_archive_offset"]}&length=#{row["raw_archive_length"]}"
  end

  defp source_summary(db_path, source_rows) do
    %{
      kind: "daemon_news_sqlite",
      db_path: db_path,
      mode: "read_only",
      message_type: @message_type,
      source_row_count: length(source_rows)
    }
  end

  defp sqlite_readonly(db_path, sql) do
    case System.cmd("sqlite3", ["-readonly", db_path, sql], stderr_to_stdout: true) do
      {output, 0} -> {:ok, output}
      {output, status} -> {:error, {:sqlite_read_failed, status, String.trim(output)}}
    end
  end

  defp collect_results(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, item}, {:ok, items} -> {:cont, {:ok, items ++ [item]}}
      {:error, reason}, _ -> {:halt, {:error, reason}}
    end)
  end

  defp string_field(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp int!(value) when is_integer(value), do: value
  defp int!(value) when is_binary(value), do: String.to_integer(value)

  defp blank?(value), do: value in [nil, ""]
end
