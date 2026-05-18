defmodule Primeradiant.DaemonNewsReplayTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.DaemonNewsReplay
  alias Primeradiant.StorageHarness.DurableSoupDb

  @tenant Ecto.UUID.generate()

  test "replays daemon news rows through primeradiant-owned state and exposes evidence report" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    db_path = Path.join(tmp, "news.db")
    soup_db_path = Path.join(tmp, "primeradiant-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Civic Clinic triage open",
        "Civic Clinic triage is open venue is north speaker is desk."
      ),
      envelope(
        "Civic Clinic triage update",
        "Civic Clinic triage is closed venue is north speaker is desk."
      )
    ]

    offsets = write_archive!(raw_path, rows)
    create_source_db!(db_path, raw_path, offsets)
    before_stat = File.stat!(db_path)

    {:ok, state, report} =
      DaemonNewsReplay.replay(
        db_path: db_path,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    after_stat = File.stat!(db_path)
    assert before_stat.mtime == after_stat.mtime
    assert before_stat.size == after_stat.size

    assert report.source.mode == "read_only"
    assert report.source.source_row_count == 2
    assert report.primeradiant_writes.owned_state_only
    assert report.primeradiant_writes.durable
    assert report.primeradiant_writes.soup_db_path == soup_db_path
    assert report.primeradiant_writes.inputs == 2
    assert report.primeradiant_writes.graph_commits > 0
    assert File.regular?(soup_db_path)

    assert DurableSoupDb.table_count(soup_db_path, "inputs", @tenant) == 2
    assert DurableSoupDb.table_count(soup_db_path, "stories", @tenant) >= 1
    assert DurableSoupDb.table_count(soup_db_path, "story_events", @tenant) >= 2
    assert DurableSoupDb.table_count(soup_db_path, "story_fact_versions", @tenant) >= 1
    assert DurableSoupDb.table_count(soup_db_path, "proposals", @tenant) >= 2
    assert DurableSoupDb.table_count(soup_db_path, "proposal_decisions", @tenant) >= 2
    assert DurableSoupDb.table_count(soup_db_path, "graph_commits", @tenant) > 0
    assert DurableSoupDb.table_count(soup_db_path, "evidence_refs", @tenant) > 0

    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))
    assert Enum.all?(state.inputs, &(&1.source_type == "news_article"))

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["source_mode"]) == "manual_real_ingest_v1")
           )

    assert [%{"evidence" => evidence} | _] = report.changed_stories
    assert evidence != []

    labels =
      evidence
      |> Enum.flat_map(& &1.evidence_refs)
      |> Enum.map(& &1.label)

    assert Enum.any?(labels, &String.contains?(&1, "news-1"))
  end

  test "changed stories report is regenerated from persisted soup database" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-report-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    db_path = Path.join(tmp, "news.db")
    soup_db_path = Path.join(tmp, "primeradiant-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Harbor ferry halted",
        "Harbor ferry service_state is halted venue is pier speaker is operator."
      ),
      envelope(
        "Harbor ferry restored",
        "Harbor ferry service_state is restored venue is pier speaker is operator."
      )
    ]

    offsets = write_archive!(raw_path, rows)
    create_source_db!(db_path, raw_path, offsets)

    {:ok, _state, first_report} =
      DaemonNewsReplay.replay(
        db_path: db_path,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    persisted_report =
      DurableSoupDb.changed_stories_report(
        soup_db_path,
        @tenant,
        first_report.source,
        first_report.ingestion
      )

    assert persisted_report.changed_stories == first_report.changed_stories
    assert persisted_report.primeradiant_writes.soup_db_path == soup_db_path
  end

  test "validates daemon source schema before replaying" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-bad-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    db_path = Path.join(tmp, "news.db")

    {_out, 0} =
      System.cmd("sqlite3", [db_path, "CREATE TABLE messages (message_id TEXT PRIMARY KEY);"])

    assert {:error, {:unsupported_source_schema, ^db_path, missing}} =
             DaemonNewsReplay.validate_source_db(db_path)

    assert "raw_archive_path" in missing
  end

  defp envelope(title, summary) do
    %{
      "type" => "swarm.channel.news.report.v0",
      "body" => %{
        "title" => title,
        "summary" => summary,
        "url" => "https://example.test/#{String.replace(title, " ", "-")}",
        "published_at" => "2026-05-17T10:00:00Z",
        "source_name" => "Example News"
      },
      "provenance" => %{"source_name" => "Example News"},
      "dedupe" => %{"key" => title}
    }
  end

  defp write_archive!(path, envelopes) do
    {chunks, _offset} =
      Enum.map_reduce(envelopes, 0, fn envelope, offset ->
        bytes = Jason.encode!(envelope)
        line = bytes <> "\n"
        {{offset, byte_size(bytes)}, offset + byte_size(line)}
      end)

    File.write!(path, Enum.map_join(envelopes, "\n", &Jason.encode!/1) <> "\n")
    chunks
  end

  defp create_source_db!(db_path, raw_path, [{offset1, length1}, {offset2, length2}]) do
    sql = """
    CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE messages (
      message_id TEXT PRIMARY KEY,
      source_space TEXT,
      receptor_id TEXT,
      sender_id TEXT,
      message_type TEXT,
      created_at TEXT,
      received_at TEXT NOT NULL,
      raw_archive_path TEXT NOT NULL,
      raw_archive_offset INTEGER NOT NULL,
      raw_archive_length INTEGER NOT NULL,
      raw_sha256 TEXT NOT NULL
    );
    INSERT INTO messages VALUES
      ('news-1', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:00:00Z', '2026-05-17T10:00:01Z', '#{raw_path}', #{offset1}, #{length1}, 'sha-1'),
      ('news-2', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:05:00Z', '2026-05-17T10:05:01Z', '#{raw_path}', #{offset2}, #{length2}, 'sha-2');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end
end
