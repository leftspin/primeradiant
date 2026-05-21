defmodule Primeradiant.DaemonNewsReplayTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.DaemonNewsReplay
  alias Primeradiant.StorageHarness.DaemonNewsEvent
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
    assert report.extraction_quality.counts != []
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

  test "unsupported live extraction shapes are refused inspectably without mutating source" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-refusal-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    db_path = Path.join(tmp, "news.db")
    soup_db_path = Path.join(tmp, "primeradiant-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Planet discovery update",
        "Planet Nine mass is debated and discovery is preliminary according to sources."
      )
    ]

    offsets = write_archive!(raw_path, rows)
    create_source_db!(db_path, raw_path, offsets, ["news-planet"])
    before_stat = File.stat!(db_path)

    {:ok, _state, report} =
      DaemonNewsReplay.replay(
        db_path: db_path,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    after_stat = File.stat!(db_path)
    assert before_stat.mtime == after_stat.mtime
    assert before_stat.size == after_stat.size

    decision = List.first(report.ingestion.decisions)
    assert decision.decision_type == :abstain
    assert decision.status == :needs_more_evidence
    assert report.primeradiant_writes.inputs == 1
    assert report.primeradiant_writes.proposals == 1
    assert report.primeradiant_writes.graph_commits == 0

    assert [%{"status" => "usable", "count" => 1}] = report.extraction_quality.counts
    assert [] = report.extraction_quality.refused_or_low_confidence_sample

    normalized =
      sqlite_json!(
        soup_db_path,
        "SELECT normalized FROM inputs WHERE external_id = 'news-planet';"
      )

    facts = Jason.decode!(normalized)["production_extractor_v1"]["claims"]
    refute Enum.any?(facts, &(&1["structural_candidate"] == true))
  end

  test "event envelope drives R1 admission and output without cadence" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-event-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    [row] = [
      envelope(
        "Event Civic Clinic triage open",
        "Event Civic Clinic triage is open venue is north speaker is desk."
      )
    ]

    [{offset, length}] = write_archive!(raw_path, [row])
    event = committed_source_item_event("event-news-1", raw_path, offset, length, row)

    {:ok, state, report} =
      DaemonNewsEvent.consume_event(event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    assert report.source.mode == "event_envelope"
    assert report.source.event_id == "evt-event-news-1"
    assert report.event_driven_r1.persistent_service_installed == false
    assert report.event_driven_r1.production_source_event_emitter_present == false
    assert report.primeradiant_writes.owned_state_only
    assert report.primeradiant_writes.inputs == 1
    assert report.primeradiant_writes.graph_commits > 0
    assert report.changed_stories != []

    assert [%{external_id: "event-news-1"}] = state.inputs
    assert get_in(List.first(state.inputs).normalized, ["metadata", "event_driven_r1"])
    assert File.regular?(soup_db_path)
    assert DurableSoupDb.table_count(soup_db_path, "inputs", @tenant) == 1
  end

  test "event envelope refuses raw digest mismatch before admission" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-event-bad-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    raw_path = Path.join(tmp, "archive.jsonl")
    row = envelope("Event digest mismatch", "Digest mismatch service is halted.")
    [{offset, length}] = write_archive!(raw_path, [row])

    event =
      committed_source_item_event("event-news-bad", raw_path, offset, length, row)
      |> put_in(["raw_ref", "sha256"], String.duplicate("0", 64))

    assert {:error, {:raw_digest_mismatch, "event-news-bad"}} =
             DaemonNewsEvent.consume_event(event,
               soup_db_path: Path.join(tmp, "soup.sqlite3"),
               tenant_id: @tenant,
               actor_id: "flynn"
             )
  end

  test "R1 push and consume handoff keeps source read-only and writes primeradiant output" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-r1-handoff-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    raw_path = Path.join(tmp, "archive.jsonl")
    handoff_root = Path.join(tmp, "handoffs")
    run_root = Path.join(tmp, "runs")

    rows = [
      envelope("R1 Harbor halt", "R1 Harbor service is halted for current route is harbor"),
      envelope("R1 Harbor crews", "R1 Harbor service is halted for current crossings are evening")
    ]

    offsets = write_archive!(raw_path, rows)
    create_source_db!(source_db, raw_path, offsets)
    before_stat = File.stat!(source_db)

    push_script = Path.expand("scripts/r1/push_snapshot.sh")
    consume_script = Path.expand("scripts/r1/consume_handoff.sh")

    {handoff_dir, 0} =
      System.cmd(push_script, [
        "--source-db",
        source_db,
        "--handoff-root",
        handoff_root,
        "--run-id",
        "r1-test"
      ])

    handoff_dir = String.trim(handoff_dir)
    assert File.regular?(Path.join(handoff_dir, "manifest.json"))
    assert File.regular?(Path.join(handoff_dir, "news.db"))
    assert File.exists?(Path.join(handoff_dir, "raw"))

    after_push_stat = File.stat!(source_db)
    assert before_stat.mtime == after_push_stat.mtime
    assert before_stat.size == after_push_stat.size

    {out_dir, 0} =
      System.cmd(consume_script, [
        "--handoff-dir",
        handoff_dir,
        "--run-root",
        run_root,
        "--tenant",
        @tenant,
        "--actor",
        "flynn"
      ])

    out_dir = String.trim(out_dir)
    soup_db = Path.join(out_dir, "soup.sqlite3")
    report = Path.join(out_dir, "changed-stories-report.json")
    assert File.regular?(soup_db)
    assert File.regular?(report)

    report_json = report |> File.read!() |> Jason.decode!()
    assert get_in(report_json, ["r1_handoff", "persistent_service_installed"]) == false
    assert get_in(report_json, ["primeradiant_writes", "soup_db_path"]) == soup_db
    assert DurableSoupDb.table_count(soup_db, "inputs", @tenant) == 2

    after_consume_stat = File.stat!(source_db)
    assert before_stat.mtime == after_consume_stat.mtime
    assert before_stat.size == after_consume_stat.size
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

  defp create_source_db!(db_path, raw_path, offsets, ids \\ ["news-1", "news-2"])

  defp create_source_db!(db_path, raw_path, [{offset1, length1}, {offset2, length2}], [
         id1,
         id2
       ]) do
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
      ('#{id1}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:00:00Z', '2026-05-17T10:00:01Z', '#{raw_path}', #{offset1}, #{length1}, 'sha-1'),
      ('#{id2}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:05:00Z', '2026-05-17T10:05:01Z', '#{raw_path}', #{offset2}, #{length2}, 'sha-2');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp create_source_db!(db_path, raw_path, [{offset, length}], [id]) do
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
      ('#{id}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:00:00Z', '2026-05-17T10:00:01Z', '#{raw_path}', #{offset}, #{length}, 'sha-1');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp sqlite_json!(db_path, sql) do
    {output, 0} = System.cmd("sqlite3", [db_path, sql])
    String.trim(output)
  end

  defp committed_source_item_event(id, raw_path, offset, length, envelope) do
    %{
      "event_type" => "primeradiant.source.committed_item.v1",
      "event_id" => "evt-#{id}",
      "cursor" => "messages:#{id}",
      "emitted_at" => "2026-05-17T10:00:02Z",
      "source" => %{
        "adapter" => "daemon-news",
        "tenant_id" => @tenant,
        "item_id" => id,
        "message_type" => "swarm.channel.news.report.v0",
        "source_space" => "swarm.channel.news",
        "receptor_id" => "receptor",
        "sender_id" => "sender",
        "created_at" => "2026-05-17T10:00:00Z",
        "committed_at" => "2026-05-17T10:00:01Z"
      },
      "raw_ref" => %{
        "path" => raw_path,
        "offset" => offset,
        "length" => length,
        "sha256" => :crypto.hash(:sha256, Jason.encode!(envelope)) |> Base.encode16(case: :lower)
      },
      "acl" => %{"privacy" => "public"}
    }
  end
end
