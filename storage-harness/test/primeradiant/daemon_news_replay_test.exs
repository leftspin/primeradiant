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

  test "real daemon news typography with explicit date can commit story state" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-typography-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    [row] = [
      envelope(
        "Tomb Raider: Legacy of Atlantis PC requirements list RTX 3080 and RX 6800 XT",
        "Tomb Raider: Legacy of Atlantis launches February 12, 2027 on PS5 — Amazon’s studio says trailer passes 5M views."
      )
    ]

    [{offset, length}] = write_archive!(raw_path, [row])
    event = committed_source_item_event("event-news-typography", raw_path, offset, length, row)

    {:ok, state, report} =
      DaemonNewsEvent.consume_event(event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    assert [%{"status" => "usable", "count" => 1}] = report.extraction_quality.counts
    assert report.primeradiant_writes.stories == 1
    assert report.primeradiant_writes.graph_commits > 0
    assert report.changed_stories != []
    assert [%{structural_facts: %{"date" => "feb-12-2027"}}] = state.stories
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

  test "source-side no-install emitter produces event envelope consumed by R1 path" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-emitter-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    soup_db_path = Path.join(tmp, "primeradiant-emitter-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    row =
      envelope(
        "Emitter Civic Clinic triage open",
        "Emitter Civic Clinic triage is open venue is north speaker is desk."
      )

    [{offset, length}] = write_archive!(raw_path, [row])
    sha = :crypto.hash(:sha256, Jason.encode!(row)) |> Base.encode16(case: :lower)
    create_source_db_with_hash!(source_db, raw_path, offset, length, "emitter-news-1", sha)
    before_stat = File.stat!(source_db)

    emitter_script = Path.expand("scripts/r1/emit_committed_event.sh")

    {event_json, 0} =
      System.cmd(emitter_script, [
        "--source-db",
        source_db,
        "--message-id",
        "emitter-news-1",
        "--tenant",
        @tenant,
        "--event-id",
        "evt-emitter-news-1"
      ])

    event = Jason.decode!(event_json)
    assert event["event_type"] == "primeradiant.source.committed_item.v1"
    assert get_in(event, ["source", "adapter"]) == "daemon-news"
    assert get_in(event, ["raw_ref", "sha256"]) == sha

    {:ok, _state, report} =
      DaemonNewsEvent.consume_event(event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn"
      )

    assert report.source.mode == "event_envelope"
    assert report.source.event_id == "evt-emitter-news-1"
    assert report.primeradiant_writes.inputs == 1
    assert report.primeradiant_writes.graph_commits > 0

    after_stat = File.stat!(source_db)
    assert before_stat.mtime == after_stat.mtime
    assert before_stat.size == after_stat.size
  end

  test "event package handoff carries bounded source bytes and drives persisted output" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-event-package-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    raw_root = Path.join(tmp, "source")
    File.mkdir_p!(raw_root)
    raw_path = Path.join(raw_root, "archive.jsonl")
    package_dir = Path.join([tmp, "handoff", "evt-package-news-1"])
    run_root = Path.join(tmp, "runs")

    row =
      envelope(
        "Package Civic Clinic triage open",
        "Package Civic Clinic triage is open venue is north speaker is desk."
      )

    [{offset, length}] = write_archive!(raw_path, [row])
    sha = :crypto.hash(:sha256, Jason.encode!(row)) |> Base.encode16(case: :lower)
    create_source_db_with_hash!(source_db, raw_path, offset, length + 1, "package-news-1", sha)
    before_stat = File.stat!(source_db)

    package_script = Path.expand("scripts/r1/emit_committed_event_package.sh")
    consume_script = Path.expand("scripts/r1/consume_event_package.sh")

    {package_out, 0} =
      System.cmd(package_script, [
        "--source-db",
        source_db,
        "--message-id",
        "package-news-1",
        "--tenant",
        @tenant,
        "--event-id",
        "evt-package-news-1",
        "--package-dir",
        package_dir
      ])

    assert String.trim(package_out) == package_dir
    assert File.regular?(Path.join(package_dir, "event.json"))
    assert File.regular?(Path.join(package_dir, "raw/source-envelope.json"))
    refute File.exists?(Path.join(package_dir, "news.db"))

    event = package_dir |> Path.join("event.json") |> File.read!() |> Jason.decode!()
    assert get_in(event, ["raw_ref", "path"]) == "raw/source-envelope.json"
    assert get_in(event, ["raw_ref", "offset"]) == 0
    assert get_in(event, ["raw_ref", "length"]) == length
    assert get_in(event, ["raw_ref", "sha256"]) == sha

    {out_dir, 0} =
      System.cmd(consume_script, [
        "--package-dir",
        package_dir,
        "--run-root",
        run_root,
        "--tenant",
        @tenant,
        "--actor",
        "flynn"
      ])

    out_dir = String.trim(out_dir)
    soup_db = Path.join(out_dir, "soup.sqlite3")
    report_path = Path.join(out_dir, "changed-stories-report.json")
    assert File.regular?(soup_db)
    assert File.regular?(report_path)

    report = report_path |> File.read!() |> Jason.decode!()
    assert get_in(report, ["source", "mode"]) == "event_envelope"
    assert get_in(report, ["source", "event_id"]) == "evt-package-news-1"
    assert get_in(report, ["event_driven_r1", "persistent_service_installed"]) == false
    assert get_in(report, ["primeradiant_writes", "soup_db_path"]) == soup_db
    assert DurableSoupDb.table_count(soup_db, "inputs", @tenant) == 1

    after_stat = File.stat!(source_db)
    assert before_stat.mtime == after_stat.mtime
    assert before_stat.size == after_stat.size
  end

  test "DB cursor reader emits bounded event packages after Primeradiant cursor" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-cursor-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    raw_path = Path.join(tmp, "archive.jsonl")
    package_root = Path.join(tmp, "cursor-packages")
    run_root = Path.join(tmp, "runs")

    rows = [
      envelope("Cursor first", "Cursor first service is open venue is north speaker is desk."),
      envelope("Cursor second", "Cursor second service is open venue is north speaker is desk."),
      envelope("Cursor third", "Cursor third service is open venue is north speaker is desk.")
    ]

    offsets = write_archive!(raw_path, rows)

    create_source_db_with_hashes!(
      source_db,
      raw_path,
      offsets,
      ["cursor-news-1", "cursor-news-2", "cursor-news-3"],
      rows
    )

    before_stat = File.stat!(source_db)
    cursor_script = Path.expand("scripts/r1/emit_cursor_event_packages.sh")
    consume_script = Path.expand("scripts/r1/consume_event_package.sh")

    {manifest_path, 0} =
      System.cmd(cursor_script, [
        "--source-db",
        source_db,
        "--tenant",
        @tenant,
        "--package-root",
        package_root,
        "--after-cursor",
        "2026-05-17T10:00:01Z|cursor-news-1",
        "--limit",
        "10",
        "--run-id",
        "cursor-test"
      ])

    manifest_path = String.trim(manifest_path)
    manifest = manifest_path |> File.read!() |> Jason.decode!()
    assert manifest["source_mode"] == "read_only_db_cursor"
    assert manifest["after_cursor"] == "2026-05-17T10:00:01Z|cursor-news-1"
    assert manifest["next_cursor"] == "2026-05-17T10:10:01Z|cursor-news-3"
    assert manifest["emitted_count"] == 2
    assert manifest["persistent_service_installed"] == false
    refute File.exists?(Path.join(package_root, "news.db"))

    [first_package | _] = manifest["packages"]

    {out_dir, 0} =
      System.cmd(consume_script, [
        "--package-dir",
        first_package["package_dir"],
        "--run-root",
        run_root,
        "--tenant",
        @tenant,
        "--actor",
        "flynn"
      ])

    report =
      out_dir
      |> String.trim()
      |> Path.join("changed-stories-report.json")
      |> File.read!()
      |> Jason.decode!()

    assert get_in(report, ["source", "mode"]) == "event_envelope"
    assert get_in(report, ["source", "event_id"]) == "cursor-test-1"
    assert get_in(report, ["primeradiant_writes", "inputs"]) == 1

    after_stat = File.stat!(source_db)
    assert before_stat.mtime == after_stat.mtime
    assert before_stat.size == after_stat.size
  end

  test "SQLite file wakeup runs read-only cursor importer without trusting file event" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-watch-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    raw_path = Path.join(tmp, "archive.jsonl")
    cursor_file = Path.join(tmp, "primeradiant/cursor.txt")
    package_root = Path.join(tmp, "packages")
    run_root = Path.join(tmp, "runs")

    first = envelope("Watch first", "Watch first service is open venue is north speaker is desk.")

    second =
      envelope("Watch second", "Watch second service is open venue is north speaker is desk.")

    [{offset1, length1}] = write_archive!(raw_path, [first])
    sha1 = :crypto.hash(:sha256, Jason.encode!(first)) |> Base.encode16(case: :lower)
    create_source_db_with_hash!(source_db, raw_path, offset1, length1 + 1, "watch-news-1", sha1)
    File.mkdir_p!(Path.dirname(cursor_file))
    File.write!(cursor_file, "2026-05-17T10:00:01Z|watch-news-1\n")

    watcher_script = Path.expand("scripts/r1/watch_sqlite_wakeup.sh")

    task =
      Task.async(fn ->
        System.cmd(watcher_script, [
          "--source-db",
          source_db,
          "--tenant",
          @tenant,
          "--cursor-file",
          cursor_file,
          "--package-root",
          package_root,
          "--run-root",
          run_root,
          "--limit",
          "10",
          "--run-id",
          "watch-test",
          "--timeout-seconds",
          "8",
          "--poll-interval-seconds",
          "1"
        ])
      end)

    Process.sleep(1200)
    {offset2, length2} = append_archive!(raw_path, second)
    sha2 = :crypto.hash(:sha256, Jason.encode!(second)) |> Base.encode16(case: :lower)
    insert_source_row!(source_db, raw_path, offset2, length2 + 1, "watch-news-2", sha2, 6)

    {report_path, 0} = Task.await(task, 10_000)
    report = report_path |> String.trim() |> File.read!() |> Jason.decode!()

    assert report["source_mode"] == "sqlite_file_wakeup_read_only_cursor"
    assert report["wake_kind"] == "sqlite_file_change"
    assert report["wake_signal_debounced"] == true
    assert report["importer_single_flight"] == true
    assert report["importer_catch_up_until_empty"] == true
    assert report["wake_payload_trusted_as_story_fact"] == false
    assert report["source_db_mutated_by_primeradiant"] == false
    assert report["persistent_service_installed"] == false
    assert report["after_cursor"] == "2026-05-17T10:00:01Z|watch-news-1"
    assert report["next_cursor"] == "2026-05-17T10:05:01Z|watch-news-2"
    assert report["emitted_count"] == 1
    assert File.read!(cursor_file) == "2026-05-17T10:05:01Z|watch-news-2\n"

    [pass] = report["passes"]
    manifest = pass["manifest_path"] |> File.read!() |> Jason.decode!()
    [package] = manifest["packages"]

    output_report =
      Path.join([run_root, package["event_id"], "changed-stories-report.json"])
      |> File.read!()
      |> Jason.decode!()

    assert get_in(output_report, ["source", "event_id"]) == "watch-test-pass-0001-1"
    assert get_in(output_report, ["primeradiant_writes", "inputs"]) == 1
    refute File.exists?(Path.join(package_root, "news.db"))
  end

  test "SQLite wakeup coalesces WAL burst and drains cursor in serialized bounded passes" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-watch-burst-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "source-news.db")
    raw_path = Path.join(tmp, "archive.jsonl")
    cursor_file = Path.join(tmp, "primeradiant/cursor.txt")
    package_root = Path.join(tmp, "packages")
    run_root = Path.join(tmp, "runs")

    rows =
      Enum.map(1..6, fn index ->
        envelope(
          "Burst #{index}",
          "Burst #{index} service is open venue is north speaker is desk."
        )
      end)

    [first | burst_rows] = rows
    [{offset1, length1}] = write_archive!(raw_path, [first])
    sha1 = :crypto.hash(:sha256, Jason.encode!(first)) |> Base.encode16(case: :lower)
    create_source_db_with_hash!(source_db, raw_path, offset1, length1 + 1, "burst-news-1", sha1)
    File.mkdir_p!(Path.dirname(cursor_file))
    File.write!(cursor_file, "2026-05-17T10:00:01Z|burst-news-1\n")

    watcher_script = Path.expand("scripts/r1/watch_sqlite_wakeup.sh")

    task =
      Task.async(fn ->
        System.cmd(watcher_script, [
          "--source-db",
          source_db,
          "--tenant",
          @tenant,
          "--cursor-file",
          cursor_file,
          "--package-root",
          package_root,
          "--run-root",
          run_root,
          "--limit",
          "2",
          "--run-id",
          "watch-burst-test",
          "--timeout-seconds",
          "8",
          "--poll-interval-seconds",
          "1",
          "--debounce-seconds",
          "1"
        ])
      end)

    Process.sleep(1200)

    Enum.with_index(burst_rows, 2)
    |> Enum.each(fn {row, index} ->
      {offset, length} = append_archive!(raw_path, row)
      sha = :crypto.hash(:sha256, Jason.encode!(row)) |> Base.encode16(case: :lower)

      insert_source_row!(
        source_db,
        raw_path,
        offset,
        length + 1,
        "burst-news-#{index}",
        sha,
        index
      )
    end)

    {report_path, 0} = Task.await(task, 15_000)
    report = report_path |> String.trim() |> File.read!() |> Jason.decode!()

    assert report["source_mode"] == "sqlite_file_wakeup_read_only_cursor"
    assert report["wake_signal_debounced"] == true
    assert report["importer_single_flight"] == true
    assert report["importer_catch_up_until_empty"] == true
    assert report["wake_payload_trusted_as_story_fact"] == false
    assert report["source_db_mutated_by_primeradiant"] == false
    assert report["persistent_service_installed"] == false
    assert report["after_cursor"] == "2026-05-17T10:00:01Z|burst-news-1"
    assert report["next_cursor"] == "2026-05-17T10:05:01Z|burst-news-6"
    assert report["emitted_count"] == 5
    assert report["pass_count"] == 3
    assert Enum.map(report["passes"], & &1["emitted_count"]) == [2, 2, 1]
    assert File.read!(cursor_file) == "2026-05-17T10:05:01Z|burst-news-6\n"

    package_event_ids =
      report["passes"]
      |> Enum.flat_map(fn pass ->
        pass["manifest_path"]
        |> File.read!()
        |> Jason.decode!()
        |> Map.fetch!("packages")
      end)
      |> Enum.map(& &1["event_id"])

    assert length(package_event_ids) == 5
    assert Enum.uniq(package_event_ids) == package_event_ids

    output_reports = Path.wildcard(Path.join([run_root, "*", "changed-stories-report.json"]))
    assert length(output_reports) == 5

    output_event_ids =
      output_reports
      |> Enum.map(fn path ->
        path |> File.read!() |> Jason.decode!() |> get_in(["source", "event_id"])
      end)
      |> Enum.sort()

    assert output_event_ids == Enum.sort(package_event_ids)
    refute File.exists?(Path.join(package_root, "news.db"))
  end

  test "Subspace daemon DB wakeup imports news envelopes without mutating source DB" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-subspace-daemon-watch-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    source_db = Path.join(tmp, "daemon.sqlite3")
    cursor_file = Path.join(tmp, "primeradiant/cursor.txt")
    package_root = Path.join(tmp, "packages")
    run_root = Path.join(tmp, "runs")

    create_subspace_daemon_db!(
      source_db,
      [
        {"subspace-news-1", "2026-06-03 05:00:00",
         envelope(
           "Subspace first",
           "Subspace first service is open venue is north speaker is desk."
         )}
      ]
    )

    before_stat = File.stat!(source_db)
    File.mkdir_p!(Path.dirname(cursor_file))
    File.write!(cursor_file, "2026-06-03 05:00:00|1\n")

    watcher_script = Path.expand("scripts/r1/watch_sqlite_wakeup.sh")
    emitter_script = Path.expand("scripts/r1/emit_subspace_daemon_cursor_event_packages.sh")

    task =
      Task.async(fn ->
        System.cmd(watcher_script, [
          "--source-db",
          source_db,
          "--tenant",
          @tenant,
          "--cursor-file",
          cursor_file,
          "--package-root",
          package_root,
          "--run-root",
          run_root,
          "--limit",
          "10",
          "--run-id",
          "subspace-watch-test",
          "--timeout-seconds",
          "8",
          "--poll-interval-seconds",
          "1",
          "--emit-cursor-script",
          emitter_script
        ])
      end)

    Process.sleep(1200)

    insert_subspace_daemon_event!(
      source_db,
      2,
      "subspace-news-2",
      "2026-06-03 05:01:00",
      envelope(
        "Subspace second",
        "Subspace second service is open venue is north speaker is desk."
      )
    )

    after_source_commit_stat = File.stat!(source_db)

    {report_path, 0} = Task.await(task, 10_000)
    report = report_path |> String.trim() |> File.read!() |> Jason.decode!()

    assert report["source_mode"] == "sqlite_file_wakeup_read_only_cursor"
    assert report["wake_payload_trusted_as_story_fact"] == false
    assert report["source_db_mutated_by_primeradiant"] == false
    assert report["emitted_count"] == 1
    assert report["next_cursor"] == "2026-06-03 05:01:00|2"

    [pass] = report["passes"]
    manifest = pass["manifest_path"] |> File.read!() |> Jason.decode!()
    assert manifest["source_mode"] == "subspace_daemon_read_only_db_cursor"

    [package] = manifest["packages"]

    output_report =
      Path.join([run_root, package["event_id"], "changed-stories-report.json"])
      |> File.read!()
      |> Jason.decode!()

    assert get_in(output_report, ["source", "event_id"]) == "subspace-watch-test-pass-0001-1"
    assert get_in(output_report, ["source", "message_type"]) == "swarm.channel.news.report.v0"
    assert get_in(output_report, ["primeradiant_writes", "inputs"]) == 1

    after_stat = File.stat!(source_db)
    assert before_stat != after_source_commit_stat
    assert after_source_commit_stat.mtime == after_stat.mtime
    assert after_source_commit_stat.size == after_stat.size
    refute File.exists?(Path.join(package_root, "daemon.sqlite3"))
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

  defp append_archive!(path, envelope) do
    {:ok, stat} = File.stat(path)
    bytes = Jason.encode!(envelope)
    File.write!(path, bytes <> "\n", [:append])
    {stat.size, byte_size(bytes)}
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

  defp create_source_db_with_hash!(db_path, raw_path, offset, length, id, sha) do
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
      ('#{id}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:00:00Z', '2026-05-17T10:00:01Z', '#{raw_path}', #{offset}, #{length}, '#{sha}');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp insert_source_row!(db_path, raw_path, offset, length, id, sha, index) do
    minute = index - 1

    sql = """
    INSERT INTO messages VALUES
      ('#{id}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:#{pad2(minute)}:00Z', '2026-05-17T10:#{pad2(minute)}:01Z', '#{raw_path}', #{offset}, #{length}, '#{sha}');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp create_subspace_daemon_db!(db_path, rows) do
    sql = """
    CREATE TABLE daemon_event (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ingress_source_id INTEGER NOT NULL,
      message_id TEXT NOT NULL,
      message_timestamp TEXT NOT NULL,
      inbound_event TEXT NOT NULL,
      author_id TEXT NOT NULL,
      author_name TEXT NOT NULL,
      text TEXT NOT NULL,
      sender_embeddings_json TEXT,
      attention_space_id TEXT,
      attention_fallback INTEGER NOT NULL,
      payload_json TEXT,
      raw_body TEXT,
      accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      attention_disposition TEXT NOT NULL DEFAULT 'deliver',
      attention_delivery_mode TEXT NOT NULL DEFAULT 'unknown_legacy'
    );
    #{Enum.map_join(rows, "\n", fn {message_id, accepted_at, item} -> "INSERT INTO daemon_event (ingress_source_id, message_id, message_timestamp, inbound_event, author_id, author_name, text, attention_fallback, accepted_at) VALUES (1, '#{message_id}', '#{accepted_at}Z', 'new_message', 'sender', 'argus-racter-publisher', '#{sql_string(Jason.encode!(item))}', 0, '#{accepted_at}');" end)}
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp insert_subspace_daemon_event!(db_path, id, message_id, accepted_at, item) do
    sql = """
    INSERT INTO daemon_event (id, ingress_source_id, message_id, message_timestamp, inbound_event, author_id, author_name, text, attention_fallback, accepted_at)
    VALUES (#{id}, 1, '#{message_id}', '#{accepted_at}Z', 'new_message', 'sender', 'argus-racter-publisher', '#{sql_string(Jason.encode!(item))}', 0, '#{accepted_at}');
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp sql_string(value), do: String.replace(value, "'", "''")

  defp create_source_db_with_hashes!(db_path, raw_path, offsets, ids, rows) do
    inserts =
      offsets
      |> Enum.zip(ids)
      |> Enum.zip(rows)
      |> Enum.with_index()
      |> Enum.map(fn {{{{offset, length}, id}, row}, index} ->
        sha = :crypto.hash(:sha256, Jason.encode!(row)) |> Base.encode16(case: :lower)
        minute = index * 5

        "('#{id}', 'swarm.channel.news', 'receptor', 'sender', 'swarm.channel.news.report.v0', '2026-05-17T10:#{pad2(minute)}:00Z', '2026-05-17T10:#{pad2(minute)}:01Z', '#{raw_path}', #{offset}, #{length + 1}, '#{sha}')"
      end)
      |> Enum.join(",\n      ")

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
      #{inserts};
    """

    {_out, 0} = System.cmd("sqlite3", [db_path, sql])
  end

  defp pad2(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")

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
