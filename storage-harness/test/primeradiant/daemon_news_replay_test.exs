defmodule Primeradiant.DaemonNewsReplayTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.DaemonNewsReplay
  alias Primeradiant.StorageHarness.DaemonNewsEvent
  alias Primeradiant.StorageHarness.DurableSoupDb
  alias Primeradiant.StorageHarness.KnowledgeWork
  alias Primeradiant.StorageHarness.LiveStoryAgentLoop
  alias Primeradiant.StorageHarness.State
  alias Primeradiant.Soup

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
    assert report.primeradiant_writes.graph_commits == 0
    assert report.story_meaning_proof == false
    assert File.regular?(soup_db_path)

    assert DurableSoupDb.table_count(soup_db_path, "inputs", @tenant) == 2
    assert DurableSoupDb.table_count(soup_db_path, "stories", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "story_events", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "story_fact_versions", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "proposals", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "proposal_decisions", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "graph_commits", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "evidence_refs", @tenant) == 0

    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))
    assert Enum.all?(state.inputs, &(&1.source_type == "news_article"))

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["source_mode"]) == "manual_real_ingest_v1")
           )

    assert report.changed_stories == []
    assert length(report.admitted_source_items) == 2
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
      DurableSoupDb.source_admission_report(
        soup_db_path,
        @tenant,
        first_report.source,
        first_report.ingestion
      )

    assert persisted_report.admitted_source_items == first_report.admitted_source_items
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

    assert report.primeradiant_writes.inputs == 1
    assert report.primeradiant_writes.proposals == 0
    assert report.primeradiant_writes.graph_commits == 0
    assert report.ingestion.meaning_proof == :requires_packet_grounded_agent_runs

    normalized =
      sqlite_json!(
        soup_db_path,
        "SELECT normalized FROM inputs WHERE external_id = 'news-planet';"
      )

    admitted = Jason.decode!(normalized)
    assert admitted["source_mode"] == "manual_real_ingest_v1"
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
    assert report.primeradiant_writes.graph_commits == 0
    assert report.changed_stories == []

    assert [%{external_id: "event-news-1"}] = state.inputs
    assert get_in(List.first(state.inputs).normalized, ["metadata", "event_driven_r1"])
    assert File.regular?(soup_db_path)
    assert DurableSoupDb.table_count(soup_db_path, "inputs", @tenant) == 1
  end

  test "event envelope can activate story agents and persist agent-authored meaning state" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-story-agents-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    [row] = [
      envelope(
        "Agent Civic Clinic triage open",
        "Agent Civic Clinic triage is open venue is north speaker is desk."
      )
    ]

    [{offset, length}] = write_archive!(raw_path, [row])
    event = committed_source_item_event("event-agent-news-1", raw_path, offset, length, row)

    {:ok, state, report} =
      DaemonNewsEvent.consume_event(event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn",
        story_agent_loop?: true,
        story_agent_opts: [adapter: &stub_story_agent/3]
      )

    loop = report.live_story_agent_loop
    [chain] = loop.correlation_chains

    assert report.story_meaning_proof == false
    assert report.substrate_proof_only == true
    assert loop.source_behavior == :evidence_admission_then_live_story_agents
    assert loop.agent_runs == 3
    assert loop.agent_families == [:story_identity, :meaning_update, :story_synthesis]
    assert loop.proposals == 1
    assert loop.proposal_ops == 1
    assert loop.proposal_decisions == 1
    assert loop.graph_commits == 1
    assert loop.story_events == 1
    assert loop.story_card_versions == 1
    assert loop.story_source_coverage == 1
    assert loop.story_key_claims == 1
    assert loop.zero_agent_zero_story_shape_nonconforming? == false
    assert loop.story_meaning_proof == false

    assert chain.source_ref == "news_article:event-agent-news-1"
    assert chain.activation_id
    assert length(chain.packet_ids) == 3
    assert length(chain.agent_run_ids) == 3
    assert chain.proposal_id
    assert chain.graph_commit_id
    assert chain.story_event_id
    assert chain.story_card_version_id
    assert chain.story_synthesis_agent_run_id
    assert chain.evidence_refs == ["evidence:news_article:event-agent-news-1:body_text:0:97"]

    assert report.primeradiant_writes.inputs == 1
    assert report.primeradiant_writes.agent_runs == 4
    assert report.primeradiant_writes.stories == 1
    assert report.primeradiant_writes.proposals == 1
    assert report.primeradiant_writes.proposal_ops == 1
    assert report.primeradiant_writes.proposal_decisions == 1
    assert report.primeradiant_writes.graph_commits == 1
    assert report.primeradiant_writes.story_events == 1
    assert report.primeradiant_writes.story_card_versions == 1
    assert report.primeradiant_writes.story_source_coverage == 1
    assert report.primeradiant_writes.story_key_claims == 1
    assert report.seen_state_delta.authored_outputs == 0
    assert report.seen_state_delta.authored_output_units == 0
    assert report.seen_state_delta.seen_states == 0
    assert report.seen_state_delta.seen_state_refs == 0
    assert report.changed_stories != []

    soup_feed = Soup.feed(state, %{"consumer" => "reporter", "projection" => "story_cards"})
    [card_item] = soup_feed.items
    assert card_item.story_card_version_id == chain.story_card_version_id
    assert card_item.deck["state"] == "complete"
    assert card_item.summary["state"] == "complete"
    assert [%{claim_ref: "claim:civic-clinic-triage:service"}] = card_item.key_claims
    assert [%{source_ref: "news_article:event-agent-news-1"}] = card_item.source_coverage

    canonical_public_url =
      card_item.source_coverage
      |> hd()
      |> Map.fetch!(:canonical_public_url)

    assert canonical_public_url["state"] in ["complete", "unavailable"]
    assert canonical_public_url["value"] || canonical_public_url["reason"]

    assert card_item.changed_since_seen.state == "complete"
    assert card_item.topic_salience["distinct_source_count"] == 1

    gazette_feed = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"})
    [gazette_item] = gazette_feed.items
    [gazette_source] = gazette_item.source_coverage
    assert gazette_feed.blockers == []
    assert gazette_item.story_card_version_id == chain.story_card_version_id
    assert gazette_source.source_ref == "news_article:event-agent-news-1"
    assert gazette_source.canonical_public_url["state"] in ["complete", "unavailable"]
    assert gazette_source.source_domain["state"] in ["complete", "unavailable"]
    assert gazette_source.source_label["state"] in ["complete", "unavailable"]
    assert gazette_source.publication["state"] in ["complete", "unavailable"]
    assert gazette_source.contribution_reason["state"] in ["complete", "unavailable"]

    assert [%{external_id: "event-agent-news-1"} = input] = state.inputs
    assert get_in(input.normalized, ["meaning_proof"]) == "not_ingest_owned"
    assert is_nil(input.normalized["story_identity"])
    assert is_nil(input.normalized["story_classification"])
    assert is_nil(input.normalized["materiality_decision"])
    assert is_nil(input.normalized["relevance_decision"])

    assert DurableSoupDb.table_count(soup_db_path, "inputs", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "agent_runs", @tenant) == 4
    assert DurableSoupDb.table_count(soup_db_path, "stories", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "proposals", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "proposal_ops", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "proposal_decisions", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "graph_commits", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_events", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_card_versions", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_source_coverage", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_key_claims", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_card_change_sets", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "story_reader_deltas", @tenant) == 1
    assert DurableSoupDb.table_count(soup_db_path, "authored_outputs", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "authored_output_units", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "seen_states", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "seen_state_refs", @tenant) == 0
    assert DurableSoupDb.table_count(soup_db_path, "evidence_refs", @tenant) > 0

    agent_scopes =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT agent_type, scope FROM agent_runs ORDER BY agent_type;"
      )
      |> Map.new(fn row -> {row["agent_type"], Jason.decode!(row["scope"])} end)

    assert agent_scopes |> Map.keys() |> Enum.sort() == [
             "flynn_seen_delta",
             "meaning_update",
             "story_identity",
             "story_synthesis"
           ]

    agent_scopes
    |> Map.take(["meaning_update", "story_identity", "story_synthesis"])
    |> Enum.each(fn {agent_type, scope} ->
      assert scope["correlation_id"] == chain.correlation_id
      assert scope["packet_id"] in chain.packet_ids
      assert String.starts_with?(scope["packet_id"], "packet:#{agent_type}:")
      assert scope["evidence_refs"] == chain.evidence_refs
      assert scope["producer_kind"] == "test_stub"
      assert scope["decision_source"] == "test_stub"
    end)

    [proposal] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT id, agent_run_id, story_id, status FROM proposals WHERE id = '#{chain.proposal_id}';"
      )

    assert proposal["id"] == chain.proposal_id
    assert proposal["agent_run_id"] == chain.meaning_agent_run_id
    assert proposal["story_id"] == chain.story_id
    assert proposal["status"] == "accepted"

    [proposal_op] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT id, proposal_id, payload, status FROM proposal_ops WHERE id = '#{chain.proposal_op_id}';"
      )

    assert proposal_op["id"] == chain.proposal_op_id
    assert proposal_op["proposal_id"] == chain.proposal_id
    assert proposal_op["status"] == "committed"
    payload = Jason.decode!(proposal_op["payload"])
    assert payload["correlation_id"] == chain.correlation_id
    assert payload["source_ref"] == chain.source_ref
    assert payload["meaning_agent_run_id"] == chain.meaning_agent_run_id
    assert payload["operation_family"] == "commit_story_meaning"

    [commit] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT id, proposal_id, proposal_op_id, committed_by_type, committed_by_id FROM graph_commits WHERE id = '#{chain.graph_commit_id}';"
      )

    assert commit["id"] == chain.graph_commit_id
    assert commit["proposal_id"] == chain.proposal_id
    assert commit["proposal_op_id"] == chain.proposal_op_id
    assert commit["committed_by_type"] == "agent"
    assert commit["committed_by_id"] == chain.meaning_agent_run_id

    [event_row] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT id, story_id, input_id, proposal_id, proposal_op_id, graph_commit_id FROM story_events;"
      )

    assert event_row["id"] == chain.story_event_id
    assert event_row["story_id"] == chain.story_id
    assert event_row["input_id"] == input.id
    assert event_row["proposal_id"] == chain.proposal_id
    assert event_row["proposal_op_id"] == chain.proposal_op_id
    assert event_row["graph_commit_id"] == chain.graph_commit_id

    evidence_labels =
      sqlite_json_rows!(soup_db_path, "SELECT evidence_label FROM evidence_refs;")
      |> Enum.map(& &1["evidence_label"])
      |> Enum.uniq()

    assert Enum.all?(chain.evidence_refs, &(&1 in evidence_labels))
  end

  test "event story agents persist Flynn seen refs and later soup-native deltas across cycles" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-seen-deltas-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Agent Civic Clinic triage open",
        "Agent Civic Clinic triage is open venue is north speaker is desk."
      ),
      envelope(
        "Agent Civic Clinic triage adds west desk",
        "Agent Civic Clinic triage remains open and now adds west desk coverage."
      )
    ]

    offsets = write_archive!(raw_path, rows)

    first_event =
      committed_source_item_event(
        "event-agent-news-1",
        raw_path,
        elem(Enum.at(offsets, 0), 0),
        elem(Enum.at(offsets, 0), 1),
        Enum.at(rows, 0)
      )

    second_event =
      committed_source_item_event(
        "event-agent-news-2",
        raw_path,
        elem(Enum.at(offsets, 1), 0),
        elem(Enum.at(offsets, 1), 1),
        Enum.at(rows, 1)
      )

    {:ok, first_state, first_report} =
      DaemonNewsEvent.consume_event(first_event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn",
        story_agent_loop?: true,
        story_agent_opts: [adapter: &stub_story_agent_with_later_update/3]
      )

    assert first_report.seen_state_delta.seen_states == 0
    assert first_report.seen_state_delta.authored_outputs == 0

    {:ok, first_state, _reader_delta} = KnowledgeWork.record_verified_delta(first_state, "flynn")
    persist_test_state!(soup_db_path, first_state)

    {:ok, second_state, second_report} =
      DaemonNewsEvent.consume_event(second_event,
        soup_db_path: soup_db_path,
        tenant_id: @tenant,
        actor_id: "flynn",
        story_agent_loop?: true,
        story_agent_opts: [adapter: &stub_story_agent_with_later_update/3]
      )

    assert length(second_state.inputs) == 2
    assert length(second_state.stories) == 1
    assert second_state.stories |> hd() |> Map.fetch!(:version) == 2

    assert second_report.live_story_agent_loop.correlation_chains
           |> hd()
           |> Map.fetch!(:classification) == "attach"

    assert second_report.seen_state_delta.authored_outputs == 1
    assert second_report.seen_state_delta.authored_output_units == 1
    assert second_report.seen_state_delta.seen_states == 1
    assert second_report.seen_state_delta.seen_state_refs >= 4

    [seen] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT seen_story_version FROM seen_states WHERE tenant_id = '#{@tenant}';"
      )

    assert seen["seen_story_version"] == 1

    seen_refs =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT ref_kind, ref_id FROM seen_state_refs WHERE tenant_id = '#{@tenant}' ORDER BY ref_kind, ref_id;"
      )

    assert %{"ref_kind" => "input", "ref_id" => "news_article:event-agent-news-1"} in seen_refs
    refute %{"ref_kind" => "input", "ref_id" => "news_article:event-agent-news-2"} in seen_refs
    assert %{"ref_kind" => "claim", "ref_id" => "claim:civic-clinic-triage:service"} in seen_refs
    refute %{"ref_kind" => "claim", "ref_id" => "claim:civic-clinic-triage:coverage"} in seen_refs
  end

  test "event story agents persist nonmaterial exclusion delta units across cycles" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-nonmaterial-deltas-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Agent Civic Clinic triage open",
        "Agent Civic Clinic triage is open venue is north speaker is desk."
      ),
      envelope(
        "Agent Civic Clinic triage duplicate",
        "Agent Civic Clinic triage is open venue is north speaker is desk."
      ),
      envelope(
        "Agent Civic Clinic triage no change",
        "Agent Civic Clinic triage reminder repeats the current desk state."
      ),
      envelope(
        "Agent Civic Clinic triage stale background",
        "Agent Civic Clinic triage background check says no current material change."
      ),
      envelope(
        "Agent Civic Clinic triage color",
        "Agent Civic Clinic triage coverage is described as calm and well organized."
      )
    ]

    offsets = write_archive!(raw_path, rows)

    rows
    |> Enum.with_index(1)
    |> Enum.reduce(nil, fn {row, index}, _state ->
      event =
        committed_source_item_event(
          "event-agent-news-#{index}",
          raw_path,
          elem(Enum.at(offsets, index - 1), 0),
          elem(Enum.at(offsets, index - 1), 1),
          row
        )

      {:ok, state, _report} =
        DaemonNewsEvent.consume_event(event,
          soup_db_path: soup_db_path,
          tenant_id: @tenant,
          actor_id: "flynn",
          story_agent_loop?: true,
          story_agent_opts: [adapter: &stub_story_agent_with_nonmaterial_deltas/3]
        )

      if index == 1 do
        {:ok, state, _reader_delta} = KnowledgeWork.record_verified_delta(state, "flynn")
        persist_test_state!(soup_db_path, state)
        state
      else
        state
      end
    end)

    [seen] =
      sqlite_json_rows!(
        soup_db_path,
        "SELECT seen_story_version FROM seen_states WHERE tenant_id = '#{@tenant}';"
      )

    assert seen["seen_story_version"] == 1

    delta_rows =
      sqlite_json_rows!(
        soup_db_path,
        """
        SELECT nonmaterial_exclusions
        FROM story_reader_deltas
        WHERE tenant_id = '#{@tenant}'
        ORDER BY inserted_at;
        """
      )

    exclusions =
      delta_rows
      |> Enum.flat_map(&(Jason.decode!(&1["nonmaterial_exclusions"]) || []))

    unique_exclusions = Enum.uniq_by(exclusions, & &1["text"])
    assert length(unique_exclusions) >= 4
    assert Enum.any?(unique_exclusions, &String.contains?(&1["text"], "duplicate evidence"))
    assert Enum.any?(unique_exclusions, &String.contains?(&1["text"], "repeated known facts"))
    assert Enum.any?(unique_exclusions, &String.contains?(&1["text"], "mostly background"))
    assert Enum.any?(unique_exclusions, &String.contains?(&1["text"], "adds color only"))

    Enum.each(unique_exclusions, fn exclusion ->
      assert exclusion["claim_refs"] == []
      assert String.contains?(exclusion["text"], "no material")
    end)
  end

  test "live story overlap hint preserves nonmaterial no-op when model overstates update" do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-daemon-news-overlap-noop-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)

    soup_db_path = Path.join(tmp, "primeradiant-event-soup.sqlite3")
    raw_path = Path.join(tmp, "archive.jsonl")

    rows = [
      envelope(
        "Xbox exclusive strategy reset",
        "Xbox exclusive strategy reset says Asha Sharma wants exclusive content and case by case decisions for console releases."
      ),
      envelope(
        "Xbox exclusive strategy reset follow-up",
        "Xbox exclusive strategy reset repeats that Asha Sharma wants exclusive content and case by case decisions for console releases."
      )
    ]

    offsets = write_archive!(raw_path, rows)

    rows
    |> Enum.with_index(1)
    |> Enum.reduce(nil, fn {row, index}, _state ->
      event =
        committed_source_item_event(
          "event-agent-news-#{index}",
          raw_path,
          elem(Enum.at(offsets, index - 1), 0),
          elem(Enum.at(offsets, index - 1), 1),
          row
        )

      {:ok, state, _report} =
        DaemonNewsEvent.consume_event(event,
          soup_db_path: soup_db_path,
          tenant_id: @tenant,
          actor_id: "flynn",
          story_agent_loop?: true,
          story_agent_opts: [adapter: &stub_story_agent_overstates_repeated_update/3]
        )

      if index == 1 do
        {:ok, state, _reader_delta} = KnowledgeWork.record_verified_delta(state, "flynn")
        persist_test_state!(soup_db_path, state)
        state
      else
        state
      end
    end)

    events =
      sqlite_json_rows!(
        soup_db_path,
        """
        SELECT story_version, classification, changed_facts
        FROM story_events
        WHERE tenant_id = '#{@tenant}'
        ORDER BY story_version;
        """
      )

    assert Enum.map(events, & &1["classification"]) == ["split", "no_op"]
    assert Jason.decode!(List.last(events)["changed_facts"]) == %{}

    [delta] =
      sqlite_json_rows!(
        soup_db_path,
        """
        SELECT nonmaterial_exclusions
        FROM story_reader_deltas
        WHERE tenant_id = '#{@tenant}'
        ORDER BY inserted_at DESC
        LIMIT 1;
        """
      )

    [exclusion] = Jason.decode!(delta["nonmaterial_exclusions"])
    assert String.contains?(exclusion["text"], "no material new update")
    assert exclusion["claim_refs"] == []
  end

  test "live story-agent loop does not claim story proof with zero admissions" do
    state = State.new(tenant_id: @tenant, user_id: "flynn")

    {_state, report} = LiveStoryAgentLoop.run(state, [], "flynn", adapter: &stub_story_agent/3)

    assert report.substrate_proof_only == true
    assert report.story_meaning_proof == false
    assert report.zero_agent_zero_story_shape_nonconforming? == true
    assert report.correlation_chains == []
    assert report.agent_runs == 0
    assert report.stories == 0
    assert report.proposals == 0
    assert report.graph_commits == 0
  end

  test "real daemon news typography with explicit date admits evidence only" do
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

    assert report.primeradiant_writes.stories == 0
    assert report.primeradiant_writes.graph_commits == 0
    assert report.changed_stories == []
    assert state.stories == []
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
    assert report.primeradiant_writes.graph_commits == 0

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

  test "live EURISKO watcher enables story agents by default with explicit evidence-only opt-out" do
    watcher_script = Path.expand("scripts/r1/live_subspace_daemon_watcher_once.sh")
    emitter_script = Path.expand("scripts/r1/emit_subspace_daemon_cursor_event_packages.sh")
    source = File.read!(watcher_script)
    emitter_source = File.read!(emitter_script)

    assert source =~ ~s(STORY_AGENTS="true")
    assert source =~ "--story-agents true|false"
    assert source =~ "consume_event_package.sh"
    assert source =~ ~s(if [[ "$STORY_AGENTS" == "true" ]]; then)
    assert source =~ "--actor '$ACTOR' --story-agents --soup-db '$EURISKO_SOUP_DB'"
    assert source =~ "--actor '$ACTOR' --soup-db '$EURISKO_SOUP_DB'"
    assert source =~ "--consume-packages false"
    assert source =~ "--eurisko-soup-db PATH"

    assert source =~
             "EURISKO_SOUP_DB=\"/home/clu/.local/state/primeradiant/soup-api/soup.sqlite3\""

    assert source =~ "eurisko_soup_db: $eurisko_soup_db"
    assert emitter_source =~ ~s(sqlite3 -readonly -cmd ".timeout 10000" -json "$SOURCE_DB")
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

  defp sqlite_json_rows!(db_path, sql) do
    {output, 0} = System.cmd("sqlite3", ["-cmd", ".mode json", db_path, sql])
    Jason.decode!(output)
  end

  defp persist_test_state!(soup_db_path, state) do
    DurableSoupDb.persist!(soup_db_path, state, %{
      source_kind: "test_reader_delta",
      source_db_path: "event:test-reader-delta",
      source_row_count: 0
    })
  end

  defp stub_story_agent(%{role: :story_identity}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "classification" => "new_story",
        "confidence" => 0.82,
        "rationale" => "packet identifies a bounded clinic triage story"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp stub_story_agent(%{role: :meaning_update}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "operation_family" => "commit_story_meaning",
        "classification" => "new_story",
        "changed_facts" => %{"service" => "triage", "state" => "open", "venue" => "north"},
        "confidence" => 0.79,
        "rationale" => "packet supports an open triage service event"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp stub_story_agent(%{role: :story_synthesis}, packet, _ctx),
    do: stub_story_synthesis(packet)

  defp stub_story_agent_with_later_update(%{role: :story_identity}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "classification" => "new_story",
        "confidence" => 0.82,
        "rationale" => "packet identifies a bounded clinic triage story"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp stub_story_agent_with_later_update(%{role: :meaning_update}, packet, _ctx) do
    changed_facts =
      if packet.external_id == "event-agent-news-2" do
        %{"service" => "triage", "state" => "open", "coverage" => "west"}
      else
        %{"service" => "triage", "state" => "open", "venue" => "north"}
      end

    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "operation_family" => "commit_story_meaning",
        "classification" => "substantive_update",
        "changed_facts" => changed_facts,
        "confidence" => 0.79,
        "rationale" => "packet supports a triage service event"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp stub_story_agent_with_later_update(%{role: :story_synthesis}, packet, _ctx),
    do: stub_story_synthesis(packet)

  defp stub_story_agent_with_nonmaterial_deltas(%{role: :story_identity}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "classification" => "new_story",
        "confidence" => 0.82,
        "rationale" => "packet identifies a bounded clinic triage story"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp stub_story_agent_with_nonmaterial_deltas(%{role: :meaning_update}, packet, _ctx) do
    {classification, changed_facts} =
      case packet.external_id do
        "event-agent-news-1" ->
          {"substantive_update", %{"service" => "triage", "state" => "open"}}

        "event-agent-news-2" ->
          {"duplicate", %{}}

        "event-agent-news-3" ->
          {"no_op", %{}}

        "event-agent-news-4" ->
          {"stale", %{}}

        "event-agent-news-5" ->
          {"adds_color", %{}}
      end

    %{
      output: %{
        "story_key" => "civic-clinic-triage",
        "operation_family" => "commit_story_meaning",
        "classification" => classification,
        "changed_facts" => changed_facts,
        "confidence" => 0.79,
        "rationale" => "packet is classified against the existing triage story"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp stub_story_agent_with_nonmaterial_deltas(%{role: :story_synthesis}, packet, _ctx),
    do: stub_story_synthesis(packet)

  defp stub_story_agent_overstates_repeated_update(%{role: :story_identity}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "xbox-exclusive-strategy-reset",
        "classification" => "substantive_update",
        "confidence" => 0.82,
        "rationale" => "packet identifies Xbox exclusivity strategy"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp stub_story_agent_overstates_repeated_update(%{role: :meaning_update}, _packet, _ctx) do
    %{
      output: %{
        "story_key" => "xbox-exclusive-strategy-reset",
        "operation_family" => "commit_story_meaning",
        "classification" => "substantive_update",
        "changed_facts" => %{"strategy" => "exclusive-content-case-by-case"},
        "confidence" => 0.79,
        "rationale" => "model overstated repeated source pressure as a material update"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp stub_story_agent_overstates_repeated_update(%{role: :story_synthesis}, packet, _ctx),
    do: stub_story_synthesis(packet)

  defp stub_story_synthesis(packet) do
    story_key = packet.story_key || "civic-clinic-triage"
    source_ref = packet.source_ref

    %{
      output: %{
        "status" => "complete",
        "title" => %{
          "text" => packet.committed_story_state.title,
          "state" => "complete",
          "provenance_refs" => ["fieldprov:test"]
        },
        "deck" => %{
          "text" => "Civic Clinic triage remains the active story card.",
          "state" => "complete",
          "provenance_refs" => ["fieldprov:test"]
        },
        "summary" => %{
          "text" =>
            "The story synthesis agent linked the source to the clinic triage story and refreshed the card from committed evidence.",
          "state" => "complete",
          "provenance_refs" => ["fieldprov:test"]
        },
        "key_claims" => [
          %{
            "claim_ref" => "claim:#{story_key}:service",
            "text" => "service=triage",
            "status" => "current",
            "materiality" => "material",
            "evidence_refs" => packet.evidence_refs,
            "conflict_refs" => [],
            "uncertainty" => %{"state" => "unavailable", "reason" => "not_supplied"},
            "appears_in_current_card" => true
          }
        ],
        "source_coverage" => [
          %{
            "source_ref" => source_ref,
            "contribution_reason" => %{
              "text" => "source supplies the current triage state",
              "state" => "complete",
              "provenance_refs" => ["fieldprov:test"]
            },
            "materiality" => "material",
            "source_posture" => %{"state" => "unavailable", "reason" => "not_supplied"},
            "source_weight" => %{"state" => "unavailable", "reason" => "not_supplied"}
          }
        ],
        "topic_salience" => %{
          "salience_explanation" => "clinic triage story has local service salience",
          "global_salience" => "source_count",
          "flynn_priority" => "unavailable",
          "durable_topic_nodes" => %{
            "state" => "complete",
            "topic_refs" => ["topic:clinic-triage"],
            "provenance_refs" => ["fieldprov:test"]
          }
        },
        "changed_field_keys" => ["title", "deck", "summary", "source_coverage", "key_claims"],
        "change_summary" => %{
          "text" => "story card refreshed from the latest linked source",
          "state" => "complete",
          "provenance_refs" => ["fieldprov:test"]
        },
        "field_completeness" => %{
          "deck" => "complete",
          "summary" => "complete",
          "canonical_public_url" => "source_level",
          "source_label" => "source_level",
          "publication" => "source_level"
        }
      },
      model: "stub-story-synthesis-agent",
      model_route: "test://story-synthesis",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-synthesis",
      duration_ms: 1
    }
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
