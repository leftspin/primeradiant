defmodule Primeradiant.LiveDataCleanupPlanTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Soup
  alias Primeradiant.StorageHarness.{DurableSoupDb, LiveDataCleanupApply, LiveDataCleanupPlan}

  @tenant "00000000-0000-0000-0000-000000001421"

  test "dry-run plan enumerates non-complete current cards and zero-article stories" do
    db_path = tmp_db_path()
    create_minimal_soup!(db_path)

    plan =
      LiveDataCleanupPlan.build(
        soup_db: db_path,
        tenant: @tenant,
        limit: 80,
        inserted_before: "2026-06-20T00:00:00Z"
      )

    assert plan["mode"] == "dry_run_only"
    assert plan["source_boundary"]["live_mutation_performed"] == false
    assert plan["counts"]["story_card_version_count"] == 2
    assert plan["affected"]["story_card_version_ids"] == ["card-incomplete", "card-refused"]
    assert plan["affected"]["story_source_coverage_ids"] == ["coverage-incomplete"]

    assert plan["affected"]["served_soup_story_ids_to_remove_if_applied"] == [
             "story-incomplete",
             "story-refused"
           ]

    assert plan["actions"]["preserve_raw_source_and_evidence"] == true
    assert String.length(plan["plan_hash"]) == 64
  end

  test "plan hash is stable for the same snapshot and parameters" do
    db_path = tmp_db_path()
    create_minimal_soup!(db_path)

    first = LiveDataCleanupPlan.build(soup_db: db_path, tenant: @tenant, limit: 80)
    second = LiveDataCleanupPlan.build(soup_db: db_path, tenant: @tenant, limit: 80)

    assert first["plan_hash"] == second["plan_hash"]
  end

  test "apply refuses a plan hash mismatch before mutation" do
    db_path = tmp_db_path()
    create_minimal_soup!(db_path)
    plan_path = write_plan!(db_path)

    assert_raise ArgumentError, ~r/plan hash mismatch/, fn ->
      LiveDataCleanupApply.apply!(
        soup_db: db_path,
        tenant: @tenant,
        plan: plan_path,
        plan_hash: String.duplicate("0", 64),
        approved_by: "operator",
        approval_ref: "approval-1"
      )
    end

    assert sqlite_count(db_path, "sqlite_master", "name = 't1421_story_card_quarantines'") == 0
    assert sqlite_count(db_path, "stories", "tenant_id = '#{@tenant}'") == 3
  end

  test "apply requires operator approval fields before mutation" do
    db_path = tmp_db_path()
    create_minimal_soup!(db_path)
    plan_path = write_plan!(db_path)
    plan = plan_path |> File.read!() |> Jason.decode!()

    assert_raise KeyError, fn ->
      LiveDataCleanupApply.apply!(
        soup_db: db_path,
        tenant: @tenant,
        plan: plan_path,
        plan_hash: plan["plan_hash"]
      )
    end

    assert sqlite_count(db_path, "sqlite_master", "name = 't1421_story_card_quarantines'") == 0
  end

  test "apply quarantines exact plan rows and preserves provenance without broad deletion" do
    db_path = tmp_db_path()
    create_minimal_soup!(db_path)
    plan_path = write_plan!(db_path)
    plan = plan_path |> File.read!() |> Jason.decode!()

    report =
      LiveDataCleanupApply.apply!(
        soup_db: db_path,
        tenant: @tenant,
        plan: plan_path,
        plan_hash: plan["plan_hash"],
        approved_by: "operator",
        approval_ref: "approval-1",
        actor: "agent",
        snapshot_hash: "snapshot-hash"
      )

    assert report["live_mutation_performed"] == true
    assert report["quarantined_story_card_version_count"] == 2
    assert sqlite_count(db_path, "t1421_story_card_quarantines", "tenant_id = '#{@tenant}'") == 2
    assert sqlite_count(db_path, "story_card_versions", "tenant_id = '#{@tenant}'") == 3
    assert sqlite_count(db_path, "stories", "tenant_id = '#{@tenant}'") == 3

    rows =
      sqlite_json_rows!(
        db_path,
        "SELECT story_id, story_card_version_id, status, original_story_json, original_story_card_json, dependent_rows_json FROM t1421_story_card_quarantines ORDER BY story_card_version_id;"
      )

    assert Enum.map(rows, & &1["story_card_version_id"]) == ["card-incomplete", "card-refused"]
    assert Enum.all?(rows, &(&1["status"] in ["incomplete", "refused"]))

    incomplete = Enum.find(rows, &(&1["story_card_version_id"] == "card-incomplete"))
    assert Jason.decode!(incomplete["original_story_json"])["id"] == "story-incomplete"
    assert Jason.decode!(incomplete["original_story_card_json"])["status"] == "incomplete"

    dependent_rows = Jason.decode!(incomplete["dependent_rows_json"])
    assert [%{"id" => "coverage-incomplete"}] = dependent_rows["story_source_coverage"]

    state = DurableSoupDb.load_soup_projection(db_path, @tenant)

    feed =
      Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning", "limit" => 10})

    assert Enum.map(feed.items, & &1.story_id) == ["story-complete"]
    assert hd(feed.items).status == "complete"
  end

  defp tmp_db_path do
    Path.join(
      System.tmp_dir!(),
      "primeradiant-t1421-cleanup-plan-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
    )
  end

  defp create_minimal_soup!(db_path) do
    DurableSoupDb.persist!(db_path, Primeradiant.StorageHarness.State.new(tenant_id: @tenant), %{
      source_kind: "t1421-cleanup-plan-test",
      source_db_path: "test",
      source_row_count: 0
    })

    sql = """
    INSERT INTO stories (id, tenant_id, story_key, title, state, version, first_observed_at, updated_at_story, structural_facts, background_facts, colors, questions, topic_tokens, attrs, inserted_at, updated_at)
    VALUES
      ('story-incomplete', '#{@tenant}', 'story-incomplete', 'Incomplete story', 'active', 1, '2026-06-14T00:00:00Z', '2026-06-15T03:00:00Z', '{}', '{}', '[]', '{}', '[]', '{}', '2026-06-14T00:00:00Z', '2026-06-15T03:00:00Z'),
      ('story-refused', '#{@tenant}', 'story-refused', 'Refused story', 'active', 1, '2026-06-14T00:00:00Z', '2026-06-15T02:00:00Z', '{}', '{}', '[]', '{}', '[]', '{}', '2026-06-14T00:00:00Z', '2026-06-15T02:00:00Z'),
      ('story-complete', '#{@tenant}', 'story-complete', 'Complete story', 'active', 1, '2026-06-14T00:00:00Z', '2026-06-15T01:00:00Z', '{}', '{}', '[]', '{}', '[]', '{}', '2026-06-14T00:00:00Z', '2026-06-15T01:00:00Z');

    INSERT INTO agent_runs (id, tenant_id, agent_run_key, agent_type, prompt_version, model, scope, status, trace_id, started_at, ended_at, inserted_at, updated_at)
    VALUES
      ('agent-run-t1421', '#{@tenant}', 'agent-run-key-t1421', 'story_synthesis', 'test', 'test', '{}', 'succeeded', 'trace-t1421', '2026-06-15T00:00:00Z', '2026-06-15T00:00:01Z', '2026-06-15T00:00:00Z', '2026-06-15T00:00:01Z');

    INSERT INTO story_card_versions
      (id, tenant_id, story_id, story_version, card_version, status, supersedes_id, refresh_reason, producing_agent_run_id, packet_hash, prompt_config_hash, output_hash, field_provenance_manifest_id, title, deck, summary, freshness, field_completeness, topic_salience, provenance, inserted_at, updated_at)
    VALUES
      ('card-incomplete', '#{@tenant}', 'story-incomplete', 1, 1, 'incomplete', NULL, 'story_card_hourly_synthesis', 'agent-run-t1421', 'packet-1', 'prompt-1', 'output-1', 'fieldprov-1', '{}', '{}', '{}', '{}', '{"overall":"incomplete"}', '{"state":"unavailable"}', '{"reason":"evidence_limited"}', '2026-06-15T00:00:00Z', '2026-06-15T00:00:00Z'),
      ('card-refused', '#{@tenant}', 'story-refused', 1, 1, 'refused', NULL, 'story_card_hourly_synthesis', 'agent-run-t1421', 'packet-2', 'prompt-1', 'output-2', 'fieldprov-2', '{}', '{}', '{}', '{}', '{"overall":"refused"}', '{"state":"refused"}', '{"reason":"model_refused"}', '2026-06-15T00:00:00Z', '2026-06-15T00:00:00Z'),
      ('card-complete', '#{@tenant}', 'story-complete', 1, 1, 'complete', NULL, 'story_card_hourly_synthesis', 'agent-run-t1421', 'packet-3', 'prompt-1', 'output-3', 'fieldprov-3', '{}', '{}', '{}', '{}', '{"overall":"complete"}', '{"state":"available"}', '{}', '2026-06-15T00:00:00Z', '2026-06-15T00:00:00Z');

    INSERT INTO story_source_coverage
      (id, tenant_id, story_id, story_card_version_id, source_ref, article_ref, canonical_public_url, source_domain, source_label, publication, source_posture, contribution_reason, materiality, source_weight, first_observed_at, last_observed_at, evidence_refs, provenance_refs, inserted_at, updated_at)
    VALUES
      ('coverage-incomplete', '#{@tenant}', 'story-incomplete', 'card-incomplete', 'source-1', 'article-1', '{}', '{}', '{}', '{}', '{}', '{}', 'material', '{}', '2026-06-14T00:00:00Z', '2026-06-15T00:00:00Z', '["evidence-1"]', '["fieldprov-1"]', '2026-06-15T00:00:00Z', '2026-06-15T00:00:00Z'),
      ('coverage-complete', '#{@tenant}', 'story-complete', 'card-complete', 'source-3', 'article-3', '{}', '{}', '{}', '{}', '{}', '{}', 'material', '{}', '2026-06-14T00:00:00Z', '2026-06-15T00:00:00Z', '["evidence-3"]', '["fieldprov-3"]', '2026-06-15T00:00:00Z', '2026-06-15T00:00:00Z');
    """

    {_, 0} = System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true)
  end

  defp write_plan!(db_path) do
    plan = LiveDataCleanupPlan.build(soup_db: db_path, tenant: @tenant, limit: 80)

    path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-t1421-plan-#{System.unique_integer([:positive])}.json"
      )

    File.write!(path, Jason.encode!(plan))
    path
  end

  defp sqlite_count(db_path, table, where) do
    sql = "SELECT count(*) FROM #{table} WHERE #{where};"
    {count, 0} = System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true)
    count |> String.trim() |> String.to_integer()
  end

  defp sqlite_json_rows!(db_path, sql) do
    case System.cmd("sqlite3", ["file:#{db_path}?mode=ro", "-json", sql], stderr_to_stdout: true) do
      {"", 0} -> []
      {json, 0} -> Jason.decode!(json)
    end
  end
end
