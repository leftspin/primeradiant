defmodule Primeradiant.LiveDataCleanupPlanTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.{DurableSoupDb, LiveDataCleanupPlan}

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
end
