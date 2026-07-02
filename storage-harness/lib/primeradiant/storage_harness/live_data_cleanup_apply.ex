defmodule Primeradiant.StorageHarness.LiveDataCleanupApply do
  @moduledoc false

  alias Primeradiant.StorageHarness.LiveDataCleanupPlan

  @quarantine_table "t1421_story_card_quarantines"

  def apply!(opts) do
    db_path = Keyword.fetch!(opts, :soup_db)
    tenant_id = Keyword.fetch!(opts, :tenant)
    plan_path = Keyword.fetch!(opts, :plan)
    expected_hash = Keyword.fetch!(opts, :plan_hash)
    approved_by = require_nonblank!(opts, :approved_by)
    approval_ref = require_nonblank!(opts, :approval_ref)
    actor = Keyword.get(opts, :actor) || approved_by
    snapshot_hash = Keyword.get(opts, :snapshot_hash)

    plan = plan_path |> File.read!() |> Jason.decode!()
    LiveDataCleanupPlan.verify_plan_hash!(plan, expected_hash)

    current_plan =
      LiveDataCleanupPlan.build(
        soup_db: db_path,
        tenant: tenant_id,
        limit: plan["selection"]["limit"],
        statuses: plan["selection"]["statuses"],
        inserted_before: plan["selection"]["inserted_before"]
      )
      |> put_in(["source_boundary", "soup_db"], plan["source_boundary"]["soup_db"])
      |> Map.put("plan_hash", expected_hash)

    LiveDataCleanupPlan.verify_plan_hash!(current_plan, expected_hash)

    run_id = "t1421-#{System.system_time(:second)}-#{System.unique_integer([:positive])}"
    now = DateTime.utc_now() |> DateTime.to_iso8601()
    affected = plan["affected"]
    card_ids = affected["story_card_version_ids"]
    story_ids = affected["served_soup_story_ids_to_remove_if_applied"]

    sql =
      [
        "PRAGMA foreign_keys = ON;",
        "BEGIN IMMEDIATE;",
        quarantine_table_sql(),
        insert_quarantine_rows_sql(
          plan,
          run_id,
          now,
          tenant_id,
          actor,
          approved_by,
          approval_ref,
          snapshot_hash
        ),
        mark_stories_quarantined_sql(
          story_ids,
          expected_hash,
          run_id,
          now,
          actor,
          card_ids
        ),
        "COMMIT;"
      ]
      |> Enum.join("\n")

    sqlite!(db_path, sql)

    %{
      "ticket" => "T1421",
      "mode" => "apply",
      "run_id" => run_id,
      "plan_hash" => expected_hash,
      "approved_by" => approved_by,
      "approval_ref" => approval_ref,
      "snapshot_hash" => snapshot_hash,
      "quarantined_story_card_version_count" => length(card_ids),
      "quarantined_story_count" => length(story_ids),
      "live_mutation_performed" => true
    }
  end

  defp require_nonblank!(opts, key) do
    case Keyword.fetch!(opts, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "--#{String.replace(to_string(key), "_", "-")} is required"
    end
  end

  defp quarantine_table_sql do
    """
    CREATE TABLE IF NOT EXISTS #{@quarantine_table} (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      plan_hash TEXT NOT NULL,
      story_id TEXT NOT NULL,
      story_card_version_id TEXT NOT NULL,
      status TEXT NOT NULL,
      reason TEXT NOT NULL,
      original_story_json TEXT NOT NULL,
      original_story_card_json TEXT NOT NULL,
      dependent_rows_json TEXT NOT NULL,
      approved_by TEXT NOT NULL,
      approval_ref TEXT NOT NULL,
      actor TEXT NOT NULL,
      snapshot_hash TEXT,
      quarantined_at TEXT NOT NULL,
      UNIQUE (tenant_id, plan_hash, story_card_version_id)
    );
    """
  end

  defp insert_quarantine_rows_sql(
         plan,
         run_id,
         now,
         tenant_id,
         actor,
         approved_by,
         approval_ref,
         snapshot_hash
       ) do
    card_rows = plan["provenance"]["story_card_versions"]
    dependent_rows = Map.drop(plan["provenance"], ["story_card_versions"])
    plan_hash = plan["plan_hash"]

    Enum.map_join(card_rows, "\n", fn card ->
      card_id = card["id"]
      story_id = card["story_id"]
      id = "#{run_id}:#{card_id}"

      """
      INSERT INTO #{@quarantine_table}
        (id, tenant_id, run_id, plan_hash, story_id, story_card_version_id, status, reason,
         original_story_json, original_story_card_json, dependent_rows_json, approved_by,
         approval_ref, actor, snapshot_hash, quarantined_at)
      SELECT #{quote_sql(id)}, #{quote_sql(tenant_id)}, #{quote_sql(run_id)}, #{quote_sql(plan_hash)},
             c.story_id, c.id, c.status, coalesce(json_extract(c.provenance, '$.reason'), c.refresh_reason),
             json_object(
               'id', s.id,
               'tenant_id', s.tenant_id,
               'story_key', s.story_key,
               'title', s.title,
               'state', s.state,
               'version', s.version,
               'first_observed_at', s.first_observed_at,
               'updated_at_story', s.updated_at_story,
               'last_material_at', s.last_material_at,
               'structural_facts', json(s.structural_facts),
               'background_facts', json(s.background_facts),
               'colors', json(s.colors),
               'questions', json(s.questions),
               'topic_tokens', json(s.topic_tokens),
               'attrs', json(s.attrs),
               'inserted_at', s.inserted_at,
               'updated_at', s.updated_at
             ),
             json_object(
               'id', c.id,
               'tenant_id', c.tenant_id,
               'story_id', c.story_id,
               'story_version', c.story_version,
               'card_version', c.card_version,
               'status', c.status,
               'supersedes_id', c.supersedes_id,
               'refresh_reason', c.refresh_reason,
               'producing_agent_run_id', c.producing_agent_run_id,
               'packet_hash', c.packet_hash,
               'prompt_config_hash', c.prompt_config_hash,
               'output_hash', c.output_hash,
               'field_provenance_manifest_id', c.field_provenance_manifest_id,
               'title', json(c.title),
               'deck', json(c.deck),
               'summary', json(c.summary),
               'freshness', json(c.freshness),
               'field_completeness', json(c.field_completeness),
               'topic_salience', json(c.topic_salience),
               'provenance', json(c.provenance),
               'inserted_at', c.inserted_at,
               'updated_at', c.updated_at
             ),
             #{quote_sql(Jason.encode!(dependent_rows_for_card(dependent_rows, card_id, story_id)))},
             #{quote_sql(approved_by)}, #{quote_sql(approval_ref)}, #{quote_sql(actor)},
             #{quote_sql(snapshot_hash)}, #{quote_sql(now)}
      FROM story_card_versions c
      JOIN stories s ON s.tenant_id = c.tenant_id AND s.id = c.story_id
      WHERE c.tenant_id = #{quote_sql(tenant_id)}
        AND c.id = #{quote_sql(card_id)}
        AND c.story_id = #{quote_sql(story_id)}
        AND c.status IN ('incomplete', 'refused');
      """
    end)
  end

  defp dependent_rows_for_card(dependent_rows, card_id, story_id) do
    Map.new(dependent_rows, fn {key, rows} ->
      {key,
       Enum.filter(rows, fn row ->
         row["story_card_version_id"] == card_id or
           row["new_card_version_id"] == card_id or
           row["current_card_version_id"] == card_id or
           row["story_id"] == story_id
       end)}
    end)
  end

  defp mark_stories_quarantined_sql([], _plan_hash, _run_id, _now, _actor, _card_ids), do: ""

  defp mark_stories_quarantined_sql(story_ids, plan_hash, run_id, now, actor, card_ids) do
    marker =
      Jason.encode!(%{
        "ticket" => "T1421",
        "active_product_truth" => false,
        "plan_hash" => plan_hash,
        "run_id" => run_id,
        "quarantined_at" => now,
        "actor" => actor,
        "story_card_version_ids" => card_ids,
        "reason" => "unfixable_story_card_not_complete"
      })

    """
    UPDATE stories
    SET attrs = json_set(attrs, '$.t1421_quarantine', json(#{quote_sql(marker)})),
        updated_at = #{quote_sql(now)}
    WHERE id IN (#{quoted_csv(story_ids)})
      AND attrs NOT LIKE '%"t1421_quarantine"%';
    """
  end

  defp sqlite!(db_path, sql) do
    case System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "sqlite T1421 cleanup apply failed #{status}: #{output}"
    end
  end

  defp quoted_csv(values), do: values |> Enum.map(&quote_sql/1) |> Enum.join(",")

  defp quote_sql(nil), do: "NULL"
  defp quote_sql(value), do: "'#{value |> to_string() |> String.replace("'", "''")}'"
end
