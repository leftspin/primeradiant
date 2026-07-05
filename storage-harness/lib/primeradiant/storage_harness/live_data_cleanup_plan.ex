defmodule Primeradiant.StorageHarness.LiveDataCleanupPlan do
  @moduledoc false

  @default_statuses ~w(incomplete refused)

  def build(opts) do
    db_path = Keyword.fetch!(opts, :soup_db)
    tenant_id = Keyword.fetch!(opts, :tenant)
    limit = Keyword.get(opts, :limit, 80)
    statuses = Keyword.get(opts, :statuses, @default_statuses)
    inserted_before = Keyword.get(opts, :inserted_before)
    selected_story_ids = opts |> Keyword.get(:story_ids, []) |> normalize_ids()

    cards =
      candidate_cards(db_path, tenant_id, limit, statuses, inserted_before, selected_story_ids)

    card_ids = Enum.map(cards, & &1["id"])
    story_ids = cards |> Enum.map(& &1["story_id"]) |> Enum.uniq() |> Enum.sort()

    coverage =
      dependent_rows(
        db_path,
        tenant_id,
        "story_source_coverage",
        "story_card_version_id",
        card_ids
      )

    claims =
      dependent_rows(db_path, tenant_id, "story_key_claims", "story_card_version_id", card_ids)

    changes =
      dependent_rows(
        db_path,
        tenant_id,
        "story_card_change_sets",
        "new_card_version_id",
        card_ids
      )

    reader_deltas =
      dependent_rows(
        db_path,
        tenant_id,
        "story_reader_deltas",
        "current_card_version_id",
        card_ids
      )

    stories_without_articles = stories_without_articles(db_path, tenant_id, story_ids, card_ids)
    provenance = provenance_rows(cards, coverage, claims, changes, reader_deltas)

    affected = %{
      "story_card_version_ids" => card_ids,
      "story_source_coverage_ids" => ids(coverage),
      "story_key_claim_ids" => ids(claims),
      "story_card_change_set_ids" => ids(changes),
      "story_reader_delta_ids" => ids(reader_deltas),
      "story_ids" => story_ids,
      "served_soup_story_ids_to_remove_if_applied" => stories_without_articles
    }

    body = %{
      "ticket" => "T1421",
      "mode" => "dry_run_only",
      "source_boundary" => %{
        "soup_db" => db_path,
        "tenant_id" => tenant_id,
        "snapshot_required_before_live_apply" => true,
        "operator_approval_required_before_live_apply" => true,
        "live_mutation_performed" => false
      },
      "selection" => %{
        "consumer" => "reporter",
        "projection" => "news-morning",
        "limit" => limit,
        "statuses" => statuses,
        "inserted_before" => inserted_before,
        "story_ids" => selected_story_ids,
        "order" => "story updated_at_story desc, story id asc, current card_version desc"
      },
      "counts" => count_map(affected),
      "affected" => affected,
      "actions" => %{
        "preserve_raw_source_and_evidence" => true,
        "delete_or_quarantine_active_story_card_product_rows" => card_ids,
        "tombstone_or_invalidate_contaminated_story_source_mapping_rows" => ids(coverage),
        "remove_from_served_soup_when_zero_attached_articles" => stories_without_articles,
        "do_not_fetch_import_ingest_or_invent_replacements" => true
      },
      "provenance" => provenance
    }

    Map.put(body, "plan_hash", stable_hash(body))
  end

  def verify_plan_hash!(%{} = plan, expected_hash) when is_binary(expected_hash) do
    actual_hash = stable_hash(Map.delete(plan, "plan_hash"))
    embedded_hash = Map.fetch!(plan, "plan_hash")

    cond do
      embedded_hash != expected_hash ->
        raise ArgumentError,
              "plan hash mismatch: expected #{expected_hash}, plan contains #{embedded_hash}"

      actual_hash != expected_hash ->
        raise ArgumentError,
              "plan hash mismatch: expected #{expected_hash}, recomputed #{actual_hash}"

      true ->
        :ok
    end
  end

  defp candidate_cards(db_path, tenant_id, limit, statuses, inserted_before, selected_story_ids) do
    status_sql = statuses |> Enum.map(&quote_sql/1) |> Enum.join(",")

    inserted_filter =
      case inserted_before do
        nil -> ""
        value -> "AND c.inserted_at < #{quote_sql(value)}"
      end

    quarantine_filter =
      if table_exists?(db_path, "t1421_story_card_quarantines") do
        """
        AND NOT EXISTS (
          SELECT 1
          FROM t1421_story_card_quarantines q
          WHERE q.tenant_id = c.tenant_id
            AND q.story_card_version_id = c.id
        )
        """
      else
        ""
      end

    top_stories_sql =
      case selected_story_ids do
        [] ->
          """
          SELECT s.id AS story_id
          FROM stories s
          JOIN current_cards c ON c.story_id = s.id
          WHERE s.tenant_id = #{quote_sql(tenant_id)}
          ORDER BY s.updated_at_story DESC, s.id ASC
          LIMIT #{limit}
          """

        story_ids ->
          """
          SELECT s.id AS story_id
          FROM stories s
          WHERE s.tenant_id = #{quote_sql(tenant_id)}
            AND s.id IN (#{quoted_csv(story_ids)})
          ORDER BY s.id ASC
          """
      end

    sql = """
    WITH current_cards AS (
      SELECT c.*
      FROM story_card_versions c
      JOIN (
        SELECT story_id, max(card_version) AS max_card_version
        FROM story_card_versions
        WHERE tenant_id = #{quote_sql(tenant_id)}
        GROUP BY story_id
      ) latest
        ON latest.story_id = c.story_id
       AND latest.max_card_version = c.card_version
      WHERE c.tenant_id = #{quote_sql(tenant_id)}
    ),
    top_stories AS (
      #{top_stories_sql}
    )
    SELECT c.id, c.story_id, c.card_version, c.status, c.refresh_reason,
           c.producing_agent_run_id, c.packet_hash, c.prompt_config_hash,
           c.output_hash, c.field_provenance_manifest_id, c.field_completeness,
           c.topic_salience, c.provenance, c.inserted_at, c.updated_at
    FROM current_cards c
    JOIN top_stories t ON t.story_id = c.story_id
    WHERE c.status IN (#{status_sql})
    #{inserted_filter}
    #{quarantine_filter}
    ORDER BY c.story_id ASC, c.card_version DESC, c.id ASC;
    """

    query_json!(db_path, sql)
  end

  defp dependent_rows(_db_path, _tenant_id, _table, _field, []), do: []

  defp dependent_rows(db_path, tenant_id, table, field, ids) do
    sql = """
    SELECT *
    FROM #{table}
    WHERE tenant_id = #{quote_sql(tenant_id)}
      AND #{field} IN (#{quoted_csv(ids)})
    ORDER BY story_id ASC, id ASC;
    """

    query_json!(db_path, sql)
  end

  defp stories_without_articles(_db_path, _tenant_id, [], _card_ids), do: []

  defp stories_without_articles(db_path, tenant_id, story_ids, card_ids) do
    sql = """
    SELECT s.id
    FROM stories s
    WHERE s.tenant_id = #{quote_sql(tenant_id)}
      AND s.id IN (#{quoted_csv(story_ids)})
      AND NOT EXISTS (
        SELECT 1
        FROM story_source_coverage sc
        WHERE sc.tenant_id = s.tenant_id
          AND sc.story_id = s.id
          AND coalesce(sc.article_ref, '') != ''
          AND sc.story_card_version_id NOT IN (#{quoted_csv(card_ids)})
      )
    ORDER BY s.id ASC;
    """

    db_path
    |> query_json!(sql)
    |> Enum.map(& &1["id"])
  end

  defp provenance_rows(cards, coverage, claims, changes, reader_deltas) do
    %{
      "story_card_versions" =>
        Enum.map(cards, fn card ->
          Map.take(card, [
            "id",
            "story_id",
            "status",
            "refresh_reason",
            "producing_agent_run_id",
            "packet_hash",
            "prompt_config_hash",
            "output_hash",
            "field_provenance_manifest_id",
            "field_completeness",
            "topic_salience",
            "provenance"
          ])
        end),
      "story_source_coverage" =>
        Enum.map(coverage, fn row ->
          Map.take(row, [
            "id",
            "story_id",
            "story_card_version_id",
            "source_ref",
            "article_ref",
            "evidence_refs",
            "provenance_refs"
          ])
        end),
      "story_key_claims" =>
        Enum.map(
          claims,
          &Map.take(&1, [
            "id",
            "story_id",
            "story_card_version_id",
            "claim_ref",
            "evidence_refs",
            "conflict_refs"
          ])
        ),
      "story_card_change_sets" =>
        Enum.map(
          changes,
          &Map.take(&1, [
            "id",
            "story_id",
            "prior_card_version_id",
            "new_card_version_id",
            "changed_field_keys",
            "change_summary"
          ])
        ),
      "story_reader_deltas" =>
        Enum.map(
          reader_deltas,
          &Map.take(&1, [
            "id",
            "story_id",
            "current_card_version_id",
            "evidence_refs",
            "provenance_refs"
          ])
        )
    }
  end

  defp table_exists?(db_path, table) do
    sql = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name = #{quote_sql(table)}
    LIMIT 1;
    """

    query_json!(db_path, sql) != []
  end

  defp query_json!(db_path, sql) do
    args = ["file:#{db_path}?mode=ro", "-json", sql]

    case System.cmd("sqlite3", args, stderr_to_stdout: true) do
      {"", 0} -> []
      {json, 0} -> Jason.decode!(json)
      {output, status} -> raise "sqlite cleanup plan query failed #{status}: #{output}"
    end
  end

  defp ids(rows), do: rows |> Enum.map(& &1["id"]) |> Enum.sort()

  defp count_map(affected) do
    Map.new(affected, fn {key, value} ->
      {String.replace_suffix(key, "ids", "count"), length(value)}
    end)
  end

  defp stable_hash(plan) do
    plan
    |> canonicalize()
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
    |> Enum.map(fn {key, value} -> {key, canonicalize(value)} end)
    |> Jason.OrderedObject.new()
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value), do: value

  defp normalize_ids(nil), do: []

  defp normalize_ids(ids) when is_list(ids) do
    ids
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp quoted_csv(values), do: values |> Enum.map(&quote_sql/1) |> Enum.join(",")
  defp quote_sql(value), do: "'#{value |> to_string() |> String.replace("'", "''")}'"
end
