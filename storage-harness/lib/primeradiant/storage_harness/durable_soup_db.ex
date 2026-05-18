defmodule Primeradiant.StorageHarness.DurableSoupDb do
  @moduledoc false

  @tables [
    :evidence_refs,
    :conflicts,
    :story_events,
    :story_fact_versions,
    :edges,
    :soup_nodes,
    :graph_commits,
    :proposal_decisions,
    :proposal_ops,
    :proposals,
    :agent_runs,
    :inputs,
    :stories
  ]

  def persist!(db_path, state, attrs \\ %{}) do
    db_path |> Path.dirname() |> File.mkdir_p!()

    sql =
      [
        "PRAGMA foreign_keys = ON;",
        schema_sql(),
        clear_tenant_sql(state.tenant_id),
        replay_run_sql(state, attrs),
        rows_sql(:agent_runs, state.agent_runs),
        rows_sql(:inputs, state.inputs),
        rows_sql(:stories, state.stories),
        rows_sql(:proposals, state.proposals),
        rows_sql(:proposal_ops, state.proposal_ops),
        rows_sql(:proposal_decisions, state.proposal_decisions),
        rows_sql(:graph_commits, state.graph_commits),
        rows_sql(:soup_nodes, state.soup_nodes),
        rows_sql(:edges, state.edges),
        rows_sql(:story_fact_versions, state.story_fact_versions),
        rows_sql(:story_events, state.story_events),
        rows_sql(:conflicts, state.conflicts),
        rows_sql(:evidence_refs, state.evidence_refs)
      ]
      |> Enum.join("\n")

    sqlite!(db_path, sql)
    :ok
  end

  def changed_stories_report(db_path, tenant_id, source_summary, ingestion_report) do
    changed_stories =
      db_path
      |> query_json("""
      SELECT json_object(
        'story_id', stories.id,
        'story_key', stories.story_key,
        'title', stories.title,
        'state', stories.state,
        'version', stories.version,
        'changed_event_count', COUNT(story_events.id),
        'current_facts', stories.structural_facts
      )
      FROM stories
      JOIN story_events ON story_events.story_id = stories.id
      WHERE stories.tenant_id = #{sql_quote(tenant_id)}
        AND story_events.classification <> 'no_op'
      GROUP BY stories.id
      ORDER BY stories.updated_at_story, stories.story_key;
      """)
      |> Enum.map(&put_story_evidence(&1, db_path, tenant_id))

    %{
      source: source_summary,
      primeradiant_writes: %{
        owned_state_only: true,
        durable: true,
        soup_db_path: db_path,
        inputs: count(db_path, "inputs", tenant_id),
        stories: count(db_path, "stories", tenant_id),
        proposals: count(db_path, "proposals", tenant_id),
        proposal_ops: count(db_path, "proposal_ops", tenant_id),
        graph_commits: count(db_path, "graph_commits", tenant_id),
        evidence_refs: count(db_path, "evidence_refs", tenant_id)
      },
      extraction_quality: extraction_quality_report(db_path, tenant_id),
      ingestion: ingestion_report,
      changed_stories: changed_stories
    }
  end

  def table_count(db_path, table, tenant_id), do: count(db_path, table, tenant_id)

  defp extraction_quality_report(db_path, tenant_id) do
    counts =
      query_json(db_path, """
      SELECT json_object(
        'status', COALESCE(json_extract(normalized, '$.production_extractor_v1.quality.status'), 'unknown'),
        'count', COUNT(*)
      )
      FROM inputs
      WHERE tenant_id = #{sql_quote(tenant_id)}
      GROUP BY COALESCE(json_extract(normalized, '$.production_extractor_v1.quality.status'), 'unknown')
      ORDER BY COALESCE(json_extract(normalized, '$.production_extractor_v1.quality.status'), 'unknown');
      """)

    refused_inputs =
      query_json(db_path, """
      SELECT json_object(
        'input_id', id,
        'external_id', external_id,
        'title', title,
        'status', json_extract(normalized, '$.production_extractor_v1.quality.status'),
        'reasons', json_extract(normalized, '$.production_extractor_v1.quality.reasons'),
        'warnings', json_extract(normalized, '$.production_extractor_v1.quality.warnings')
      )
      FROM inputs
      WHERE tenant_id = #{sql_quote(tenant_id)}
        AND json_extract(normalized, '$.production_extractor_v1.quality.status') IN ('refused', 'low_confidence')
      ORDER BY observed_at, external_id
      LIMIT 25;
      """)
      |> Enum.map(fn row ->
        row
        |> Map.update("reasons", [], &decode_json(&1, []))
        |> Map.update("warnings", [], &decode_json(&1, []))
      end)

    %{
      counts: counts,
      refused_or_low_confidence_sample: refused_inputs
    }
  end

  defp put_story_evidence(story, db_path, tenant_id) do
    story_id = story["story_id"]

    evidence =
      query_json(db_path, """
      SELECT json_object(
        'subject_type', evidence_refs.subject_type,
        'subject_id', evidence_refs.subject_id,
        'input_id', evidence_refs.input_id,
        'external_id', inputs.external_id,
        'label', evidence_refs.evidence_label,
        'span_start', evidence_refs.span_start,
        'span_end', evidence_refs.span_end
      )
      FROM evidence_refs
      JOIN inputs ON inputs.id = evidence_refs.input_id
      WHERE evidence_refs.tenant_id = #{sql_quote(tenant_id)}
        AND (
          evidence_refs.subject_id IN (
            SELECT id FROM story_events WHERE story_id = #{sql_quote(story_id)}
            UNION
            SELECT id FROM story_fact_versions WHERE story_id = #{sql_quote(story_id)}
          )
        )
      ORDER BY evidence_refs.subject_type, evidence_refs.subject_id, evidence_refs.evidence_label;
      """)

    story
    |> Map.put("current_facts", decode_json(story["current_facts"], %{}))
    |> Map.put("evidence", group_evidence(evidence))
  end

  defp group_evidence(evidence_rows) do
    evidence_rows
    |> Enum.group_by(&{&1["subject_type"], &1["subject_id"]})
    |> Enum.map(fn {{subject_type, subject_id}, refs} ->
      %{
        subject_type: subject_type,
        subject_id: subject_id,
        evidence_refs:
          Enum.map(refs, fn ref ->
            %{
              input_id: ref["input_id"],
              external_id: ref["external_id"],
              label: ref["label"],
              span_start: ref["span_start"],
              span_end: ref["span_end"]
            }
          end)
      }
    end)
  end

  defp count(db_path, table, tenant_id) do
    db_path
    |> sqlite!("SELECT COUNT(*) FROM #{table} WHERE tenant_id = #{sql_quote(tenant_id)};")
    |> String.trim()
    |> String.to_integer()
  end

  defp query_json(db_path, sql) do
    db_path
    |> sqlite!(sql)
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
  end

  defp clear_tenant_sql(tenant_id) do
    @tables
    |> Enum.map(fn table -> "DELETE FROM #{table} WHERE tenant_id = #{sql_quote(tenant_id)};" end)
    |> Enum.join("\n")
  end

  defp replay_run_sql(state, attrs) do
    insert_sql(:replay_runs, %{
      id: Ecto.UUID.generate(),
      tenant_id: state.tenant_id,
      source_kind: Map.fetch!(attrs, :source_kind),
      source_db_path: Map.fetch!(attrs, :source_db_path),
      source_row_count: Map.fetch!(attrs, :source_row_count),
      source_mode: "read_only",
      inserted_at: now()
    })
  end

  defp rows_sql(table, rows),
    do: Enum.map_join(rows, "\n", &insert_sql(table, row_map(table, &1)))

  defp row_map(:agent_runs, row) do
    take(
      row,
      ~w(id tenant_id agent_run_key agent_type prompt_version model scope status trace_id started_at ended_at inserted_at updated_at)a
    )
  end

  defp row_map(:inputs, row) do
    take(
      row,
      ~w(id tenant_id fixture_id source_type external_id observed_at title body_text object_uri content_sha256 acl normalized facts background questions colors topic_tokens inserted_at updated_at)a
    )
  end

  defp row_map(:stories, row) do
    take(
      row,
      ~w(id tenant_id story_key title state version first_observed_at updated_at_story last_material_at structural_facts background_facts colors questions topic_tokens attrs inserted_at updated_at)a
    )
  end

  defp row_map(:proposals, row) do
    take(
      row,
      ~w(id tenant_id proposal_key agent_run_id actor_id story_id fixture_id classification confidence rationale status inserted_at updated_at)a
    )
  end

  defp row_map(:proposal_ops, row) do
    take(
      row,
      ~w(id tenant_id proposal_id position op_type payload evidence_refs confidence status committed_at inserted_at updated_at)a
    )
  end

  defp row_map(:proposal_decisions, row) do
    take(
      row,
      ~w(id tenant_id proposal_id from_status to_status actor_type actor_id evidence_refs confidence rationale inserted_at)a
    )
  end

  defp row_map(:graph_commits, row) do
    take(
      row,
      ~w(id tenant_id proposal_id proposal_op_id commit_type committed_by_type committed_by_id evidence_refs confidence inserted_at)a
    )
  end

  defp row_map(:soup_nodes, row) do
    take(
      row,
      ~w(id tenant_id node_key node_type title state input_id story_id watch_id authored_output_id proposal_id proposal_op_id graph_commit_id confidence attrs inserted_at updated_at)a
    )
  end

  defp row_map(:edges, row) do
    take(
      row,
      ~w(id tenant_id from_node_id to_node_id edge_type status confidence proposal_id proposal_op_id graph_commit_id attrs inserted_at updated_at)a
    )
  end

  defp row_map(:story_fact_versions, row) do
    take(
      row,
      ~w(id tenant_id story_id claim_node_id fact_key fact_value time_scope status proposal_id proposal_op_id graph_commit_id input_id confidence replaced_fact_version_id observed_at inserted_at)a
    )
  end

  defp row_map(:story_events, row) do
    take(
      row,
      ~w(id tenant_id story_id input_id classification story_version changed_facts observed_at proposal_id proposal_op_id graph_commit_id confidence inserted_at)a
    )
  end

  defp row_map(:conflicts, row) do
    take(
      row,
      ~w(id tenant_id story_id fact_key prior_value incoming_value status input_id proposal_id proposal_op_id graph_commit_id agent_run_id confidence inserted_at updated_at)a
    )
  end

  defp row_map(:evidence_refs, row) do
    take(
      row,
      ~w(id tenant_id subject_type subject_id input_id soup_node_id proposal_id proposal_op_id edge_id conflict_id authored_output_id authored_output_unit_id span_start span_end evidence_label evidence_hash inserted_at)a
    )
  end

  defp take(row, fields) do
    Map.new(fields, fn field -> {field, Map.get(row, field)} end)
  end

  defp insert_sql(table, attrs) do
    attrs = fill_storage_timestamps(attrs)
    columns = Map.keys(attrs)
    values = Enum.map(columns, &sql_value(Map.fetch!(attrs, &1)))

    "INSERT OR REPLACE INTO #{table} (#{Enum.join(columns, ", ")}) VALUES (#{Enum.join(values, ", ")});"
  end

  defp fill_storage_timestamps(attrs) do
    inserted_at = Map.get(attrs, :inserted_at) || now()

    attrs
    |> put_existing_timestamp(:inserted_at, inserted_at)
    |> put_existing_timestamp(:updated_at, inserted_at)
  end

  defp put_existing_timestamp(attrs, key, value) do
    if Map.has_key?(attrs, key) do
      Map.update!(attrs, key, &(&1 || value))
    else
      attrs
    end
  end

  defp sql_value(nil), do: "NULL"
  defp sql_value(%Decimal{} = value), do: sql_quote(Decimal.to_string(value, :normal))
  defp sql_value(%DateTime{} = value), do: sql_quote(DateTime.to_iso8601(value))
  defp sql_value(value) when is_binary(value), do: sql_quote(value)
  defp sql_value(value) when is_integer(value), do: Integer.to_string(value)
  defp sql_value(value) when is_float(value), do: Float.to_string(value)
  defp sql_value(value) when is_boolean(value), do: if(value, do: "1", else: "0")

  defp sql_value(value) when is_list(value) or is_map(value),
    do: sql_quote(Jason.encode!(json_safe(value)))

  defp sql_quote(value), do: "'#{String.replace(to_string(value), "'", "''")}'"

  defp json_safe(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe(%_struct{} = value), do: value |> Map.from_struct() |> json_safe()

  defp json_safe(value) when is_map(value),
    do: Map.new(value, fn {key, value} -> {to_string(key), json_safe(value)} end)

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value), do: value

  defp decode_json(nil, default), do: default
  defp decode_json("", default), do: default

  defp decode_json(value, _default) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decoded
      {:error, _error} -> [value]
    end
  end

  defp decode_json(value, _default) when is_list(value) or is_map(value), do: value
  defp decode_json(_value, default), do: default

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

  defp sqlite!(db_path, sql) do
    sql_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-sql-#{System.unique_integer([:positive])}.sql"
      )

    File.write!(sql_path, sql)

    try do
      case System.cmd("sqlite3", [db_path, ".read #{sql_path}"], stderr_to_stdout: true) do
        {output, 0} -> output
        {output, status} -> raise "sqlite durable soup operation failed #{status}: #{output}"
      end
    after
      File.rm(sql_path)
    end
  end

  defp schema_sql do
    """
    CREATE TABLE IF NOT EXISTS replay_runs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      source_kind TEXT NOT NULL,
      source_db_path TEXT NOT NULL,
      source_row_count INTEGER NOT NULL,
      source_mode TEXT NOT NULL CHECK (source_mode = 'read_only'),
      inserted_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS inputs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      fixture_id TEXT,
      source_type TEXT NOT NULL,
      external_id TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      title TEXT,
      body_text TEXT,
      object_uri TEXT,
      content_sha256 TEXT NOT NULL,
      acl TEXT NOT NULL,
      normalized TEXT NOT NULL,
      facts TEXT NOT NULL,
      background TEXT NOT NULL,
      questions TEXT NOT NULL,
      colors TEXT NOT NULL,
      topic_tokens TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, source_type, external_id),
      UNIQUE (tenant_id, content_sha256)
    );

    CREATE TABLE IF NOT EXISTS stories (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_key TEXT NOT NULL,
      title TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('active', 'background', 'stale', 'resolved')),
      version INTEGER NOT NULL CHECK (version >= 0),
      first_observed_at TEXT NOT NULL,
      updated_at_story TEXT NOT NULL,
      last_material_at TEXT,
      structural_facts TEXT NOT NULL,
      background_facts TEXT NOT NULL,
      colors TEXT NOT NULL,
      questions TEXT NOT NULL,
      topic_tokens TEXT NOT NULL,
      attrs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, story_key)
    );

    CREATE TABLE IF NOT EXISTS agent_runs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      agent_run_key TEXT NOT NULL,
      agent_type TEXT NOT NULL,
      prompt_version TEXT,
      model TEXT,
      scope TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'running')),
      trace_id TEXT,
      started_at TEXT,
      ended_at TEXT,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, agent_run_key)
    );

    CREATE TABLE IF NOT EXISTS proposals (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      proposal_key TEXT NOT NULL,
      agent_run_id TEXT NOT NULL REFERENCES agent_runs(id),
      actor_id TEXT NOT NULL,
      story_id TEXT REFERENCES stories(id),
      fixture_id TEXT,
      classification TEXT NOT NULL,
      confidence TEXT NOT NULL,
      rationale TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'accepted_weak', 'rejected', 'needs_more_evidence')),
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, proposal_key)
    );

    CREATE TABLE IF NOT EXISTS proposal_ops (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      position INTEGER NOT NULL,
      op_type TEXT NOT NULL,
      payload TEXT NOT NULL,
      evidence_refs TEXT NOT NULL,
      confidence TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'accepted_weak', 'rejected', 'needs_more_evidence', 'committed')),
      committed_at TEXT,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (proposal_id, position)
    );

    CREATE TABLE IF NOT EXISTS proposal_decisions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      from_status TEXT NOT NULL,
      to_status TEXT NOT NULL CHECK (to_status IN ('accepted', 'accepted_weak', 'rejected', 'needs_more_evidence')),
      actor_type TEXT NOT NULL,
      actor_id TEXT NOT NULL,
      evidence_refs TEXT NOT NULL,
      confidence TEXT NOT NULL,
      rationale TEXT,
      inserted_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS graph_commits (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      commit_type TEXT NOT NULL,
      committed_by_type TEXT NOT NULL,
      committed_by_id TEXT NOT NULL,
      evidence_refs TEXT NOT NULL,
      confidence TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      UNIQUE (proposal_op_id)
    );

    CREATE TABLE IF NOT EXISTS soup_nodes (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      node_key TEXT NOT NULL,
      node_type TEXT NOT NULL CHECK (node_type IN ('input', 'story', 'claim', 'entity', 'user_watch', 'authored_output')),
      title TEXT NOT NULL,
      state TEXT NOT NULL,
      input_id TEXT REFERENCES inputs(id),
      story_id TEXT REFERENCES stories(id),
      watch_id TEXT,
      authored_output_id TEXT,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      graph_commit_id TEXT NOT NULL REFERENCES graph_commits(id),
      confidence TEXT NOT NULL,
      attrs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, node_key)
    );

    CREATE TABLE IF NOT EXISTS edges (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      from_node_id TEXT NOT NULL REFERENCES soup_nodes(id),
      to_node_id TEXT NOT NULL REFERENCES soup_nodes(id),
      edge_type TEXT NOT NULL CHECK (edge_type IN ('supports', 'updates', 'duplicates', 'contradicts', 'adds_color', 'part_of', 'watch_applies_to') AND edge_type <> 'related'),
      status TEXT NOT NULL CHECK (status = 'committed'),
      confidence TEXT NOT NULL,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      graph_commit_id TEXT NOT NULL REFERENCES graph_commits(id),
      attrs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (proposal_op_id)
    );

    CREATE TABLE IF NOT EXISTS story_fact_versions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      claim_node_id TEXT REFERENCES soup_nodes(id),
      fact_key TEXT NOT NULL,
      fact_value TEXT NOT NULL,
      time_scope TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('current', 'replaced')),
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      graph_commit_id TEXT NOT NULL REFERENCES graph_commits(id),
      input_id TEXT NOT NULL REFERENCES inputs(id),
      confidence TEXT NOT NULL,
      replaced_fact_version_id TEXT,
      observed_at TEXT NOT NULL,
      inserted_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_events (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      input_id TEXT NOT NULL REFERENCES inputs(id),
      classification TEXT NOT NULL,
      story_version INTEGER NOT NULL,
      changed_facts TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      graph_commit_id TEXT NOT NULL REFERENCES graph_commits(id),
      confidence TEXT NOT NULL,
      inserted_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS conflicts (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      fact_key TEXT NOT NULL,
      prior_value TEXT NOT NULL,
      incoming_value TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('open', 'accepted_correction', 'resolved', 'rejected')),
      input_id TEXT NOT NULL REFERENCES inputs(id),
      proposal_id TEXT NOT NULL REFERENCES proposals(id),
      proposal_op_id TEXT NOT NULL REFERENCES proposal_ops(id),
      graph_commit_id TEXT NOT NULL REFERENCES graph_commits(id),
      agent_run_id TEXT NOT NULL REFERENCES agent_runs(id),
      confidence TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS evidence_refs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      subject_type TEXT NOT NULL CHECK (subject_type IN ('soup_node', 'proposal', 'proposal_op', 'edge', 'conflict', 'story_fact_version', 'story_event', 'graph_commit')),
      subject_id TEXT NOT NULL,
      input_id TEXT NOT NULL REFERENCES inputs(id),
      soup_node_id TEXT REFERENCES soup_nodes(id),
      proposal_id TEXT REFERENCES proposals(id),
      proposal_op_id TEXT REFERENCES proposal_ops(id),
      edge_id TEXT REFERENCES edges(id),
      conflict_id TEXT REFERENCES conflicts(id),
      authored_output_id TEXT,
      authored_output_unit_id TEXT,
      span_start INTEGER,
      span_end INTEGER,
      evidence_label TEXT,
      evidence_hash TEXT,
      inserted_at TEXT NOT NULL,
      UNIQUE (tenant_id, subject_type, subject_id, input_id, span_start, span_end)
    );
    """
  end
end
