defmodule Primeradiant.StorageHarness.DurableSoupDb do
  @moduledoc false

  @tables [
    :story_card_projection_audits,
    :story_reader_deltas,
    :story_card_change_sets,
    :story_key_claims,
    :story_source_coverage,
    :story_card_versions,
    :evidence_refs,
    :conflicts,
    :story_events,
    :story_fact_versions,
    :edges,
    :soup_nodes,
    :graph_commits,
    :seen_state_refs,
    :seen_states,
    :authored_output_units,
    :authored_outputs,
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
        ".bail on",
        "PRAGMA foreign_keys = ON;",
        "BEGIN IMMEDIATE;",
        "PRAGMA defer_foreign_keys = ON;",
        schema_sql(),
        tenant_revision_guard_sql(state.tenant_id, Map.get(attrs, :expected_tenant_revision)),
        clear_tenant_sql(state.tenant_id),
        replay_run_sql(state, attrs),
        rows_sql(:agent_runs, state.agent_runs),
        rows_sql(:inputs, state.inputs),
        rows_sql(:stories, state.stories),
        rows_sql(:proposals, state.proposals),
        rows_sql(:proposal_ops, state.proposal_ops),
        rows_sql(:proposal_decisions, state.proposal_decisions),
        rows_sql(:graph_commits, state.graph_commits),
        rows_sql(:authored_outputs, state.authored_outputs),
        rows_sql(:authored_output_units, state.authored_output_units),
        rows_sql(:seen_states, state.seen_states),
        rows_sql(:seen_state_refs, state.seen_state_refs),
        rows_sql(:soup_nodes, state.soup_nodes),
        rows_sql(:edges, state.edges),
        rows_sql(:story_fact_versions, state.story_fact_versions),
        rows_sql(:story_events, state.story_events),
        rows_sql(:story_card_versions, state.story_card_versions),
        rows_sql(:story_source_coverage, state.story_source_coverage),
        rows_sql(:story_key_claims, state.story_key_claims),
        rows_sql(:story_card_change_sets, state.story_card_change_sets),
        rows_sql(:story_reader_deltas, state.story_reader_deltas),
        rows_sql(:story_card_projection_audits, state.story_card_projection_audits),
        rows_sql(:conflicts, state.conflicts),
        rows_sql(:evidence_refs, state.evidence_refs),
        "COMMIT;"
      ]
      |> Enum.join("\n")

    sqlite!(db_path, sql)
    :ok
  end

  def load_tenant(db_path, tenant_id) do
    if File.regular?(db_path) do
      state = Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")

      state
      |> put_rows(
        :agent_runs,
        load_rows(db_path, "agent_runs", tenant_id, Primeradiant.StorageHarness.AgentRun)
      )
      |> put_rows(
        :inputs,
        load_rows(db_path, "inputs", tenant_id, Primeradiant.StorageHarness.Input)
      )
      |> put_rows(
        :stories,
        load_rows(db_path, "stories", tenant_id, Primeradiant.StorageHarness.Story)
      )
      |> put_rows(
        :proposals,
        load_rows(db_path, "proposals", tenant_id, Primeradiant.StorageHarness.Proposal)
      )
      |> put_rows(
        :proposal_ops,
        load_rows(db_path, "proposal_ops", tenant_id, Primeradiant.StorageHarness.ProposalOp)
      )
      |> put_rows(
        :proposal_decisions,
        load_rows(
          db_path,
          "proposal_decisions",
          tenant_id,
          Primeradiant.StorageHarness.ProposalDecision
        )
      )
      |> put_rows(
        :graph_commits,
        load_rows(db_path, "graph_commits", tenant_id, Primeradiant.StorageHarness.GraphCommit)
      )
      |> put_rows(
        :authored_outputs,
        load_rows(
          db_path,
          "authored_outputs",
          tenant_id,
          Primeradiant.StorageHarness.AuthoredOutput
        )
      )
      |> put_rows(
        :authored_output_units,
        load_rows(
          db_path,
          "authored_output_units",
          tenant_id,
          Primeradiant.StorageHarness.AuthoredOutputUnit
        )
      )
      |> put_rows(
        :seen_states,
        load_rows(db_path, "seen_states", tenant_id, Primeradiant.StorageHarness.SeenState)
      )
      |> put_rows(
        :seen_state_refs,
        load_rows(db_path, "seen_state_refs", tenant_id, Primeradiant.StorageHarness.SeenStateRef)
      )
      |> put_rows(
        :soup_nodes,
        load_rows(db_path, "soup_nodes", tenant_id, Primeradiant.StorageHarness.SoupNode)
      )
      |> put_rows(
        :edges,
        load_rows(db_path, "edges", tenant_id, Primeradiant.StorageHarness.Edge)
      )
      |> put_rows(
        :story_fact_versions,
        load_rows(
          db_path,
          "story_fact_versions",
          tenant_id,
          Primeradiant.StorageHarness.StoryFactVersion
        )
      )
      |> put_rows(
        :story_events,
        load_rows(db_path, "story_events", tenant_id, Primeradiant.StorageHarness.StoryEvent)
      )
      |> put_rows(
        :story_card_versions,
        load_rows(
          db_path,
          "story_card_versions",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardVersion
        )
      )
      |> put_rows(
        :story_source_coverage,
        load_rows(
          db_path,
          "story_source_coverage",
          tenant_id,
          Primeradiant.StorageHarness.StorySourceCoverage
        )
      )
      |> put_rows(
        :story_key_claims,
        load_rows(
          db_path,
          "story_key_claims",
          tenant_id,
          Primeradiant.StorageHarness.StoryKeyClaim
        )
      )
      |> put_rows(
        :story_card_change_sets,
        load_rows(
          db_path,
          "story_card_change_sets",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardChangeSet
        )
      )
      |> put_rows(
        :story_reader_deltas,
        load_rows(
          db_path,
          "story_reader_deltas",
          tenant_id,
          Primeradiant.StorageHarness.StoryReaderDelta
        )
      )
      |> put_rows(
        :story_card_projection_audits,
        load_rows(
          db_path,
          "story_card_projection_audits",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardProjectionAudit
        )
      )
      |> put_rows(
        :conflicts,
        load_rows(db_path, "conflicts", tenant_id, Primeradiant.StorageHarness.Conflict)
      )
      |> put_rows(
        :evidence_refs,
        load_rows(db_path, "evidence_refs", tenant_id, Primeradiant.StorageHarness.EvidenceRef)
      )
      |> rebuild_source_ids()
    else
      Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")
    end
  end

  def load_soup_projection(db_path, tenant_id) do
    if File.regular?(db_path) do
      state = Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")

      state
      |> put_rows(
        :inputs,
        load_rows(db_path, "inputs", tenant_id, Primeradiant.StorageHarness.Input)
      )
      |> put_rows(
        :stories,
        load_rows(db_path, "stories", tenant_id, Primeradiant.StorageHarness.Story)
      )
      |> put_rows(
        :soup_nodes,
        load_rows(db_path, "soup_nodes", tenant_id, Primeradiant.StorageHarness.SoupNode)
      )
      |> put_rows(
        :edges,
        load_rows(db_path, "edges", tenant_id, Primeradiant.StorageHarness.Edge)
      )
      |> put_rows(
        :story_fact_versions,
        load_rows(
          db_path,
          "story_fact_versions",
          tenant_id,
          Primeradiant.StorageHarness.StoryFactVersion
        )
      )
      |> put_rows(
        :story_events,
        load_rows(db_path, "story_events", tenant_id, Primeradiant.StorageHarness.StoryEvent)
      )
      |> put_rows(
        :story_card_versions,
        load_rows(
          db_path,
          "story_card_versions",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardVersion
        )
      )
      |> put_rows(
        :story_source_coverage,
        load_rows(
          db_path,
          "story_source_coverage",
          tenant_id,
          Primeradiant.StorageHarness.StorySourceCoverage
        )
      )
      |> put_rows(
        :story_key_claims,
        load_rows(
          db_path,
          "story_key_claims",
          tenant_id,
          Primeradiant.StorageHarness.StoryKeyClaim
        )
      )
      |> put_rows(
        :story_card_change_sets,
        load_rows(
          db_path,
          "story_card_change_sets",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardChangeSet
        )
      )
      |> put_rows(
        :story_reader_deltas,
        load_rows(
          db_path,
          "story_reader_deltas",
          tenant_id,
          Primeradiant.StorageHarness.StoryReaderDelta
        )
      )
      |> put_rows(
        :evidence_refs,
        load_rows(db_path, "evidence_refs", tenant_id, Primeradiant.StorageHarness.EvidenceRef)
      )
      |> rebuild_source_ids()
    else
      Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")
    end
  end

  def load_soup_ready_projection(db_path, tenant_id) do
    if File.regular?(db_path) do
      state = Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")

      state
      |> put_rows(
        :inputs,
        load_rows(db_path, "inputs", tenant_id, Primeradiant.StorageHarness.Input)
      )
      |> put_rows(
        :stories,
        load_rows(db_path, "stories", tenant_id, Primeradiant.StorageHarness.Story)
      )
      |> put_rows(
        :story_events,
        load_rows(db_path, "story_events", tenant_id, Primeradiant.StorageHarness.StoryEvent)
      )
      |> put_rows(
        :story_card_change_sets,
        load_rows(
          db_path,
          "story_card_change_sets",
          tenant_id,
          Primeradiant.StorageHarness.StoryCardChangeSet
        )
      )
      |> rebuild_source_ids()
    else
      Primeradiant.StorageHarness.State.new(tenant_id: tenant_id, user_id: "flynn")
    end
  end

  def tenant_revision(db_path, tenant_id) do
    if File.regular?(db_path) do
      db_path
      |> sqlite!("""
      SELECT COUNT(*) || ':' || COALESCE(MAX(inserted_at || ':' || id), '')
      FROM replay_runs
      WHERE tenant_id = #{sql_quote(tenant_id)};
      """)
      |> String.trim()
    else
      ""
    end
  end

  def merge_state(%Primeradiant.StorageHarness.State{} = prior, current) do
    %Primeradiant.StorageHarness.State{
      prior
      | source_ids: Map.merge(prior.source_ids, current.source_ids)
    }
    |> merge_rows(:raw_inputs, current.raw_inputs)
    |> merge_rows(:inputs, current.inputs)
    |> merge_rows(:agent_runs, current.agent_runs)
    |> merge_rows(:evidence_refs, current.evidence_refs)
    |> merge_rows(:story_card_versions, current.story_card_versions)
    |> merge_rows(:story_source_coverage, current.story_source_coverage)
    |> merge_rows(:story_key_claims, current.story_key_claims)
    |> merge_rows(:story_card_change_sets, current.story_card_change_sets)
    |> merge_rows(:story_reader_deltas, current.story_reader_deltas)
    |> merge_rows(:story_card_projection_audits, current.story_card_projection_audits)
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
        agent_runs: count(db_path, "agent_runs", tenant_id),
        inputs: count(db_path, "inputs", tenant_id),
        stories: count(db_path, "stories", tenant_id),
        proposals: count(db_path, "proposals", tenant_id),
        proposal_ops: count(db_path, "proposal_ops", tenant_id),
        proposal_decisions: count(db_path, "proposal_decisions", tenant_id),
        graph_commits: count(db_path, "graph_commits", tenant_id),
        story_events: count(db_path, "story_events", tenant_id),
        story_card_versions: count(db_path, "story_card_versions", tenant_id),
        story_source_coverage: count(db_path, "story_source_coverage", tenant_id),
        story_key_claims: count(db_path, "story_key_claims", tenant_id),
        evidence_refs: count(db_path, "evidence_refs", tenant_id)
      },
      seen_state_delta: seen_state_delta_report(db_path, tenant_id),
      extraction_quality: extraction_quality_report(db_path, tenant_id),
      ingestion: ingestion_report,
      changed_stories: changed_stories
    }
  end

  def source_admission_report(db_path, tenant_id, source_summary, ingestion_report) do
    %{
      source: source_summary,
      primeradiant_writes: %{
        owned_state_only: true,
        durable: true,
        soup_db_path: db_path,
        agent_runs: count(db_path, "agent_runs", tenant_id),
        inputs: count(db_path, "inputs", tenant_id),
        stories: count(db_path, "stories", tenant_id),
        proposals: count(db_path, "proposals", tenant_id),
        proposal_ops: count(db_path, "proposal_ops", tenant_id),
        proposal_decisions: count(db_path, "proposal_decisions", tenant_id),
        graph_commits: count(db_path, "graph_commits", tenant_id),
        story_events: count(db_path, "story_events", tenant_id),
        evidence_refs: count(db_path, "evidence_refs", tenant_id),
        substrate_proof_only: true,
        story_meaning_proof: false
      },
      ingestion: ingestion_report,
      admitted_source_items: Map.get(ingestion_report, :admissions, []),
      changed_stories: [],
      substrate_proof_only: true,
      story_meaning_proof: false
    }
  end

  def table_count(db_path, table, tenant_id), do: count(db_path, table, tenant_id)

  defp seen_state_delta_report(db_path, tenant_id) do
    %{
      authored_outputs: count(db_path, "authored_outputs", tenant_id),
      authored_output_units: count(db_path, "authored_output_units", tenant_id),
      seen_states: count(db_path, "seen_states", tenant_id),
      seen_state_refs: count(db_path, "seen_state_refs", tenant_id),
      current_seen_refs:
        query_json(db_path, """
        SELECT json_object(
          'seen_state_id', seen_states.id,
          'user_id', seen_states.user_id,
          'story_id', seen_states.story_id,
          'seen_story_version', seen_states.seen_story_version,
          'seen_at', seen_states.seen_at,
          'ref_kind', seen_state_refs.ref_kind,
          'ref_id', seen_state_refs.ref_id,
          'authored_output_id', seen_state_refs.authored_output_id
        )
        FROM seen_states
        JOIN seen_state_refs ON seen_state_refs.seen_state_id = seen_states.id
        WHERE seen_states.tenant_id = #{sql_quote(tenant_id)}
        ORDER BY seen_states.seen_at, seen_state_refs.ref_kind, seen_state_refs.ref_id;
        """),
      delta_outputs:
        query_json(db_path, """
        SELECT json_object(
          'delta_report_id', authored_outputs.id,
          'user_id', authored_outputs.user_id,
          'story_id', authored_outputs.story_id,
          'story_version', authored_outputs.story_version,
          'verified', authored_outputs.verified,
          'unit_count', COUNT(authored_output_units.id)
        )
        FROM authored_outputs
        LEFT JOIN authored_output_units ON authored_output_units.authored_output_id = authored_outputs.id
        WHERE authored_outputs.tenant_id = #{sql_quote(tenant_id)}
        GROUP BY authored_outputs.id
        ORDER BY authored_outputs.inserted_at;
        """)
    }
  end

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

  defp tenant_revision_guard_sql(_tenant_id, nil), do: ""

  defp tenant_revision_guard_sql(tenant_id, expected_revision) do
    """
    CREATE TEMP TABLE IF NOT EXISTS primeradiant_revision_guard (
      ok INTEGER NOT NULL CHECK (ok = 1)
    );
    DELETE FROM primeradiant_revision_guard;
    INSERT INTO primeradiant_revision_guard(ok)
    SELECT CASE
      WHEN (
        SELECT COUNT(*) || ':' || COALESCE(MAX(inserted_at || ':' || id), '')
        FROM replay_runs
        WHERE tenant_id = #{sql_quote(tenant_id)}
      ) = #{sql_quote(expected_revision)}
      THEN 1
      ELSE 0
    END;
    DROP TABLE primeradiant_revision_guard;
    """
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

  defp row_map(:authored_outputs, row) do
    take(
      row,
      ~w(id tenant_id user_id story_id output_type content evidence_packet verified story_version status inserted_at updated_at)a
    )
  end

  defp row_map(:authored_output_units, row) do
    take(
      row,
      ~w(id tenant_id authored_output_id position unit_type content story_id evidence_refs claim_refs inserted_at)a
    )
  end

  defp row_map(:seen_states, row) do
    take(
      row,
      ~w(id tenant_id user_id story_id seen_story_version last_authored_output_id seen_at inserted_at updated_at)a
    )
  end

  defp row_map(:seen_state_refs, row) do
    take(row, ~w(id tenant_id seen_state_id ref_kind ref_id authored_output_id inserted_at)a)
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

  defp row_map(:story_card_versions, row) do
    take(
      row,
      ~w(id tenant_id story_id story_version card_version status supersedes_id refresh_reason producing_agent_run_id packet_hash prompt_config_hash output_hash field_provenance_manifest_id title deck summary freshness field_completeness topic_salience provenance inserted_at updated_at)a
    )
  end

  defp row_map(:story_source_coverage, row) do
    take(
      row,
      ~w(id tenant_id story_id story_card_version_id source_ref article_ref canonical_public_url source_domain source_label publication source_posture contribution_reason materiality source_weight first_observed_at last_observed_at evidence_refs provenance_refs inserted_at updated_at)a
    )
  end

  defp row_map(:story_key_claims, row) do
    take(
      row,
      ~w(id tenant_id story_id story_card_version_id claim_ref text status materiality evidence_refs conflict_refs uncertainty appears_in_current_card inserted_at updated_at)a
    )
  end

  defp row_map(:story_card_change_sets, row) do
    take(
      row,
      ~w(id tenant_id story_id prior_card_version_id new_card_version_id changed_field_keys added_claim_refs removed_claim_refs changed_claim_refs changed_source_coverage_refs refresh_reason change_summary inserted_at updated_at)a
    )
  end

  defp row_map(:story_reader_deltas, row) do
    take(
      row,
      ~w(id tenant_id user_id story_id seen_state_id prior_seen_story_version prior_seen_card_version_id current_story_version current_card_version_id material_unseen_deltas nonmaterial_exclusions producing_agent_run_id evidence_refs provenance_refs inserted_at updated_at)a
    )
  end

  defp row_map(:story_card_projection_audits, row) do
    take(
      row,
      ~w(id tenant_id projection_id consumer query_time cursor story_card_version_ids omitted_story_reasons visibility_scope status inserted_at updated_at)a
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

  defp put_rows(state, field, rows), do: Map.put(state, field, rows)

  defp merge_rows(state, field, rows), do: Map.update!(state, field, &uniq_by_id(&1 ++ rows))

  defp uniq_by_id(rows), do: rows |> Enum.reverse() |> Enum.uniq_by(& &1.id) |> Enum.reverse()

  defp rebuild_source_ids(state) do
    source_ids =
      Enum.reduce(state.inputs, state.source_ids, fn input, acc ->
        Map.put(acc, {:input, "#{input.source_type}:#{input.external_id}"}, input.id)
      end)
      |> then(fn acc ->
        Enum.reduce(state.stories, acc, &Map.put(&2, {:story, &1.story_key}, &1.id))
      end)
      |> then(fn acc ->
        Enum.reduce(state.soup_nodes, acc, &Map.put(&2, {:node, &1.node_key}, &1.id))
      end)
      |> then(fn acc ->
        Enum.reduce(state.agent_runs, acc, &Map.put(&2, {:agent_run, &1.agent_run_key}, &1.id))
      end)
      |> then(fn acc ->
        Enum.reduce(state.proposals, acc, &Map.put(&2, {:proposal, &1.proposal_key}, &1.id))
      end)

    %{state | source_ids: source_ids}
  end

  defp load_rows(db_path, table, tenant_id, module) do
    query_table_json(db_path, "SELECT * FROM #{table} WHERE tenant_id = #{sql_quote(tenant_id)};")
    |> Enum.map(&row_struct(module, &1))
  rescue
    _ -> []
  end

  defp query_table_json(db_path, sql) do
    case System.cmd("sqlite3", ["-cmd", ".mode json", db_path, sql], stderr_to_stdout: true) do
      {output, 0} -> Jason.decode!(blank_json_array(output))
      {_output, _status} -> []
    end
  end

  defp blank_json_array(output) do
    case String.trim(output) do
      "" -> "[]"
      json -> json
    end
  end

  defp row_struct(module, row) do
    attrs =
      row
      |> Map.new(fn {key, value} -> {String.to_atom(key), load_value(key, value)} end)

    struct(module, attrs)
  end

  defp load_value(_key, nil), do: nil
  defp load_value("confidence", value), do: Decimal.new(to_string(value))
  defp load_value("verified", value), do: value in [1, true, "1"]
  defp load_value("appears_in_current_card", value), do: value in [1, true, "1"]

  defp load_value(key, value)
       when key in ~w(acl scope normalized facts background questions colors topic_tokens attrs payload evidence_refs changed_facts structural_facts background_facts evidence_packet claim_refs title deck summary freshness field_completeness topic_salience provenance canonical_public_url source_domain source_label publication source_posture contribution_reason source_weight provenance_refs conflict_refs uncertainty changed_field_keys added_claim_refs removed_claim_refs changed_claim_refs changed_source_coverage_refs change_summary material_unseen_deltas nonmaterial_exclusions story_card_version_ids omitted_story_reasons visibility_scope) and
              is_binary(value),
       do: decode_json(value, %{})

  defp load_value(key, value)
       when key in ~w(inserted_at updated_at started_at ended_at committed_at observed_at first_observed_at updated_at_story last_material_at seen_at first_observed_at last_observed_at query_time) and
              is_binary(value),
       do: Primeradiant.StorageHarness.ChangesetStore.iso!(value)

  defp load_value(_key, value), do: value

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
       CHECK (lower(story_key) NOT IN ('new-story', 'new_story', 'newstory', 'story', 'news-story')),
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

    CREATE TABLE IF NOT EXISTS authored_outputs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      story_id TEXT REFERENCES stories(id),
      output_type TEXT NOT NULL,
      content TEXT NOT NULL,
      evidence_packet TEXT NOT NULL,
      verified INTEGER NOT NULL,
      story_version INTEGER,
      status TEXT NOT NULL CHECK (status = 'recorded'),
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS authored_output_units (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      authored_output_id TEXT NOT NULL REFERENCES authored_outputs(id),
      position INTEGER NOT NULL,
      unit_type TEXT NOT NULL,
      content TEXT NOT NULL,
      story_id TEXT REFERENCES stories(id),
      evidence_refs TEXT NOT NULL,
      claim_refs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      UNIQUE (authored_output_id, position)
    );

    CREATE TABLE IF NOT EXISTS seen_states (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      seen_story_version INTEGER NOT NULL,
      last_authored_output_id TEXT NOT NULL REFERENCES authored_outputs(id),
      seen_at TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, user_id, story_id)
    );

    CREATE TABLE IF NOT EXISTS seen_state_refs (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      seen_state_id TEXT NOT NULL REFERENCES seen_states(id),
      ref_kind TEXT NOT NULL CHECK (ref_kind IN ('input', 'claim', 'story', 'edge', 'conflict', 'authored_output')),
      ref_id TEXT NOT NULL,
      authored_output_id TEXT NOT NULL REFERENCES authored_outputs(id),
      inserted_at TEXT NOT NULL,
      UNIQUE (seen_state_id, ref_kind, ref_id)
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
        CHECK (
          json_extract(attrs, '$.edge_contract') IS NULL
          OR json_extract(attrs, '$.edge_contract') != 'article_story_contribution'
          OR (
            json_extract(attrs, '$.edge_contract') = 'article_story_contribution'
           AND coalesce(length(json_extract(attrs, '$.link_basis')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.contribution_type')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.source_ref')), 0) > 0
           AND json_extract(attrs, '$.evidence_refs') IS NOT NULL
           AND coalesce(json_array_length(json_extract(attrs, '$.evidence_refs')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.agent_run_id')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.agent_prompt_version')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.agent_output_hash')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.packet_hash')), 0) > 0
           AND coalesce(length(json_extract(attrs, '$.correlation_id')), 0) > 0
         )
       ),
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

    CREATE TABLE IF NOT EXISTS story_card_versions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      story_version INTEGER NOT NULL,
      card_version INTEGER NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('complete', 'incomplete', 'refused', 'unavailable')),
      supersedes_id TEXT,
      refresh_reason TEXT NOT NULL CHECK (refresh_reason IN ('story_created', 'source_linked', 'source_content_changed', 'source_weight_changed', 'reader_delta_requested', 'repair_backfill', 'stale_recheck', 'manual_review', 'active_story_recurring_15m', 'story_card_hourly_synthesis', 'daily_deep_soup_sweep')),
      producing_agent_run_id TEXT NOT NULL REFERENCES agent_runs(id),
      packet_hash TEXT NOT NULL,
      prompt_config_hash TEXT NOT NULL,
      output_hash TEXT NOT NULL,
      field_provenance_manifest_id TEXT NOT NULL,
      title TEXT NOT NULL,
      deck TEXT NOT NULL,
      summary TEXT NOT NULL,
      freshness TEXT NOT NULL,
      field_completeness TEXT NOT NULL,
      topic_salience TEXT NOT NULL,
      provenance TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (tenant_id, story_id, card_version)
    );

    CREATE TABLE IF NOT EXISTS story_source_coverage (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      story_card_version_id TEXT NOT NULL REFERENCES story_card_versions(id),
      source_ref TEXT NOT NULL,
      article_ref TEXT,
      canonical_public_url TEXT NOT NULL,
      source_domain TEXT NOT NULL,
      source_label TEXT NOT NULL,
      publication TEXT NOT NULL,
      source_posture TEXT NOT NULL,
      contribution_reason TEXT NOT NULL,
      materiality TEXT NOT NULL,
      source_weight TEXT NOT NULL,
      first_observed_at TEXT NOT NULL,
      last_observed_at TEXT NOT NULL,
      evidence_refs TEXT NOT NULL,
      provenance_refs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (story_card_version_id, source_ref, article_ref)
    );

    CREATE TABLE IF NOT EXISTS story_key_claims (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      story_card_version_id TEXT NOT NULL REFERENCES story_card_versions(id),
      claim_ref TEXT NOT NULL,
      text TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('current', 'disputed', 'stale', 'background', 'unresolved')),
      materiality TEXT NOT NULL,
      evidence_refs TEXT NOT NULL,
      conflict_refs TEXT NOT NULL,
      uncertainty TEXT NOT NULL,
      appears_in_current_card INTEGER NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE (story_card_version_id, claim_ref)
    );

    CREATE TABLE IF NOT EXISTS story_card_change_sets (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      prior_card_version_id TEXT,
      new_card_version_id TEXT NOT NULL REFERENCES story_card_versions(id),
      changed_field_keys TEXT NOT NULL,
      added_claim_refs TEXT NOT NULL,
      removed_claim_refs TEXT NOT NULL,
      changed_claim_refs TEXT NOT NULL,
      changed_source_coverage_refs TEXT NOT NULL,
      refresh_reason TEXT NOT NULL,
      change_summary TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_reader_deltas (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      story_id TEXT NOT NULL REFERENCES stories(id),
      seen_state_id TEXT,
      prior_seen_story_version INTEGER NOT NULL,
      prior_seen_card_version_id TEXT,
      current_story_version INTEGER NOT NULL,
      current_card_version_id TEXT NOT NULL REFERENCES story_card_versions(id),
      material_unseen_deltas TEXT NOT NULL,
      nonmaterial_exclusions TEXT NOT NULL,
      producing_agent_run_id TEXT NOT NULL REFERENCES agent_runs(id),
      evidence_refs TEXT NOT NULL,
      provenance_refs TEXT NOT NULL,
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS story_card_projection_audits (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      projection_id TEXT NOT NULL,
      consumer TEXT NOT NULL,
      query_time TEXT NOT NULL,
      cursor TEXT NOT NULL,
      story_card_version_ids TEXT NOT NULL,
      omitted_story_reasons TEXT NOT NULL,
      visibility_scope TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('complete', 'partial', 'stale')),
      inserted_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
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
      subject_type TEXT NOT NULL CHECK (subject_type IN ('soup_node', 'proposal', 'proposal_op', 'edge', 'conflict', 'story_fact_version', 'story_event', 'graph_commit', 'authored_output_unit')),
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
