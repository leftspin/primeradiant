\set ON_ERROR_STOP on

DO $$
DECLARE
  expected_tables text[] := ARRAY[
    'inputs',
    'soup_nodes',
    'stories',
    'story_fact_versions',
    'story_events',
    'watches',
    'agent_runs',
    'proposals',
    'proposal_ops',
    'proposal_decisions',
    'graph_commits',
    'edges',
    'conflicts',
    'evidence_refs',
    'authored_outputs',
    'authored_output_units',
    'seen_states',
    'seen_state_refs'
  ];
  expected_triggers text[] := ARRAY[
    'graph_commits_commit_boundary_trigger',
    'seen_states_verified_output_trigger',
    'evidence_refs_subject_contract_trigger'
  ];
  expected_indexes text[] := ARRAY[
    'graph_commits_op_idx',
    'edges_op_idx',
    'evidence_refs_subject_idx',
    'seen_states_tenant_user_story_idx'
  ];
  found_count integer;
  tenant uuid := gen_random_uuid();
  agent_id uuid;
  input_id uuid;
  story_id uuid;
  proposal_id uuid;
  proposal_op_id uuid;
  graph_commit_id uuid;
  input_node_id uuid;
  story_node_id uuid;
  edge_id uuid;
  verified_output_id uuid;
  unverified_output_id uuid;
  blocked boolean;
BEGIN
  SELECT count(*)
  INTO found_count
  FROM information_schema.tables
  WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
    AND table_name = ANY(expected_tables);

  IF found_count <> array_length(expected_tables, 1) THEN
    RAISE EXCEPTION 'expected % storage tables, found %', array_length(expected_tables, 1), found_count;
  END IF;

  SELECT count(DISTINCT trigger_name)
  INTO found_count
  FROM information_schema.triggers
  WHERE trigger_schema = 'public'
    AND trigger_name = ANY(expected_triggers);

  IF found_count <> array_length(expected_triggers, 1) THEN
    RAISE EXCEPTION 'expected % hardening triggers, found %', array_length(expected_triggers, 1), found_count;
  END IF;

  SELECT count(*)
  INTO found_count
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND indexname = ANY(expected_indexes);

  IF found_count <> array_length(expected_indexes, 1) THEN
    RAISE EXCEPTION 'expected % hardening indexes, found %', array_length(expected_indexes, 1), found_count;
  END IF;

  INSERT INTO agent_runs (
    tenant_id,
    agent_run_key,
    agent_type,
    scope,
    status,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-agent-run',
    'storage_validation',
    '{}'::jsonb,
    'succeeded',
    now(),
    now()
  )
  RETURNING id INTO agent_id;

  INSERT INTO inputs (
    tenant_id,
    fixture_id,
    source_type,
    external_id,
    observed_at,
    title,
    body_text,
    content_sha256,
    acl,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-input',
    'fixture',
    'validation-input',
    now(),
    'Validation input',
    'Validation body',
    encode(digest('validation-input', 'sha256'), 'hex'),
    '{"privacy":"public"}'::jsonb,
    now(),
    now()
  )
  RETURNING id INTO input_id;

  INSERT INTO stories (
    tenant_id,
    story_key,
    title,
    first_observed_at,
    updated_at_story,
    last_material_at,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-story',
    'Validation story',
    now(),
    now(),
    now(),
    now(),
    now()
  )
  RETURNING id INTO story_id;

  INSERT INTO proposals (
    tenant_id,
    proposal_key,
    agent_run_id,
    actor_id,
    story_id,
    classification,
    confidence,
    rationale,
    status,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-proposal',
    agent_id,
    'flynn',
    story_id,
    'validation',
    1.0,
    'validate commit boundary',
    'pending',
    now(),
    now()
  )
  RETURNING id INTO proposal_id;

  INSERT INTO proposal_ops (
    tenant_id,
    proposal_id,
    position,
    op_type,
    payload,
    evidence_refs,
    confidence,
    status,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    proposal_id,
    0,
    'create_story',
    '{}'::jsonb,
    jsonb_build_array(jsonb_build_object('input_ref', 'validation-input')),
    1.0,
    'pending',
    now(),
    now()
  )
  RETURNING id INTO proposal_op_id;

  blocked := false;
  BEGIN
    INSERT INTO graph_commits (
      tenant_id,
      proposal_id,
      proposal_op_id,
      commit_type,
      committed_by_type,
      committed_by_id,
      evidence_refs,
      confidence,
      inserted_at
    )
    VALUES (
      tenant,
      proposal_id,
      proposal_op_id,
      'create_story',
      'arbiter',
      'flynn',
      jsonb_build_array(jsonb_build_object('input_ref', 'validation-input')),
      1.0,
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'graph commit inserted before proposal acceptance';
  END IF;

  UPDATE proposals
  SET status = 'accepted', updated_at = now()
  WHERE id = proposal_id;

  UPDATE proposal_ops
  SET status = 'committed', committed_at = now(), updated_at = now()
  WHERE id = proposal_op_id;

  blocked := false;
  BEGIN
    INSERT INTO graph_commits (
      tenant_id,
      proposal_id,
      proposal_op_id,
      commit_type,
      committed_by_type,
      committed_by_id,
      evidence_refs,
      confidence,
      inserted_at
    )
    VALUES (
      tenant,
      proposal_id,
      proposal_op_id,
      'create_story',
      'arbiter',
      'flynn',
      jsonb_build_array(jsonb_build_object('input_ref', 'validation-input')),
      1.0,
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'graph commit inserted before accepted decision';
  END IF;

  INSERT INTO proposal_decisions (
    tenant_id,
    proposal_id,
    from_status,
    to_status,
    actor_type,
    actor_id,
    evidence_refs,
    confidence,
    rationale,
    inserted_at
  )
  VALUES (
    tenant,
    proposal_id,
    'pending',
    'accepted',
    'arbiter',
    'flynn',
    jsonb_build_array(jsonb_build_object('input_ref', 'validation-input')),
    1.0,
    'accepted for validation',
    now()
  );

  INSERT INTO graph_commits (
    tenant_id,
    proposal_id,
    proposal_op_id,
    commit_type,
    committed_by_type,
    committed_by_id,
    evidence_refs,
    confidence,
    inserted_at
  )
  VALUES (
    tenant,
    proposal_id,
    proposal_op_id,
    'create_story',
    'arbiter',
    'flynn',
    jsonb_build_array(jsonb_build_object('input_ref', 'validation-input')),
    1.0,
    now()
  )
  RETURNING id INTO graph_commit_id;

  INSERT INTO soup_nodes (
    tenant_id,
    node_key,
    node_type,
    title,
    input_id,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    confidence,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-input',
    'input',
    'Validation input',
    input_id,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    1.0,
    now(),
    now()
  )
  RETURNING id INTO input_node_id;

  INSERT INTO soup_nodes (
    tenant_id,
    node_key,
    node_type,
    title,
    story_id,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    confidence,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'validation-story',
    'story',
    'Validation story',
    story_id,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    1.0,
    now(),
    now()
  )
  RETURNING id INTO story_node_id;

  blocked := false;
  BEGIN
    INSERT INTO edges (
      tenant_id,
      from_node_id,
      to_node_id,
      edge_type,
      confidence,
      proposal_id,
      proposal_op_id,
      graph_commit_id,
      inserted_at,
      updated_at
    )
    VALUES (
      tenant,
      input_node_id,
      story_node_id,
      'related',
      1.0,
      proposal_id,
      proposal_op_id,
      graph_commit_id,
      now(),
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'unsupported related edge inserted';
  END IF;

  INSERT INTO edges (
    tenant_id,
    from_node_id,
    to_node_id,
    edge_type,
    confidence,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    input_node_id,
    story_node_id,
    'supports',
    1.0,
    proposal_id,
    proposal_op_id,
    graph_commit_id,
    now(),
    now()
  )
  RETURNING id INTO edge_id;

  INSERT INTO evidence_refs (
    tenant_id,
    subject_type,
    subject_id,
    input_id,
    proposal_id,
    proposal_op_id,
    evidence_label,
    evidence_hash,
    inserted_at
  )
  VALUES (
    tenant,
    'graph_commit',
    graph_commit_id,
    input_id,
    proposal_id,
    proposal_op_id,
    'validation-input',
    encode(digest('validation-input', 'sha256'), 'hex'),
    now()
  );

  blocked := false;
  BEGIN
    INSERT INTO evidence_refs (
      tenant_id,
      subject_type,
      subject_id,
      input_id,
      evidence_label,
      inserted_at
    )
    VALUES (
      tenant,
      'graph_commit',
      graph_commit_id,
      input_id,
      'validation-input',
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'graph_commit evidence inserted without proposal/op subject contract';
  END IF;

  blocked := false;
  BEGIN
    INSERT INTO evidence_refs (
      tenant_id,
      subject_type,
      subject_id,
      input_id,
      evidence_label,
      inserted_at
    )
    VALUES (
      tenant,
      'unknown',
      graph_commit_id,
      input_id,
      'validation-input',
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'unknown evidence subject_type inserted';
  END IF;

  INSERT INTO authored_outputs (
    tenant_id,
    user_id,
    story_id,
    output_type,
    content,
    evidence_packet,
    verified,
    story_version,
    status,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'flynn',
    story_id,
    'briefing',
    'unverified',
    '{}'::jsonb,
    false,
    0,
    'recorded',
    now(),
    now()
  )
  RETURNING id INTO unverified_output_id;

  blocked := false;
  BEGIN
    INSERT INTO seen_states (
      tenant_id,
      user_id,
      story_id,
      seen_story_version,
      last_authored_output_id,
      seen_at,
      inserted_at,
      updated_at
    )
    VALUES (
      tenant,
      'flynn',
      story_id,
      1,
      unverified_output_id,
      now(),
      now(),
      now()
    );
  EXCEPTION WHEN others THEN
    blocked := true;
  END;

  IF NOT blocked THEN
    RAISE EXCEPTION 'seen_state inserted for unverified output';
  END IF;

  INSERT INTO authored_outputs (
    tenant_id,
    user_id,
    story_id,
    output_type,
    content,
    evidence_packet,
    verified,
    story_version,
    status,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'flynn',
    story_id,
    'briefing',
    'verified',
    '{}'::jsonb,
    true,
    1,
    'recorded',
    now(),
    now()
  )
  RETURNING id INTO verified_output_id;

  INSERT INTO seen_states (
    tenant_id,
    user_id,
    story_id,
    seen_story_version,
    last_authored_output_id,
    seen_at,
    inserted_at,
    updated_at
  )
  VALUES (
    tenant,
    'flynn',
    story_id,
    1,
    verified_output_id,
    now(),
    now(),
    now()
  );
END;
$$;

SELECT 'postgres_schema_validation_ok' AS result;
