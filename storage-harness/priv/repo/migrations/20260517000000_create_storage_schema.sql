CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE inputs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  fixture_id text,
  source_type text NOT NULL,
  external_id text NOT NULL,
  observed_at timestamptz NOT NULL,
  title text,
  body_text text,
  object_uri text,
  content_sha256 text NOT NULL,
  acl jsonb NOT NULL DEFAULT '{}'::jsonb,
  normalized jsonb NOT NULL DEFAULT '{}'::jsonb,
  facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  background jsonb NOT NULL DEFAULT '{}'::jsonb,
  questions jsonb NOT NULL DEFAULT '{}'::jsonb,
  colors text[] NOT NULL DEFAULT '{}',
  topic_tokens text[] NOT NULL DEFAULT '{}',
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE stories (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  story_key text NOT NULL,
  title text NOT NULL,
  state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'background', 'stale', 'resolved')),
  version integer NOT NULL DEFAULT 0 CHECK (version >= 0),
  first_observed_at timestamptz NOT NULL,
  updated_at_story timestamptz NOT NULL,
  last_material_at timestamptz,
  structural_facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  background_facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  colors text[] NOT NULL DEFAULT '{}',
  questions jsonb NOT NULL DEFAULT '{}'::jsonb,
  topic_tokens text[] NOT NULL DEFAULT '{}',
  attrs jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE watches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  user_id text NOT NULL,
  watch_key text NOT NULL,
  intent text NOT NULL,
  priority integer NOT NULL DEFAULT 0,
  match_any text[] NOT NULL DEFAULT '{}',
  filters jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'active',
  attrs jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE agent_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  agent_run_key text NOT NULL,
  agent_type text NOT NULL,
  prompt_version text,
  model text,
  scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'succeeded',
  trace_id text,
  started_at timestamptz,
  ended_at timestamptz,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE authored_outputs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  user_id text NOT NULL,
  story_id uuid REFERENCES stories(id),
  output_type text NOT NULL,
  content text NOT NULL,
  evidence_packet jsonb NOT NULL DEFAULT '{}'::jsonb,
  verified boolean NOT NULL DEFAULT false,
  story_version integer,
  status text NOT NULL DEFAULT 'recorded',
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE authored_output_units (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  authored_output_id uuid NOT NULL REFERENCES authored_outputs(id),
  position integer NOT NULL,
  unit_type text NOT NULL,
  content text NOT NULL,
  story_id uuid REFERENCES stories(id),
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_array_length(evidence_refs) > 0),
  claim_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_array_length(claim_refs) > 0),
  inserted_at timestamptz NOT NULL
);

CREATE TABLE proposals (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  proposal_key text NOT NULL,
  agent_run_id uuid NOT NULL REFERENCES agent_runs(id),
  actor_id text NOT NULL,
  story_id uuid REFERENCES stories(id),
  fixture_id text,
  classification text NOT NULL,
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  rationale text NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'accepted_weak', 'rejected', 'needs_more_evidence')),
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE proposal_ops (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  position integer NOT NULL,
  op_type text NOT NULL CHECK (op_type IN ('create_input', 'create_story', 'attach_input', 'merge_facts', 'merge_background', 'append_colors', 'add_questions', 'record_conflicts', 'attach_watch', 'attach_story_part_of', 'mark_state', 'mark_last_seen_input', 'record_event')),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_array_length(evidence_refs) > 0),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'accepted_weak', 'rejected', 'needs_more_evidence', 'committed')),
  committed_at timestamptz,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE proposal_decisions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  from_status text NOT NULL,
  to_status text NOT NULL CHECK (to_status IN ('accepted', 'accepted_weak', 'rejected', 'needs_more_evidence')),
  actor_type text NOT NULL,
  actor_id text NOT NULL,
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_array_length(evidence_refs) > 0),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  rationale text,
  inserted_at timestamptz NOT NULL
);

CREATE TABLE graph_commits (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  commit_type text NOT NULL,
  committed_by_type text NOT NULL,
  committed_by_id text NOT NULL,
  evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_array_length(evidence_refs) > 0),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  inserted_at timestamptz NOT NULL
);

CREATE FUNCTION enforce_graph_commit_boundary()
RETURNS trigger AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM proposals p
    WHERE p.id = NEW.proposal_id
      AND p.status IN ('accepted', 'accepted_weak')
  ) THEN
    RAISE EXCEPTION 'graph commits require an accepted proposal';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM proposal_ops op
    WHERE op.id = NEW.proposal_op_id
      AND op.proposal_id = NEW.proposal_id
      AND op.status = 'committed'
      AND op.committed_at IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'graph commits require a committed proposal op';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM proposal_decisions decision
    WHERE decision.proposal_id = NEW.proposal_id
      AND decision.from_status = 'pending'
      AND decision.to_status IN ('accepted', 'accepted_weak')
  ) THEN
    RAISE EXCEPTION 'graph commits require an accepted arbitration decision';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER graph_commits_commit_boundary_trigger
BEFORE INSERT OR UPDATE ON graph_commits
FOR EACH ROW EXECUTE FUNCTION enforce_graph_commit_boundary();

CREATE TABLE soup_nodes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  node_key text NOT NULL,
  node_type text NOT NULL CHECK (node_type IN ('input', 'story', 'claim', 'entity', 'user_watch', 'authored_output')),
  title text NOT NULL,
  state text NOT NULL DEFAULT 'active',
  input_id uuid REFERENCES inputs(id),
  story_id uuid REFERENCES stories(id),
  watch_id uuid REFERENCES watches(id),
  authored_output_id uuid REFERENCES authored_outputs(id),
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  graph_commit_id uuid NOT NULL REFERENCES graph_commits(id),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  attrs jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE edges (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  from_node_id uuid NOT NULL REFERENCES soup_nodes(id),
  to_node_id uuid NOT NULL REFERENCES soup_nodes(id),
  edge_type text NOT NULL CHECK (edge_type IN ('supports', 'updates', 'duplicates', 'contradicts', 'adds_color', 'part_of', 'watch_applies_to') AND edge_type <> 'related'),
  status text NOT NULL DEFAULT 'committed',
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  graph_commit_id uuid NOT NULL REFERENCES graph_commits(id),
  attrs jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE story_fact_versions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  story_id uuid NOT NULL REFERENCES stories(id),
  claim_node_id uuid REFERENCES soup_nodes(id),
  fact_key text NOT NULL,
  fact_value text NOT NULL,
  time_scope text NOT NULL DEFAULT 'current',
  status text NOT NULL DEFAULT 'current',
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  graph_commit_id uuid NOT NULL REFERENCES graph_commits(id),
  input_id uuid NOT NULL REFERENCES inputs(id),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  replaced_fact_version_id uuid REFERENCES story_fact_versions(id),
  observed_at timestamptz NOT NULL,
  inserted_at timestamptz NOT NULL
);

CREATE TABLE story_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  story_id uuid NOT NULL REFERENCES stories(id),
  input_id uuid NOT NULL REFERENCES inputs(id),
  classification text NOT NULL,
  story_version integer NOT NULL,
  changed_facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  observed_at timestamptz NOT NULL,
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  graph_commit_id uuid NOT NULL REFERENCES graph_commits(id),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  inserted_at timestamptz NOT NULL
);

CREATE TABLE conflicts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  story_id uuid NOT NULL REFERENCES stories(id),
  fact_key text NOT NULL,
  prior_value text NOT NULL,
  incoming_value text NOT NULL,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'accepted_correction', 'resolved', 'rejected')),
  input_id uuid NOT NULL REFERENCES inputs(id),
  proposal_id uuid NOT NULL REFERENCES proposals(id),
  proposal_op_id uuid NOT NULL REFERENCES proposal_ops(id),
  graph_commit_id uuid NOT NULL REFERENCES graph_commits(id),
  agent_run_id uuid NOT NULL REFERENCES agent_runs(id),
  confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE seen_states (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  user_id text NOT NULL,
  story_id uuid NOT NULL REFERENCES stories(id),
  seen_story_version integer NOT NULL,
  last_authored_output_id uuid NOT NULL REFERENCES authored_outputs(id),
  seen_at timestamptz NOT NULL,
  inserted_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE seen_state_refs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  seen_state_id uuid NOT NULL REFERENCES seen_states(id),
  ref_kind text NOT NULL CHECK (ref_kind IN ('input', 'claim', 'story', 'edge', 'conflict', 'authored_output')),
  ref_id text NOT NULL,
  authored_output_id uuid NOT NULL REFERENCES authored_outputs(id),
  inserted_at timestamptz NOT NULL
);

CREATE OR REPLACE FUNCTION enforce_verified_seen_state_output()
RETURNS trigger AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM authored_outputs
    WHERE id = NEW.last_authored_output_id
      AND verified = true
  ) THEN
    RAISE EXCEPTION 'seen_states require a verified authored output';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER seen_states_verified_output_trigger
BEFORE INSERT OR UPDATE ON seen_states
FOR EACH ROW EXECUTE FUNCTION enforce_verified_seen_state_output();

CREATE TABLE evidence_refs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL,
  subject_type text NOT NULL,
  subject_id uuid NOT NULL,
  input_id uuid NOT NULL REFERENCES inputs(id),
  soup_node_id uuid REFERENCES soup_nodes(id),
  proposal_id uuid REFERENCES proposals(id),
  proposal_op_id uuid REFERENCES proposal_ops(id),
  edge_id uuid REFERENCES edges(id),
  conflict_id uuid REFERENCES conflicts(id),
  authored_output_id uuid REFERENCES authored_outputs(id),
  authored_output_unit_id uuid REFERENCES authored_output_units(id),
  span_start integer,
  span_end integer,
  evidence_label text,
  evidence_hash text,
  inserted_at timestamptz NOT NULL,
  CHECK (subject_type IN (
    'soup_node',
    'proposal',
    'proposal_op',
    'edge',
    'conflict',
    'authored_output',
    'authored_output_unit',
    'story_fact_version',
    'story_event',
    'graph_commit'
  )),
  CHECK (
    (subject_type = 'soup_node' AND soup_node_id = subject_id)
    OR (subject_type = 'proposal' AND proposal_id = subject_id)
    OR (subject_type = 'proposal_op' AND proposal_op_id = subject_id)
    OR (subject_type = 'edge' AND edge_id = subject_id)
    OR (subject_type = 'conflict' AND conflict_id = subject_id)
    OR (subject_type = 'authored_output' AND authored_output_id = subject_id)
    OR (subject_type = 'authored_output_unit' AND authored_output_unit_id = subject_id)
    OR (subject_type IN ('story_fact_version', 'story_event', 'graph_commit') AND proposal_id IS NOT NULL AND proposal_op_id IS NOT NULL)
  )
);

CREATE FUNCTION enforce_evidence_ref_subject_contract()
RETURNS trigger AS $$
BEGIN
  IF NEW.subject_type = 'story_fact_version' AND NOT EXISTS (
    SELECT 1 FROM story_fact_versions row
    WHERE row.id = NEW.subject_id
      AND row.proposal_id = NEW.proposal_id
      AND row.proposal_op_id = NEW.proposal_op_id
  ) THEN
    RAISE EXCEPTION 'story_fact_version evidence subject is not committed';
  END IF;

  IF NEW.subject_type = 'story_event' AND NOT EXISTS (
    SELECT 1 FROM story_events row
    WHERE row.id = NEW.subject_id
      AND row.proposal_id = NEW.proposal_id
      AND row.proposal_op_id = NEW.proposal_op_id
  ) THEN
    RAISE EXCEPTION 'story_event evidence subject is not committed';
  END IF;

  IF NEW.subject_type = 'graph_commit' AND NOT EXISTS (
    SELECT 1 FROM graph_commits row
    WHERE row.id = NEW.subject_id
      AND row.proposal_id = NEW.proposal_id
      AND row.proposal_op_id = NEW.proposal_op_id
  ) THEN
    RAISE EXCEPTION 'graph_commit evidence subject is not committed';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evidence_refs_subject_contract_trigger
BEFORE INSERT OR UPDATE ON evidence_refs
FOR EACH ROW EXECUTE FUNCTION enforce_evidence_ref_subject_contract();

CREATE UNIQUE INDEX inputs_tenant_source_external_idx ON inputs(tenant_id, source_type, external_id);
CREATE UNIQUE INDEX inputs_tenant_content_sha_idx ON inputs(tenant_id, content_sha256);
CREATE UNIQUE INDEX inputs_tenant_fixture_idx ON inputs(tenant_id, fixture_id) WHERE fixture_id IS NOT NULL;
CREATE UNIQUE INDEX stories_tenant_key_idx ON stories(tenant_id, story_key);
CREATE UNIQUE INDEX watches_tenant_user_key_idx ON watches(tenant_id, user_id, watch_key);
CREATE UNIQUE INDEX agent_runs_tenant_key_idx ON agent_runs(tenant_id, agent_run_key);
CREATE UNIQUE INDEX proposals_tenant_key_idx ON proposals(tenant_id, proposal_key);
CREATE UNIQUE INDEX proposal_ops_proposal_position_idx ON proposal_ops(proposal_id, position);
CREATE UNIQUE INDEX graph_commits_op_idx ON graph_commits(proposal_op_id);
CREATE UNIQUE INDEX soup_nodes_tenant_key_idx ON soup_nodes(tenant_id, node_key);
CREATE UNIQUE INDEX edges_op_idx ON edges(proposal_op_id);
CREATE UNIQUE INDEX story_fact_versions_current_idx ON story_fact_versions(story_id, fact_key, time_scope) WHERE status = 'current';
CREATE UNIQUE INDEX authored_output_units_position_idx ON authored_output_units(authored_output_id, position);
CREATE UNIQUE INDEX seen_states_tenant_user_story_idx ON seen_states(tenant_id, user_id, story_id);
CREATE UNIQUE INDEX seen_state_refs_unique_idx ON seen_state_refs(seen_state_id, ref_kind, ref_id);
CREATE UNIQUE INDEX evidence_refs_unique_span_idx ON evidence_refs(tenant_id, subject_type, subject_id, input_id, coalesce(span_start, -1), coalesce(span_end, -1));

CREATE INDEX soup_nodes_tenant_type_state_idx ON soup_nodes(tenant_id, node_type, state);
CREATE INDEX proposals_status_idx ON proposals(tenant_id, status, inserted_at);
CREATE INDEX proposal_ops_type_status_idx ON proposal_ops(tenant_id, op_type, status);
CREATE INDEX edges_from_type_idx ON edges(tenant_id, from_node_id, edge_type);
CREATE INDEX edges_to_type_idx ON edges(tenant_id, to_node_id, edge_type);
CREATE INDEX evidence_refs_subject_idx ON evidence_refs(tenant_id, subject_type, subject_id);
CREATE INDEX evidence_refs_input_idx ON evidence_refs(tenant_id, input_id);
CREATE INDEX conflicts_story_fact_status_idx ON conflicts(tenant_id, story_id, fact_key, status);
CREATE INDEX story_events_story_version_idx ON story_events(tenant_id, story_id, story_version);
