defmodule Primeradiant.StorageHarness.State do
  @moduledoc false

  defstruct tenant_id: nil,
            user_id: "flynn",
            raw_inputs: [],
            expected_artifacts: [],
            source_registrations: [],
            source_gap_records: [],
            raw_envelopes: [],
            resolution_cases: [],
            resolution_evidence: [],
            resolved_source_fields: [],
            resolution_attempts: [],
            resolution_outcomes: [],
            package_acknowledgements: [],
            inputs: [],
            watches: [],
            agent_runs: [],
            proposals: [],
            proposal_ops: [],
            proposal_decisions: [],
            graph_commits: [],
            stories: [],
            soup_nodes: [],
            edges: [],
            story_fact_versions: [],
            story_events: [],
            story_card_versions: [],
            story_source_coverage: [],
            story_key_claims: [],
            story_card_change_sets: [],
            story_reader_deltas: [],
            story_card_projection_audits: [],
            repair_runs: [],
            story_quarantines: [],
            conflicts: [],
            evidence_refs: [],
            authored_outputs: [],
            authored_output_units: [],
            seen_states: [],
            seen_state_refs: [],
            audit_events: [],
            source_ids: %{},
            decisions: []

  def new(opts \\ []) do
    %__MODULE__{
      tenant_id: Keyword.get(opts, :tenant_id, Ecto.UUID.generate()),
      user_id: Keyword.get(opts, :user_id, "flynn")
    }
  end

  def append(%__MODULE__{} = state, field, value), do: Map.update!(state, field, &(&1 ++ [value]))

  def replace(%__MODULE__{} = state, field, id, value) do
    Map.update!(state, field, fn rows ->
      Enum.map(rows, fn
        %{id: ^id} -> value
        row -> row
      end)
    end)
  end

  def audit(%__MODULE__{} = state, event) do
    update_in(state.audit_events, &(&1 ++ [event]))
  end

  def put_source_id(%__MODULE__{} = state, kind, key, id) do
    update_in(state.source_ids, &Map.put(&1, {kind, key}, id))
  end

  def source_id!(%__MODULE__{} = state, kind, key), do: Map.fetch!(state.source_ids, {kind, key})
end
