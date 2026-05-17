defmodule Primeradiant.Proposals.Proposal do
  @moduledoc false

  @enforce_keys [
    :id,
    :agent_run_id,
    :actor_id,
    :fixture_id,
    :story_key,
    :classification,
    :ops,
    :evidence_refs,
    :confidence,
    :rationale,
    :status
  ]
  defstruct [
    :id,
    :agent_run_id,
    :actor_id,
    :fixture_id,
    :story_key,
    :classification,
    :ops,
    :evidence_refs,
    :confidence,
    :rationale,
    :status
  ]
end
