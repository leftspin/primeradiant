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
    :status,
    :agent_role,
    :agent_config_version,
    :prompt_version_hash,
    :input_packet_hash,
    :visibility,
    :uncertainty_class
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
    :status,
    :agent_role,
    :agent_config_version,
    :prompt_version_hash,
    :input_packet_hash,
    :visibility,
    :uncertainty_class
  ]
end
