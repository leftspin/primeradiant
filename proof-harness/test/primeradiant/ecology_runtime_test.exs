defmodule Primeradiant.EcologyRuntimeTest do
  use ExUnit.Case, async: true

  alias Primeradiant.Agentic.EcologyRuntime

  test "T1178 registry, scheduler, packet, run, write, and boundary audits pass" do
    result = EcologyRuntime.run()
    audit = result.audit

    assert audit.scheduler.trigger_kinds_represented
    assert audit.scheduler.deterministic_eligibility_inspected
    assert audit.scheduler.leases_recorded
    assert audit.scheduler.cascade_budgets
    assert audit.scheduler.cooldowns
    assert audit.scheduler.suppressions_are_substrate
    assert Enum.any?(result.suppressions, &(&1.reason == :trigger_disabled))

    assert Enum.all?(
             result.suppressions,
             &(&1.scheduler_substrate and &1.agent_abstention == false)
           )

    assert audit.registry.yaml_backed
    assert audit.registry.versioned_rows
    assert audit.registry.readable_and_emitted_separate
    assert audit.registry.permissions_visible
    assert audit.registry.readable_family_coverage
    assert audit.registry.emitted_family_coverage
    assert audit.registry.trigger_limit_hash_coverage
    assert audit.registry.enabled_disabled_version_state
    assert audit.registry.version_history
    assert length(result.registry_history) == length(result.registry)

    assert audit.packets.bounded
    assert audit.packets.packet_hashes
    assert audit.packets.visibility_scope
    assert audit.packets.traversal_bounds
    assert audit.packets.evidence_policy

    assert audit.runs.actual_agent_invocations >= 3
    assert audit.runs.at_least_two_agent_families
    assert audit.runs.scheduled_or_interval_without_fresh_import
    assert audit.runs.soup_change_after_committed_mutation
    assert audit.runs.abstention_or_refusal_or_noop
    assert audit.runs.agent_choice_recorded
    assert audit.runs.provenance_hashes

    assert audit.writes.typed_proposals
    assert audit.writes.evidence_backed_proposals
    assert audit.writes.append_only_mutations
    assert audit.writes.linked_to_agent_runs
    assert audit.writes.downstream_activations

    run_ids = result.agent_runs |> Enum.map(& &1.agent_run_id) |> MapSet.new()
    proposal_run_ids = Map.new(result.proposals, &{&1.proposal_id, &1.agent_run_id})

    assert Enum.all?(result.proposals, &MapSet.member?(run_ids, &1.agent_run_id))

    assert Enum.all?(
             result.mutations,
             &MapSet.member?(run_ids, Map.get(proposal_run_ids, &1.proposal_id))
           )

    run_by_id = Map.new(result.agent_runs, &{&1.agent_run_id, &1})

    assert Enum.all?(
             result.proposals,
             &(Map.fetch!(run_by_id, &1.agent_run_id).status != :skipped_duplicate)
           )

    assert audit.boundary.no_t328_source_emitter_registered
    assert audit.boundary.no_persistent_service_installed
    assert audit.boundary.no_launchd_cron_systemd_autostart
    assert audit.boundary.no_source_db_mutation
    assert audit.boundary.no_production_runtime_state_mutation
  end

  test "T1178 proof distinguishes actual agent work from deterministic substrate" do
    result = EcologyRuntime.run()
    audit = result.audit

    assert audit.anti_cheat.deterministic_standins_not_counted
    assert audit.anti_cheat.no_scripted_fixture_oracle_product_proof
    assert audit.anti_cheat.source_admission_substrate_not_product_proof
    assert audit.anti_cheat.prompt_config_packet_output_hashes_present
    assert audit.anti_cheat.scripted_outputs_not_counted
    assert audit.anti_cheat.hard_coded_clusters_not_counted
    assert audit.anti_cheat.fixture_trusted_story_ids_not_counted
    assert audit.anti_cheat.precomputed_authored_deltas_not_counted
    assert audit.anti_cheat.wrong_product_shape_exclusions

    refute Enum.any?(result.agent_runs, &(&1.operation_family == :source_admission))

    assert Enum.any?(
             result.agent_runs,
             &(&1.activation_kind == :cron and &1.fresh_import_event == false)
           )

    assert Enum.any?(
             result.agent_runs,
             &(&1.activation_kind == :interval and &1.fresh_import_event == false)
           )

    assert Enum.any?(
             result.agent_runs,
             &(&1.activation_kind == :soup_change and &1.status == :abstained)
           )
  end

  test "T1178 failure and backoff outcomes remain visible and typed" do
    result = EcologyRuntime.run()
    audit = result.audit

    assert audit.failures.lease_conflict
    assert audit.failures.retryable_backoff
    assert audit.failures.validation_failure
    assert audit.failures.terminal_failure
    assert audit.failures.cascade_budget_exhaustion

    assert Enum.any?(result.leases, &(&1.release_outcome == :conflict))
    assert Enum.any?(result.leases, &(&1.release_outcome == :expired))
    assert Enum.any?(result.agent_runs, &(&1.status == :skipped_duplicate))

    refute Enum.any?(
             result.agent_runs,
             &(&1.status == :skipped_duplicate and Map.has_key?(&1, :output_hash))
           )

    assert Enum.any?(
             result.failures,
             &(&1.outcome == :validation_failed and &1.retried_as_transport_failure == false)
           )

    assert Enum.any?(
             result.failures,
             &(&1.outcome == :budget_exhausted and &1.downstream_activations_suppressed)
           )
  end

  test "T1178 requirement coverage records satisfied, unsatisfied, and unproven ids" do
    result = EcologyRuntime.run()
    coverage = result.audit.requirements

    assert "REQ1178-PROOF-02" in coverage.satisfied
    assert "REQ1178-PROOF-03" in coverage.satisfied
    assert coverage.evidence["REQ1178-SCH-10"]
    assert coverage.evidence["REQ1178-SOUP-01"]
    assert coverage.evidence["REQ1178-SOUP-02"]
    assert coverage.evidence["REQ1178-WRITE-03"]
    assert coverage.evidence["AC1178-02"]
    assert coverage.evidence["AC1178-03"]
    assert coverage.evidence["AC1178-07"]
    assert coverage.evidence["VT1178-01"]
    assert coverage.evidence["VT1178-07"]
    assert coverage.evidence["VT1178-08"]
    assert "AC1178-06" in coverage.satisfied
    assert "VT1178-09" in coverage.satisfied
    assert coverage.unsatisfied == []

    assert Enum.any?(
             coverage.unproven,
             &String.contains?(&1, "Production deployment behavior")
           )

    assert coverage.proof_counts.activations >= 5
    assert coverage.proof_counts.agent_runs >= 3
    assert coverage.proof_counts.packets >= 3
    assert coverage.proof_counts.proposals > 0
    assert coverage.proof_counts.mutations > 0
  end
end
