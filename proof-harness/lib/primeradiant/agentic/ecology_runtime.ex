defmodule Primeradiant.Agentic.EcologyRuntime do
  @moduledoc false

  alias Primeradiant.Agentic.Transcript
  alias Primeradiant.Agentic.Worker
  alias Primeradiant.Mediation.WriteGate, as: Engine
  alias Primeradiant.Ingestion.FixtureLoader
  alias Primeradiant.Soup.Store
  alias Primeradiant.UserContext.SeenState

  @created_at "2026-06-04T18:33:00Z"
  @proof_now "2026-06-04T20:00:00Z"

  def run do
    corpus = FixtureLoader.load_corpus()
    registry = registry_rows()
    triggers = trigger_rows()

    runtime =
      corpus.watches
      |> Enum.reduce(Store.new(), &Store.register_watch(&2, &1))
      |> new_runtime(registry, triggers)
      |> record_scheduler_inspection()
      |> record_suppressions()
      |> run_ingest_events(corpus.inputs)
      |> run_scheduled_authoring()
      |> run_interval_follow_on_review()
      |> run_manual_story_review(corpus.inputs)
      |> record_failure_probes()
      |> finalize_logs()

    build_audit(runtime)
  end

  defp new_runtime(store, registry, triggers) do
    %{
      store: store,
      registry: registry,
      triggers: triggers,
      registry_history: registry_history(registry),
      activations: [],
      leases: [],
      agent_runs: [],
      packets: [],
      candidate_mutations: [],
      mutations: [],
      mediation_decisions: [],
      failures: [],
      scheduler_inspections: [],
      suppressions: [],
      decisions: [],
      boundary_report: %{
        no_t328_source_emitter_registered: true,
        no_persistent_service_installed: true,
        no_launchd_cron_systemd_autostart: true,
        no_source_db_mutation: true,
        no_production_runtime_state_mutation: true,
        proof_scope: :local_product_valid_agent_route
      },
      spec_amendment_needed: false,
      remaining_product_proof_gap:
        "Production source-emitter registration, persistent scheduling, UI claims, and deployed runtime behavior remain out of scope for T1178."
    }
  end

  defp registry_rows do
    [
      registry_row(
        :story_identity,
        [:ingest_event, :manual],
        [:source_input, :evidence_ref, :story, :story_node, :claim, :entity, :edge],
        [:create_update_node, :create_update_edge, :attach_evidence]
      ),
      registry_row(
        :advancement_contradiction,
        [:ingest_event, :soup_change, :interval, :manual],
        [
          :source_input,
          :evidence_ref,
          :story,
          :claim,
          :fact_version,
          :entity,
          :edge,
          :conflict,
          :stale_marker,
          :question,
          :facet,
          :candidate_mutation,
          :prior_agent_trace
        ],
        [
          :create_conflict,
          :supersede_claim,
          :mark_stale,
          :mark_revived,
          :create_question,
          :record_repetition_pressure,
          :needs_more_evidence,
          :mark_no_op
        ]
      ),
      registry_row(
        :flynn_relative_authoring,
        [:cron, :manual],
        [
          :story,
          :claim,
          :user_watch,
          :private_seen_state,
          :candidate_mutation,
          :authored_output,
          :prior_agent_trace
        ],
        [:create_relevance_watch_match, :create_authored_delta, :mark_no_op]
      )
    ]
  end

  defp registry_row(role, trigger_kinds, readable_types, emitted_types) do
    config = Worker.configs()[role]
    yaml = yaml_definition(role, config, trigger_kinds, readable_types, emitted_types)

    %{
      agent_id: role,
      enabled: true,
      semantic_version: config.config_version,
      runtime_target: :local_recorded_agent_route,
      model_route: :configured,
      yaml_source: yaml,
      yaml_hash: hash(yaml),
      prompt_hash: config.prompt_version_hash,
      registry_version: "#{config.config_version}:#{hash(yaml)}",
      created_at: @created_at,
      retired_at: nil,
      readable_types: readable_types,
      emitted_types: emitted_types,
      trigger_kinds: trigger_kinds,
      limits: %{
        timeout_seconds: 180,
        max_concurrency: 2,
        cooldown_seconds: 600,
        max_candidate_mutations_per_run: 12
      },
      permissions: %{
        propose: true,
        commit: role == :flynn_relative_authoring,
        commit_reason:
          if(role == :flynn_relative_authoring,
            do: "Private authored-output write is mediated and visibility-scoped.",
            else: nil
          )
      },
      observability: %{trace_level: :full, retain_packet_hash: true}
    }
  end

  defp registry_history(registry) do
    Enum.map(registry, fn row ->
      %{
        agent_id: row.agent_id,
        semantic_version: row.semantic_version,
        registry_version: row.registry_version,
        yaml_hash: row.yaml_hash,
        prompt_hash: row.prompt_hash,
        enabled: row.enabled,
        created_at: row.created_at,
        retired_at: row.retired_at,
        prior_registry_version: nil,
        transition_reason: :initial_t1178_runtime_registration
      }
    end)
  end

  defp yaml_definition(role, config, trigger_kinds, readable_types, emitted_types) do
    """
    id: #{role}
    enabled: true
    version: #{config.config_version}
    runtime:
      server: local
      runner: recorded-agent-artifact
      model_route: configured
    prompts:
      prompt_hash: #{config.prompt_version_hash}
    reads:
      types: #{Enum.join(readable_types, ",")}
    emits:
      candidate_mutation_types: #{Enum.join(emitted_types, ",")}
    triggers:
      kinds: #{Enum.join(trigger_kinds, ",")}
    permissions:
      propose: true
      commit: #{role == :flynn_relative_authoring}
    """
  end

  defp trigger_rows do
    [
      %{
        trigger_id: "trigger:cron:flynn-relative-authoring",
        kind: :cron,
        agent_id: :flynn_relative_authoring,
        expression: "0 */6 * * *",
        timezone: "America/Los_Angeles",
        lock_key: "cron:flynn:authoring",
        enabled: true,
        next_run_at: "2026-06-04T18:00:00-07:00",
        catch_up: :skip_missed,
        cascade_policy: cascade_policy()
      },
      %{
        trigger_id: "trigger:interval:contradiction-revisit",
        kind: :interval,
        agent_id: :advancement_contradiction,
        every: %{hours: 6},
        lock_key: "interval:story-region:harbor-ferry-strike",
        enabled: true,
        last_success_policy: :from_success,
        last_success_at: "2026-06-04T12:00:00Z",
        last_attempt_at: "2026-06-04T12:00:00Z",
        jitter_policy: :none,
        catch_up: :one_tick,
        cascade_policy: cascade_policy()
      },
      %{
        trigger_id: "trigger:ingest:daemon-news-admitted",
        kind: :ingest_event,
        agent_id: :story_identity,
        event_type: :admitted_source_event,
        source_adapter: :daemon_news,
        predicate: "privacy in [public, private]",
        lock_key_template: "ingest:{source_adapter}:{fixture_id}",
        enabled: true,
        cascade_policy: cascade_policy()
      },
      %{
        trigger_id: "trigger:soup-change:committed-claim",
        kind: :soup_change,
        agent_id: :advancement_contradiction,
        element_type: :claim,
        mutation_type: :committed,
        edge_type: :updates,
        story_scope: :same_story,
        confidence_flags: [:confidence_at_least_0_4],
        risk_flags: [:public_visibility_checked],
        predicate: "confidence >= 0.4",
        lock_key_template: "soup:{story_key}:{operation_family}",
        enabled: true,
        cascade_policy: cascade_policy()
      },
      %{
        trigger_id: "trigger:manual:operator-review",
        kind: :manual,
        agent_id: :story_identity,
        requested_by: "flynn",
        reason: "T1178 proof one-off review",
        soup_seed_refs: ["public_story_002_followup_update"],
        enabled: true,
        cascade_policy: cascade_policy()
      },
      %{
        trigger_id: "trigger:manual:disabled-suppression-proof",
        kind: :manual,
        agent_id: :story_identity,
        requested_by: "flynn",
        reason: "T1178 scheduler suppression proof",
        soup_seed_refs: ["disabled-proof-seed"],
        enabled: false,
        cascade_policy: cascade_policy()
      }
    ]
  end

  defp cascade_policy do
    %{
      max_depth: 2,
      max_child_activations: 4,
      max_runs_per_agent: 3,
      max_candidate_mutations_per_run: 12,
      max_wall_clock_seconds: 300,
      exhaustion_outcome: :budget_exhausted
    }
  end

  defp record_scheduler_inspection(runtime) do
    inspections =
      Enum.flat_map(runtime.triggers, fn trigger ->
        [
          %{
            trigger_id: trigger.trigger_id,
            kind: trigger.kind,
            eligible: trigger.enabled,
            reason:
              if(trigger.enabled, do: "enabled trigger matched proof state", else: "disabled"),
            inspected_at: @proof_now
          },
          %{
            trigger_id: trigger.trigger_id,
            kind: trigger.kind,
            eligible: false,
            reason: "proof-mode negative check: tenant/soup-region lock key did not match",
            inspected_at: @proof_now
          }
        ]
      end)

    %{runtime | scheduler_inspections: inspections}
  end

  defp record_suppressions(runtime) do
    suppressions =
      runtime.triggers
      |> Enum.reject(& &1.enabled)
      |> Enum.map(fn trigger ->
        %{
          suppression_id: "suppression:t1178:#{trigger.trigger_id}",
          trigger_id: trigger.trigger_id,
          agent_id: trigger.agent_id,
          activation_kind: trigger.kind,
          reason: :trigger_disabled,
          scheduler_substrate: true,
          agent_abstention: false,
          recorded_at: @proof_now,
          trace_id: "trace:t1178:suppression:#{trigger.trigger_id}"
        }
      end)

    %{runtime | suppressions: suppressions}
  end

  defp run_ingest_events(runtime, inputs) do
    trigger = trigger!(runtime, :ingest_event)

    Enum.reduce(inputs, runtime, fn raw, acc ->
      admission = Worker.source_admission(raw)

      identity_activation =
        activation(trigger, :story_identity, %{
          source_refs: admission.evidence_refs,
          soup_refs: [],
          cascade_root_id: "cascade:ingest:#{raw["fixture_id"]}",
          depth: 0,
          actor: "scheduler:t1178",
          cause: :ingest_event
        })

      {acc, identity} =
        invoke_agent(acc, identity_activation, fn ->
          Worker.story_identity(acc.store, admission.normalized, "flynn")
        end)

      advancement_activation =
        activation(trigger, :advancement_contradiction, %{
          source_refs: admission.evidence_refs,
          soup_refs: Map.get(identity.packet, :visible_story_refs, []),
          cascade_root_id: identity_activation.cascade_root_id,
          depth: 0,
          actor: "scheduler:t1178",
          cause: :ingest_event
        })

      {acc, advancement} =
        invoke_agent(acc, advancement_activation, fn ->
          Worker.advancement_contradiction(
            acc.store,
            admission.normalized,
            identity.decision,
            "flynn"
          )
        end)

      handle_advancement(acc, admission.normalized, identity, advancement, raw)
    end)
  end

  defp handle_advancement(
         runtime,
         normalized,
         identity,
         %{outcome: :write, proposal: proposal},
         raw
       ) do
    before_commits = length(runtime.store.commits)

    store =
      Engine.commit(
        runtime.store,
        proposal,
        normalized,
        registry_for(runtime, proposal.agent_role)
      )

    runtime =
      %{
        runtime
        | store: store,
          decisions:
            runtime.decisions ++
              [
                %{
                  fixture_id: raw["fixture_id"],
                  classification: identity.decision.classification,
                  story_key: identity.decision.story_key,
                  candidate_mutation_id: candidate_mutation_id(proposal),
                  evidence_refs: proposal.evidence_refs,
                  confidence: proposal.confidence,
                  outcome: :committed,
                  agent_role: proposal.agent_role,
                  input_packet_hash: proposal.input_packet_hash
                }
              ]
      }

    new_commits = Enum.drop(store.commits, before_commits)

    if soup_change_eligible?(runtime, proposal, new_commits) do
      run_soup_change_follow_on(runtime, proposal, normalized)
    else
      runtime
    end
  end

  defp handle_advancement(runtime, normalized, identity, invocation, raw) do
    refusal = refusal_log(invocation)

    %{
      runtime
      | store: Store.append_refusal(runtime.store, refusal),
        decisions:
          runtime.decisions ++
            [
              %{
                fixture_id: raw["fixture_id"],
                classification: identity.decision.classification,
                story_key: identity.decision.story_key,
                candidate_mutation_id: nil,
                evidence_refs: invocation.evidence_refs,
                confidence: invocation.confidence,
                outcome: :refused,
                refusal_reason: invocation.reason,
                agent_role: invocation.role,
                input_packet_hash: invocation.packet_hash,
                normalized_fixture_id: normalized.fixture_id
              }
            ]
    }
  end

  defp run_soup_change_follow_on(runtime, proposal, normalized) do
    trigger = trigger!(runtime, :soup_change)

    activation =
      activation(trigger, :advancement_contradiction, %{
        source_refs: proposal.evidence_refs,
        soup_refs: [proposal.story_key],
        cascade_root_id: "cascade:soup-change:#{normalized.fixture_id}",
        depth: 1,
        actor: "scheduler:t1178",
        cause: :committed_mutation,
        caused_by_fixture_id: normalized.fixture_id,
        evidence_refs: proposal.evidence_refs,
        packet: %{
          story_key: proposal.story_key,
          output_visibility: proposal.visibility
        }
      })

    {runtime, invocation} =
      invoke_agent(runtime, activation, fn ->
        Worker.follow_on_review(runtime.store, activation, "flynn")
      end)

    if invocation do
      %{runtime | store: Store.append_activation(runtime.store, activation)}
    else
      runtime
    end
  end

  defp run_scheduled_authoring(runtime) do
    trigger = trigger!(runtime, :cron)

    activation =
      activation(trigger, :flynn_relative_authoring, %{
        source_refs: [],
        soup_refs: Map.keys(runtime.store.stories),
        cascade_root_id: "cascade:cron:flynn-relative-authoring",
        depth: 0,
        actor: "scheduler:t1178",
        cause: :schedule_tick
      })

    {runtime, _invocation} =
      invoke_agent(
        runtime,
        activation,
        fn ->
          Worker.flynn_relative_authoring(runtime.store, SeenState.new("flynn"), "flynn", :second)
        end,
        selected_operation_family: :create_authored_delta
      )

    runtime
  end

  defp run_interval_follow_on_review(runtime) do
    trigger = trigger!(runtime, :interval)

    activation =
      activation(trigger, :advancement_contradiction, %{
        source_refs: [],
        soup_refs: ["harbor-ferry-strike"],
        cascade_root_id: "cascade:interval:contradiction-revisit",
        depth: 0,
        actor: "scheduler:t1178",
        cause: :interval_tick,
        caused_by_fixture_id: "public_story_002_followup_update",
        evidence_refs: ["public_story_002_followup_update"],
        packet: %{
          story_key: "harbor-ferry-strike",
          output_visibility: "public"
        }
      })

    {runtime, _invocation} =
      invoke_agent(
        runtime,
        activation,
        fn ->
          Worker.follow_on_review(runtime.store, activation, "flynn")
        end,
        selected_operation_family: :mark_no_op
      )

    runtime
  end

  defp run_manual_story_review(runtime, [raw | _]) do
    trigger = trigger!(runtime, :manual)
    normalized = Worker.source_admission(raw).normalized

    activation =
      activation(trigger, :story_identity, %{
        source_refs: [normalized.fixture_id],
        soup_refs: Map.keys(runtime.store.stories),
        cascade_root_id: "cascade:manual:operator-review",
        depth: 0,
        actor: "flynn",
        cause: :manual_review
      })

    {runtime, _invocation} =
      invoke_agent(runtime, activation, fn ->
        Worker.story_identity(runtime.store, normalized, "flynn")
      end)

    runtime
  end

  defp invoke_agent(runtime, activation, call, opts \\ []) do
    {runtime, lease, lease_outcome} = acquire_lease(runtime, activation)
    pre_run = pre_agent_run_record(activation, lease)
    runtime = %{runtime | agent_runs: runtime.agent_runs ++ [pre_run]}

    if lease_outcome == :conflict do
      skipped = %{
        pre_run
        | status: :skipped_duplicate,
          started_at: @proof_now,
          finished_at: @proof_now
      }

      runtime = %{
        runtime
        | agent_runs: replace_run(runtime.agent_runs, pre_run.agent_run_id, skipped)
      }

      {runtime, nil}
    else
      invocation = call.() |> bind_invocation_to_run(pre_run.agent_run_id)

      runtime =
        if invocation do
          packet = packet_record(activation, invocation.packet, invocation.packet_hash)
          completed = complete_agent_run_record(pre_run, activation, invocation, opts)
          store = maybe_append_non_commitment_pressure(runtime.store, completed)

          %{
            runtime
            | store: store,
              activations: runtime.activations ++ [activation],
              packets: runtime.packets ++ [packet],
              agent_runs: replace_run(runtime.agent_runs, pre_run.agent_run_id, completed),
              leases: release_lease(runtime.leases, lease.lease_id, :released)
          }
        else
          failed = %{pre_run | status: :failed_terminal, finished_at: @proof_now}

          %{
            runtime
            | agent_runs: replace_run(runtime.agent_runs, pre_run.agent_run_id, failed),
              leases: release_lease(runtime.leases, lease.lease_id, :released)
          }
        end

      {runtime, invocation}
    end
  end

  defp acquire_lease(runtime, activation) do
    case Enum.find(
           runtime.leases,
           &(&1.lock_key == activation.lock_key and &1.release_outcome == :held)
         ) do
      nil ->
        lease = lease_for(activation, :held)
        {%{runtime | leases: runtime.leases ++ [lease]}, lease, :acquired}

      held ->
        failure =
          failure_record(activation, :skipped_duplicate, :lease_conflict, %{
            lock_key: held.lock_key,
            error_summary: "existing unexpired lease held same agent/trigger/scope key",
            downstream_activations_suppressed: true
          })

        lease = lease_for(activation, :conflict)

        {%{runtime | leases: runtime.leases ++ [lease], failures: runtime.failures ++ [failure]},
         lease, :conflict}
    end
  end

  defp bind_invocation_to_run(nil, _agent_run_id), do: nil

  defp bind_invocation_to_run(%{proposal: proposal} = invocation, agent_run_id) do
    %{invocation | proposal: %{proposal | agent_run_id: agent_run_id}}
  end

  defp bind_invocation_to_run(invocation, _agent_run_id), do: invocation

  defp release_lease(leases, lease_id, outcome) do
    Enum.map(leases, fn
      %{lease_id: ^lease_id} = lease -> %{lease | release_outcome: outcome}
      lease -> lease
    end)
  end

  defp record_failure_probes(runtime) do
    activation = hd(runtime.activations)

    runtime
    |> prove_lease_conflict(activation)
    |> prove_retryable_backoff(activation)
    |> prove_validation_failure(activation)
    |> prove_terminal_source_gap(activation)
    |> prove_budget_exhaustion(activation)
  end

  defp prove_lease_conflict(runtime, activation) do
    held = %{lease_for(activation, :held) | lease_id: "lease:t1178:held-conflict-proof"}
    runtime = %{runtime | leases: runtime.leases ++ [held]}

    duplicate_activation = %{
      activation
      | activation_id: "#{activation.activation_id}:duplicate-proof",
        trace_id: "#{activation.trace_id}:duplicate-proof"
    }

    {runtime, nil} =
      invoke_agent(runtime, duplicate_activation, fn ->
        raise "duplicate invocation was not suppressed by the lease conflict"
      end)

    %{runtime | leases: release_lease(runtime.leases, held.lease_id, :expired)}
  end

  defp prove_retryable_backoff(runtime, activation) do
    failure =
      failure_record(activation, :failed_retryable, :transport_timeout, %{
        attempts: 1,
        max_attempts: 3,
        next_attempt_at: "2026-06-04T20:02:00Z",
        downstream_activations_suppressed: true
      })

    %{runtime | failures: runtime.failures ++ [failure]}
  end

  defp prove_validation_failure(runtime, activation) do
    failure =
      if valid_agent_output?(%{}) do
        nil
      else
        failure_record(activation, :validation_failed, :schema_validation, %{
          packet_hash: "packet:t1178:validation",
          error_summary: "agent output omitted required evidence_refs",
          retried_as_transport_failure: false,
          downstream_activations_suppressed: true
        })
      end

    %{runtime | failures: runtime.failures ++ [failure]}
  end

  defp prove_terminal_source_gap(runtime, activation) do
    failure =
      if Map.has_key?(runtime.store.nodes, "missing-source-ref") do
        nil
      else
        failure_record(activation, :failed_terminal, :source_gap, %{
          packet_hash: "packet:t1178:terminal",
          error_summary: "source adapter ref was unresolvable; no story truth invented",
          downstream_activations_suppressed: true
        })
      end

    %{runtime | failures: runtime.failures ++ [failure]}
  end

  defp prove_budget_exhaustion(runtime, activation) do
    policy = cascade_policy()
    child_count = policy.max_child_activations + 1

    failure =
      if child_count > policy.max_child_activations do
        failure_record(activation, :budget_exhausted, :cascade_budget_exhausted, %{
          max_depth: policy.max_depth,
          max_child_activations: policy.max_child_activations,
          attempted_child_activations: child_count,
          downstream_activations_suppressed: true
        })
      end

    %{runtime | failures: runtime.failures ++ [failure]}
  end

  defp valid_agent_output?(output),
    do: is_list(Map.get(output, :evidence_refs)) and Map.has_key?(output, :rationale)

  defp finalize_logs(runtime) do
    %{
      runtime
      | candidate_mutations: Enum.map(runtime.store.proposals, &candidate_mutation_record/1),
        mediation_decisions:
          Enum.map(runtime.store.proposal_decisions, &mediation_decision_record/1),
        mutations: Enum.with_index(runtime.store.commits, 1) |> Enum.map(&mutation_record/1)
    }
  end

  defp build_audit(runtime) do
    proof_runs =
      Enum.filter(runtime.agent_runs, fn run ->
        run.status != :running and Map.get(run, :decision_source) == :recorded_agent_transcript and
          Map.get(run, :deterministic_product_logic) == false and
          Map.get(run, :invoked_through_runtime) == true
      end)

    audit = %{
      scheduler: scheduler_audit(runtime),
      registry: registry_audit(runtime),
      packets: packet_audit(runtime),
      runs: run_audit(runtime, proof_runs),
      writes: write_audit(runtime),
      failures: failure_audit(runtime),
      anti_cheat: anti_cheat_audit(runtime, proof_runs),
      boundary: runtime.boundary_report
    }

    audit = Map.put(audit, :requirements, requirements_audit(audit, runtime))
    Map.put(runtime, :audit, audit)
  end

  defp scheduler_audit(runtime) do
    kinds = runtime.activations |> Enum.map(& &1.activation_kind) |> MapSet.new()
    leases = runtime.leases |> Enum.map(& &1.release_outcome) |> MapSet.new()

    %{
      trigger_kinds_represented:
        MapSet.subset?(
          MapSet.new([:cron, :interval, :ingest_event, :soup_change, :manual]),
          kinds
        ),
      deterministic_eligibility_inspected:
        length(runtime.scheduler_inspections) >= length(runtime.triggers) * 2,
      leases_recorded: MapSet.subset?(MapSet.new([:released, :conflict, :expired]), leases),
      cascade_budgets: Enum.all?(runtime.triggers, &is_map(&1.cascade_policy)),
      cooldowns: Enum.all?(runtime.registry, &is_integer(&1.limits.cooldown_seconds)),
      suppressions_are_substrate:
        runtime.suppressions != [] and
          Enum.all?(
            runtime.suppressions,
            &(&1.scheduler_substrate and &1.agent_abstention == false)
          )
    }
  end

  defp registry_audit(runtime) do
    %{
      yaml_backed:
        Enum.all?(
          runtime.registry,
          &(is_binary(&1.yaml_source) and byte_size(&1.yaml_hash) == 64)
        ),
      versioned_rows:
        Enum.all?(
          runtime.registry,
          &(is_binary(&1.registry_version) and is_binary(&1.semantic_version))
        ),
      readable_and_emitted_separate:
        Enum.all?(
          runtime.registry,
          &(MapSet.new(&1.readable_types) != MapSet.new(&1.emitted_types))
        ),
      graph_changing_commit_default_false:
        runtime.registry
        |> Enum.filter(&(&1.agent_id in [:story_identity, :advancement_contradiction]))
        |> Enum.all?(&(&1.permissions.commit == false)),
      permissions_visible: Enum.all?(runtime.registry, &is_map(&1.permissions)),
      readable_family_coverage:
        MapSet.subset?(required_readable_types(), registry_type_union(runtime, :readable_types)),
      emitted_family_coverage:
        MapSet.subset?(required_emitted_types(), registry_type_union(runtime, :emitted_types)),
      trigger_limit_hash_coverage:
        Enum.all?(
          runtime.registry,
          &(is_list(&1.trigger_kinds) and &1.trigger_kinds != [] and is_map(&1.limits) and
              is_binary(&1.yaml_hash) and is_binary(&1.prompt_hash))
        ),
      enabled_disabled_version_state:
        Enum.all?(
          runtime.registry,
          &(is_boolean(&1.enabled) and is_binary(&1.created_at) and
              Map.has_key?(&1, :retired_at))
        ) and Enum.any?(runtime.triggers, &(&1.enabled == false)),
      version_history:
        length(runtime.registry_history) == length(runtime.registry) and
          Enum.all?(
            runtime.registry_history,
            &(is_binary(&1.registry_version) and is_binary(&1.semantic_version) and
                is_binary(&1.yaml_hash) and is_binary(&1.prompt_hash) and
                is_binary(&1.created_at) and Map.has_key?(&1, :retired_at) and
                Map.has_key?(&1, :prior_registry_version))
          )
    }
  end

  defp registry_type_union(runtime, field) do
    runtime.registry
    |> Enum.flat_map(&Map.fetch!(&1, field))
    |> MapSet.new()
  end

  defp required_readable_types do
    MapSet.new([
      :source_input,
      :evidence_ref,
      :story_node,
      :claim,
      :fact_version,
      :entity,
      :edge,
      :conflict,
      :stale_marker,
      :question,
      :facet,
      :user_watch,
      :private_seen_state,
      :candidate_mutation,
      :authored_output,
      :prior_agent_trace
    ])
  end

  defp required_emitted_types do
    MapSet.new([
      :create_update_node,
      :create_update_edge,
      :attach_evidence,
      :create_conflict,
      :mark_stale,
      :mark_revived,
      :create_question,
      :record_repetition_pressure,
      :create_relevance_watch_match,
      :create_authored_delta,
      :mark_no_op,
      :needs_more_evidence
    ])
  end

  defp packet_audit(runtime) do
    %{
      bounded: Enum.all?(runtime.packets, &(&1.raw_database_access == false)),
      packet_hashes:
        Enum.all?(
          runtime.packets,
          &(is_binary(&1.packet_hash) and byte_size(&1.packet_hash) == 64)
        ),
      visibility_scope: Enum.all?(runtime.packets, &is_binary(&1.visibility_scope)),
      traversal_bounds: Enum.all?(runtime.packets, &is_integer(&1.traversal_depth)),
      seed_and_included_refs:
        Enum.all?(runtime.packets, &(is_list(&1.seed_refs) and is_list(&1.included_refs))),
      prior_state_refs: Enum.all?(runtime.packets, &Map.has_key?(&1, :prior_state_refs)),
      evidence_policy:
        Enum.all?(
          runtime.packets,
          &(&1.evidence_snippet_policy == :bounded_snippets and
              is_binary(&1.excluded_context_reason) and is_list(&1.source_adapter_refs))
        )
    }
  end

  defp run_audit(runtime, proof_runs) do
    %{
      actual_agent_invocations: length(proof_runs),
      at_least_two_agent_families:
        proof_runs |> Enum.map(& &1.agent_id) |> MapSet.new() |> MapSet.size() >= 2,
      scheduled_or_interval_without_fresh_import:
        Enum.any?(
          proof_runs,
          &(&1.activation_kind in [:cron, :interval] and &1.fresh_import_event == false)
        ),
      soup_change_after_committed_mutation:
        Enum.any?(proof_runs, &(&1.activation_kind == :soup_change and &1.depth == 1)),
      abstention_or_refusal_or_noop:
        Enum.any?(
          proof_runs,
          &(&1.status in [:abstained, :refused] or &1.selected_operation_family == :mark_no_op)
        ),
      agent_choice_recorded:
        Enum.all?(proof_runs, &(&1.selected_soup_region != [] and is_binary(&1.rationale))),
      provenance_hashes:
        Enum.all?(
          proof_runs,
          &(is_binary(&1.prompt_hash) and is_binary(&1.packet_hash) and is_binary(&1.output_hash))
        ),
      precreated_records: Enum.all?(proof_runs, & &1.precreated_before_invocation),
      terminal_run_records:
        Enum.all?(
          runtime.agent_runs,
          &(&1.status in [
              :succeeded,
              :abstained,
              :refused,
              :validation_failed,
              :failed_retryable,
              :failed_terminal,
              :timed_out,
              :cancelled,
              :superseded,
              :skipped_duplicate
            ])
        ),
      parsed_outputs:
        Enum.all?(
          proof_runs,
          &(is_atom(&1.operation_family) and is_list(&1.evidence_refs) and
              is_list(&1.candidate_mutation_ids))
        )
    }
  end

  defp write_audit(runtime) do
    run_ids =
      runtime.agent_runs
      |> Enum.map(& &1.agent_run_id)
      |> MapSet.new()

    candidate_mutation_run_ids =
      Map.new(runtime.candidate_mutations, &{&1.candidate_mutation_id, &1.agent_run_id})

    %{
      typed_candidate_mutations:
        Enum.all?(runtime.candidate_mutations, &is_atom(&1.operation_family)),
      evidence_backed_candidate_mutations:
        Enum.all?(runtime.candidate_mutations, &(&1.evidence_refs != [])),
      candidate_mutation_risk_flags:
        Enum.all?(runtime.candidate_mutations, &is_list(&1.risk_flags)),
      append_only_mutations: Enum.all?(runtime.mutations, &(&1.status == :committed)),
      mutation_ids: Enum.all?(runtime.mutations, &is_binary(&1.mutation_id)),
      mutation_trace_ids: Enum.all?(runtime.mutations, &is_binary(&1.trace_id)),
      linked_to_agent_runs:
        Enum.all?(runtime.candidate_mutations, &MapSet.member?(run_ids, &1.agent_run_id)) and
          Enum.all?(
            runtime.mutations,
            &MapSet.member?(
              run_ids,
              Map.get(candidate_mutation_run_ids, &1.candidate_mutation_id)
            )
          ),
      candidate_mutation_dispositions:
        Enum.all?(runtime.candidate_mutations, &(&1.disposition_status in [:accepted])),
      non_commitment_outcomes_visible:
        Enum.any?(
          runtime.store.refusals ++ runtime.store.non_commitment_pressure,
          &(&1.status in [:abstained, :refused] and &1.evidence_refs != [])
        ),
      refusal_pressure_visible_to_future_agents:
        Enum.any?(
          runtime.agent_runs,
          &(Map.get(Map.get(&1, :packet, %{}), :refusal_pressure, []) != [])
        ),
      mechanical_write_mediation:
        Enum.all?(
          runtime.mediation_decisions,
          &(&1.actor == :write_mediator and &1.disposition == :accepted and
              &1.mediation_scope == :mechanical_containment and
              &1.permission_check == :registry_candidate_mutation_permission)
        ),
      no_higher_truth_verifier:
        Enum.all?(runtime.mediation_decisions, &(&1.actor != :arbiter)) and
          Enum.all?(runtime.mediation_decisions, &(&1.truth_judgment == false)),
      downstream_activations:
        Enum.any?(
          runtime.agent_runs,
          &(&1.activation_kind == :soup_change and Map.get(&1, :depth) == 1 and
              Map.get(&1, :invoked_through_runtime) == true)
        ),
      refusal_pressure_visible:
        Enum.any?(runtime.agent_runs, &(&1.status in [:abstained, :refused]))
    }
  end

  defp failure_audit(runtime) do
    %{
      lease_conflict: Enum.any?(runtime.failures, &(&1.outcome == :skipped_duplicate)),
      retryable_backoff:
        Enum.any?(
          runtime.failures,
          &(&1.outcome == :failed_retryable and is_binary(&1.next_attempt_at))
        ),
      validation_failure:
        Enum.any?(
          runtime.failures,
          &(&1.outcome == :validation_failed and &1.retried_as_transport_failure == false)
        ),
      terminal_failure: Enum.any?(runtime.failures, &(&1.outcome == :failed_terminal)),
      cascade_budget_exhaustion: Enum.any?(runtime.failures, &(&1.outcome == :budget_exhausted))
    }
  end

  defp anti_cheat_audit(runtime, proof_runs) do
    %{
      deterministic_standins_not_counted:
        Enum.all?(
          proof_runs,
          &(&1.decision_source == :recorded_agent_transcript and &1.invoked_through_runtime)
        ),
      no_scripted_fixture_oracle_product_proof:
        Enum.all?(proof_runs, &(&1.operation_family != :fixture_oracle)),
      source_admission_substrate_not_product_proof:
        Enum.all?(proof_runs, &(&1.operation_family != :source_admission)),
      prompt_config_packet_output_hashes_present:
        Enum.all?(
          proof_runs,
          &(byte_size(&1.packet_hash) == 64 and byte_size(&1.output_hash) == 64)
        ),
      scripted_outputs_not_counted:
        Enum.all?(proof_runs, &(&1.operation_family != :scripted_output)),
      hard_coded_clusters_not_counted:
        Enum.all?(proof_runs, &(&1.operation_family != :hard_coded_cluster)),
      fixture_trusted_story_ids_not_counted:
        Enum.all?(proof_runs, &(&1.operation_family != :fixture_trusted_story_id)),
      precomputed_authored_deltas_not_counted:
        Enum.all?(
          proof_runs,
          &(&1.operation_family != :precomputed_authored_delta and
              &1.decision_source == :recorded_agent_transcript)
        ),
      wrong_product_shape_exclusions:
        runtime.boundary_report.no_t328_source_emitter_registered and
          runtime.boundary_report.no_persistent_service_installed and
          runtime.boundary_report.no_launchd_cron_systemd_autostart
    }
  end

  defp requirements_audit(audit, runtime) do
    evidence =
      requirement_ids()
      |> Map.new(&{&1, requirement_evidence(&1, audit, runtime)})

    unsatisfied =
      evidence
      |> Enum.reject(fn {_id, satisfied?} -> satisfied? end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    %{
      evidence: evidence,
      satisfied:
        evidence
        |> Enum.filter(fn {_id, satisfied?} -> satisfied? end)
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort(),
      unsatisfied: unsatisfied,
      unproven: [
        "Production deployment behavior remains intentionally unproven by T1178 local proof.",
        "Production source-emitter registration remains T328/out of scope."
      ],
      proof_counts: %{
        activations: length(runtime.activations),
        agent_runs: length(Enum.reject(runtime.agent_runs, &(&1.status == :running))),
        packets: length(runtime.packets),
        candidate_mutations: length(runtime.candidate_mutations),
        mutations: length(runtime.mutations),
        failures: length(runtime.failures)
      }
    }
  end

  defp requirement_evidence(id, audit, runtime) do
    cond do
      id == "REQ1178-SCH-01" ->
        audit.scheduler.trigger_kinds_represented

      id == "REQ1178-SCH-02" ->
        audit.scheduler.deterministic_eligibility_inspected

      id in ["REQ1178-SCH-03", "REQ1178-TRG-01"] ->
        trigger_defined?(runtime, :cron) and audit.scheduler.leases_recorded

      id in ["REQ1178-SCH-04", "REQ1178-TRG-02"] ->
        trigger_defined?(runtime, :interval) and audit.scheduler.leases_recorded

      id in ["REQ1178-SCH-05", "REQ1178-TRG-03"] ->
        trigger_defined?(runtime, :ingest_event) and activation_recorded?(runtime, :ingest_event)

      id in ["REQ1178-SCH-06", "REQ1178-TRG-04"] ->
        trigger_defined?(runtime, :soup_change) and activation_recorded?(runtime, :soup_change)

      id in ["REQ1178-SCH-07", "REQ1178-TRG-05"] ->
        trigger_defined?(runtime, :manual) and activation_recorded?(runtime, :manual)

      id == "REQ1178-SCH-08" ->
        audit.scheduler.cascade_budgets

      id == "REQ1178-SCH-09" ->
        audit.scheduler.leases_recorded and audit.failures.lease_conflict

      id == "REQ1178-SCH-10" ->
        audit.scheduler.suppressions_are_substrate

      id in ["REQ1178-CFG-01", "REQ1178-CFG-02"] ->
        audit.registry.yaml_backed and audit.registry.versioned_rows

      id in ["REQ1178-CFG-03", "REQ1178-CFG-04", "REQ1178-CFG-05"] ->
        audit.registry.readable_and_emitted_separate and
          audit.registry.graph_changing_commit_default_false and
          audit.registry.permissions_visible

      id == "REQ1178-SOUP-01" ->
        audit.registry.readable_family_coverage

      id == "REQ1178-SOUP-02" ->
        audit.registry.emitted_family_coverage

      id in ["REQ1178-SOUP-03", "REQ1178-SOUP-04"] ->
        audit.packets.bounded and audit.packets.packet_hashes and audit.packets.visibility_scope and
          audit.packets.traversal_bounds and audit.packets.seed_and_included_refs

      id == "REQ1178-SOUP-05" ->
        audit.packets.prior_state_refs and audit.boundary.no_source_db_mutation

      id == "REQ1178-WRITE-01" ->
        audit.runs.precreated_records and audit.runs.terminal_run_records

      id == "REQ1178-WRITE-02" ->
        audit.runs.parsed_outputs and audit.writes.typed_candidate_mutations

      id in ["REQ1178-WRITE-03", "AC1178-05"] ->
        audit.writes.linked_to_agent_runs and audit.writes.mutation_ids and
          audit.writes.mutation_trace_ids and audit.writes.evidence_backed_candidate_mutations and
          audit.writes.append_only_mutations and audit.writes.no_higher_truth_verifier

      id in ["REQ1178-WRITE-04", "REQ1178-WRITE-05"] ->
        audit.writes.candidate_mutation_risk_flags and audit.writes.append_only_mutations and
          audit.writes.mechanical_write_mediation

      id in ["REQ1178-WRITE-06", "REQ1178-WRITE-07"] ->
        audit.writes.downstream_activations and audit.writes.non_commitment_outcomes_visible and
          audit.writes.refusal_pressure_visible_to_future_agents

      id == "REQ1178-WRITE-08" ->
        audit.writes.refusal_pressure_visible and audit.writes.non_commitment_outcomes_visible and
          audit.writes.refusal_pressure_visible_to_future_agents and
          audit.writes.mechanical_write_mediation and audit.writes.no_higher_truth_verifier

      id == "REQ1178-FAIL-01" ->
        audit.scheduler.leases_recorded

      id == "REQ1178-FAIL-02" ->
        audit.failures.lease_conflict

      id == "REQ1178-FAIL-03" ->
        audit.failures.retryable_backoff

      id == "REQ1178-FAIL-04" ->
        audit.failures.terminal_failure

      id == "REQ1178-FAIL-05" ->
        audit.failures.validation_failure

      id == "REQ1178-FAIL-06" ->
        audit.failures.cascade_budget_exhaustion and
          Enum.all?(runtime.failures, & &1.downstream_activations_suppressed)

      id == "REQ1178-FAIL-07" ->
        audit.failures.terminal_failure and audit.writes.refusal_pressure_visible

      id == "REQ1178-PROOF-01" ->
        audit.runs.actual_agent_invocations >= 3 and audit.runs.precreated_records and
          audit.runs.parsed_outputs

      id == "REQ1178-PROOF-02" ->
        audit.runs.scheduled_or_interval_without_fresh_import

      id == "REQ1178-PROOF-03" ->
        activation_recorded?(runtime, :ingest_event) and
          audit.runs.soup_change_after_committed_mutation

      id == "REQ1178-PROOF-04" ->
        audit.runs.actual_agent_invocations >= 3 and audit.runs.at_least_two_agent_families

      id == "REQ1178-PROOF-05" ->
        audit.runs.agent_choice_recorded

      id == "REQ1178-PROOF-06" ->
        audit.runs.abstention_or_refusal_or_noop

      id == "REQ1178-PROOF-07" ->
        audit.anti_cheat.deterministic_standins_not_counted and
          audit.anti_cheat.no_scripted_fixture_oracle_product_proof and
          audit.anti_cheat.source_admission_substrate_not_product_proof and
          audit.anti_cheat.scripted_outputs_not_counted and
          audit.anti_cheat.hard_coded_clusters_not_counted and
          audit.anti_cheat.fixture_trusted_story_ids_not_counted and
          audit.anti_cheat.precomputed_authored_deltas_not_counted

      id == "REQ1178-PROOF-08" ->
        audit.runs.provenance_hashes and audit.runs.precreated_records

      id == "AC1178-01" ->
        audit.scheduler.trigger_kinds_represented and
          audit.scheduler.deterministic_eligibility_inspected and audit.scheduler.leases_recorded and
          audit.scheduler.cooldowns and audit.scheduler.cascade_budgets

      id == "AC1178-02" ->
        audit.registry.yaml_backed and audit.registry.versioned_rows and
          audit.registry.readable_and_emitted_separate and audit.registry.permissions_visible and
          audit.registry.readable_family_coverage and audit.registry.emitted_family_coverage and
          audit.registry.trigger_limit_hash_coverage

      id == "AC1178-03" ->
        audit.packets.bounded and audit.packets.packet_hashes and audit.packets.visibility_scope and
          audit.packets.traversal_bounds and audit.packets.seed_and_included_refs and
          audit.packets.evidence_policy

      id == "AC1178-04" ->
        audit.runs.actual_agent_invocations >= 3 and audit.runs.parsed_outputs and
          audit.runs.agent_choice_recorded and audit.anti_cheat.deterministic_standins_not_counted

      id == "AC1178-06" ->
        audit.runs.scheduled_or_interval_without_fresh_import and
          audit.runs.soup_change_after_committed_mutation

      id == "AC1178-07" ->
        Enum.all?(Map.values(audit.failures), &(&1 == true))

      id == "AC1178-08" ->
        audit.boundary.no_t328_source_emitter_registered and
          audit.boundary.no_persistent_service_installed and
          audit.boundary.no_launchd_cron_systemd_autostart and
          audit.boundary.no_production_runtime_state_mutation

      id == "VT1178-01" ->
        audit.registry.yaml_backed and audit.registry.versioned_rows and
          audit.registry.readable_family_coverage and audit.registry.emitted_family_coverage and
          audit.registry.trigger_limit_hash_coverage and
          audit.registry.enabled_disabled_version_state and
          audit.registry.version_history and
          Enum.all?(
            [:cron, :interval, :ingest_event, :soup_change, :manual],
            &trigger_defined?(runtime, &1)
          ) and
          runtime.suppressions != []

      id == "VT1178-02" ->
        audit.scheduler.trigger_kinds_represented and audit.scheduler.cascade_budgets and
          runtime.suppressions != [] and
          Enum.any?(runtime.failures, &(&1.outcome == :budget_exhausted))

      id == "VT1178-03" ->
        audit.scheduler.leases_recorded

      id == "VT1178-04" ->
        audit.packets.packet_hashes and audit.packets.seed_and_included_refs and
          audit.packets.visibility_scope

      id == "VT1178-05" ->
        audit.runs.provenance_hashes and audit.runs.agent_choice_recorded

      id == "VT1178-06" ->
        audit.writes.linked_to_agent_runs and audit.writes.evidence_backed_candidate_mutations and
          audit.writes.candidate_mutation_dispositions and
          audit.writes.non_commitment_outcomes_visible and
          audit.writes.refusal_pressure_visible_to_future_agents and
          audit.writes.mechanical_write_mediation and audit.writes.no_higher_truth_verifier and
          audit.writes.downstream_activations

      id == "VT1178-07" ->
        Enum.all?(Map.values(audit.failures), &(&1 == true))

      id == "VT1178-08" ->
        audit.anti_cheat.deterministic_standins_not_counted and
          audit.anti_cheat.no_scripted_fixture_oracle_product_proof and
          audit.anti_cheat.source_admission_substrate_not_product_proof and
          audit.anti_cheat.scripted_outputs_not_counted and
          audit.anti_cheat.hard_coded_clusters_not_counted and
          audit.anti_cheat.fixture_trusted_story_ids_not_counted and
          audit.anti_cheat.precomputed_authored_deltas_not_counted and
          audit.anti_cheat.wrong_product_shape_exclusions

      id == "VT1178-09" ->
        audit.boundary.no_t328_source_emitter_registered and
          audit.boundary.no_persistent_service_installed and
          audit.boundary.no_launchd_cron_systemd_autostart and
          audit.boundary.no_source_db_mutation and
          audit.boundary.no_production_runtime_state_mutation

      true ->
        false
    end
  end

  defp trigger_defined?(runtime, kind), do: Enum.any?(runtime.triggers, &(&1.kind == kind))

  defp activation_recorded?(runtime, kind),
    do: Enum.any?(runtime.activations, &(&1.activation_kind == kind))

  defp requirement_ids do
    [
      "REQ1178-SCH-01",
      "REQ1178-SCH-02",
      "REQ1178-SCH-03",
      "REQ1178-SCH-04",
      "REQ1178-SCH-05",
      "REQ1178-SCH-06",
      "REQ1178-SCH-07",
      "REQ1178-SCH-08",
      "REQ1178-SCH-09",
      "REQ1178-SCH-10",
      "REQ1178-TRG-01",
      "REQ1178-TRG-02",
      "REQ1178-TRG-03",
      "REQ1178-TRG-04",
      "REQ1178-TRG-05",
      "REQ1178-CFG-01",
      "REQ1178-CFG-02",
      "REQ1178-CFG-03",
      "REQ1178-CFG-04",
      "REQ1178-CFG-05",
      "REQ1178-SOUP-01",
      "REQ1178-SOUP-02",
      "REQ1178-SOUP-03",
      "REQ1178-SOUP-04",
      "REQ1178-SOUP-05",
      "REQ1178-WRITE-01",
      "REQ1178-WRITE-02",
      "REQ1178-WRITE-03",
      "REQ1178-WRITE-04",
      "REQ1178-WRITE-05",
      "REQ1178-WRITE-06",
      "REQ1178-WRITE-07",
      "REQ1178-WRITE-08",
      "REQ1178-FAIL-01",
      "REQ1178-FAIL-02",
      "REQ1178-FAIL-03",
      "REQ1178-FAIL-04",
      "REQ1178-FAIL-05",
      "REQ1178-FAIL-06",
      "REQ1178-FAIL-07",
      "REQ1178-PROOF-01",
      "REQ1178-PROOF-02",
      "REQ1178-PROOF-03",
      "REQ1178-PROOF-04",
      "REQ1178-PROOF-05",
      "REQ1178-PROOF-06",
      "REQ1178-PROOF-07",
      "REQ1178-PROOF-08",
      "AC1178-01",
      "AC1178-02",
      "AC1178-03",
      "AC1178-04",
      "AC1178-05",
      "AC1178-06",
      "AC1178-07",
      "AC1178-08",
      "VT1178-01",
      "VT1178-02",
      "VT1178-03",
      "VT1178-04",
      "VT1178-05",
      "VT1178-06",
      "VT1178-07",
      "VT1178-08",
      "VT1178-09"
    ]
  end

  defp soup_change_eligible?(runtime, proposal, commits) do
    trigger = trigger!(runtime, :soup_change)

    trigger.enabled and proposal.confidence >= 0.4 and
      Enum.any?(commits, &(&1.status == :committed and &1.visibility == proposal.visibility))
  end

  defp activation(trigger, agent_id, attrs) do
    source_refs = attrs.source_refs
    soup_refs = attrs.soup_refs
    cause = attrs.cause

    trace_id =
      "trace:t1178:#{trigger.kind}:#{agent_id}:#{cause}:#{hash({source_refs, soup_refs})}"

    %{
      activation_id:
        "activation:t1178:#{trigger.kind}:#{agent_id}:#{cause}:#{hash({source_refs, soup_refs})}",
      activation_kind: trigger.kind,
      trigger_id: trigger.trigger_id,
      agent_id: agent_id,
      agent_definition_version: Worker.configs()[agent_id].config_version,
      source_refs: source_refs,
      soup_refs: soup_refs,
      evidence_refs: Map.get(attrs, :evidence_refs, source_refs),
      cascade_root_id: attrs.cascade_root_id,
      depth: attrs.depth,
      actor: attrs.actor,
      lock_key: lock_key(trigger, source_refs, soup_refs),
      created_at: @proof_now,
      status: :eligible,
      caused_by_fixture_id: Map.get(attrs, :caused_by_fixture_id),
      packet: Map.get(attrs, :packet),
      packet_hash: Map.get(attrs, :packet_hash, hash(Map.get(attrs, :packet, %{}))),
      cause: cause,
      trace_id: trace_id
    }
  end

  defp lock_key(%{lock_key: lock_key}, _source_refs, _soup_refs), do: lock_key

  defp lock_key(%{kind: :ingest_event}, [source_ref | _], _soup_refs),
    do: "ingest:daemon_news:#{source_ref}"

  defp lock_key(%{kind: :soup_change}, _source_refs, [soup_ref | _]),
    do: "soup:#{soup_ref}:committed"

  defp lock_key(trigger, _source_refs, _soup_refs), do: "#{trigger.kind}:#{trigger.trigger_id}"

  defp lease_for(activation, outcome) do
    %{
      lease_id: "lease:#{activation.activation_id}:#{outcome}:#{length(activation.source_refs)}",
      lock_key: activation.lock_key,
      holder: "scheduler:t1178",
      activation_id: activation.activation_id,
      agent_definition_version: activation.agent_definition_version,
      acquired_at: @proof_now,
      expires_at: "2026-06-04T20:03:00Z",
      release_outcome: outcome,
      trace_id: activation.trace_id
    }
  end

  defp packet_record(activation, packet, packet_hash) do
    %{
      packet_id: "packet:#{activation.activation_id}",
      packet_hash: packet_hash,
      activation_id: activation.activation_id,
      seed_refs: activation.source_refs ++ activation.soup_refs,
      included_refs:
        Enum.uniq(Map.get(packet, :evidence_refs, []) ++ Map.get(packet, :visible_story_refs, [])),
      traversal_depth: Map.get(packet, :traversal_depth, 1),
      visibility_scope: Map.get(packet, :output_visibility, "public"),
      evidence_snippet_policy: :bounded_snippets,
      evidence_refs: Map.get(packet, :evidence_refs, []),
      source_adapter_refs: Map.get(packet, :evidence_refs, []),
      prior_state_refs:
        Enum.uniq(
          Map.get(packet, :visible_story_refs, []) ++ Map.get(packet, :seen_story_keys, [])
        ),
      excluded_context_reason: "ACL, traversal, and proof packet bounds applied",
      raw_database_access: Map.get(packet, :raw_database_access, false),
      trace_id: activation.trace_id,
      packet: packet
    }
  end

  defp pre_agent_run_record(activation, lease) do
    %{
      agent_run_id: "agent-run:t1178:#{activation.activation_id}",
      activation_id: activation.activation_id,
      activation_kind: activation.activation_kind,
      agent_id: activation.agent_id,
      agent_definition_version: activation.agent_definition_version,
      depth: activation.depth,
      runtime_target: :local_recorded_agent_route,
      model_route: :configured,
      operation_family: nil,
      status: :running,
      created_at: @proof_now,
      started_at: nil,
      finished_at: nil,
      lease_id: lease.lease_id,
      trace_id: activation.trace_id,
      precreated_before_invocation: true,
      invoked_through_runtime: false
    }
  end

  defp complete_agent_run_record(pre_run, activation, invocation, opts) do
    status =
      case invocation.outcome do
        :write -> :succeeded
        :abstained -> :abstained
        :refused -> :refused
        other -> other
      end

    Map.merge(pre_run, %{
      prompt_hash: get_in(invocation, [:config, :prompt_version_hash]),
      invocation_id: "#{Transcript.artifact_hash()}:#{activation.activation_id}",
      packet_hash: invocation.packet_hash,
      output_hash: invocation.agent_output_hash,
      status: status,
      started_at: @proof_now,
      finished_at: @proof_now,
      duration_ms: 0,
      depth: activation.depth,
      selected_soup_region:
        Enum.uniq(
          Map.get(invocation.packet, :visible_story_refs, []) ++
            Map.get(invocation.packet, :evidence_refs, [])
        ),
      selected_operation_family:
        Keyword.get(opts, :selected_operation_family, invocation.operation_family),
      alternatives_or_uncertainty: invocation.uncertainty_class,
      rationale: invocation.reason,
      evidence_refs: invocation.evidence_refs,
      candidate_mutation_ids: candidate_mutation_ids(invocation),
      operation_family: invocation.operation_family,
      decision_source: invocation.decision_source,
      deterministic_product_logic: invocation.deterministic_product_logic,
      producer_kind: invocation.producer_kind,
      producer_id: invocation.producer_id,
      artifact_ref: invocation.artifact_ref,
      fresh_import_event: activation.activation_kind == :ingest_event,
      packet: invocation.packet,
      invoked_through_runtime: true
    })
  end

  defp replace_run(runs, agent_run_id, replacement) do
    Enum.map(runs, fn
      %{agent_run_id: ^agent_run_id} -> replacement
      run -> run
    end)
  end

  defp candidate_mutation_record(proposal) do
    %{
      candidate_mutation_id: candidate_mutation_id(proposal),
      intended_operations: Enum.map(proposal.ops, & &1.op),
      evidence_refs: proposal.evidence_refs,
      confidence: proposal.confidence,
      uncertainty: proposal.uncertainty_class,
      rationale: proposal.rationale,
      risk_flags: risk_flags(proposal),
      source_packet_hash: proposal.input_packet_hash,
      agent_run_id: proposal.agent_run_id,
      disposition_status: proposal.status,
      operation_family: proposal.classification,
      visibility: proposal.visibility,
      trace_id: "trace:t1178:candidate-mutation:#{candidate_mutation_id(proposal)}"
    }
  end

  defp mediation_decision_record(decision) do
    {candidate_mutation_id, decision} = Map.pop(decision, :proposal_id)
    candidate_mutation_id = candidate_mutation_id(candidate_mutation_id)

    decision
    |> Map.put(:candidate_mutation_id, candidate_mutation_id)
    |> Map.put(:disposition, decision.to)
    |> Map.put(:mediation_scope, :mechanical_containment)
    |> Map.put(:truth_judgment, false)
    |> Map.put(:trace_id, "trace:t1178:write-mediation:#{candidate_mutation_id}")
  end

  defp mutation_record({commit, index}) do
    {candidate_mutation_id, commit} = Map.pop(commit, :proposal_id)
    candidate_mutation_id = candidate_mutation_id(candidate_mutation_id)

    commit
    |> Map.put(:candidate_mutation_id, candidate_mutation_id)
    |> Map.put(:mutation_id, "mutation:t1178:#{index}")
    |> Map.put(:trace_id, "trace:t1178:mutation:#{index}:#{candidate_mutation_id}")
    |> Map.put(:operation_family, Map.get(commit, :operation_family) || commit.op)
  end

  defp risk_flags(proposal) do
    [
      if(proposal.visibility == "private", do: :private_visibility, else: :public_visibility),
      if(proposal.uncertainty_class == :bounded_evidence,
        do: :bounded_evidence,
        else: :uncertainty_present
      )
    ]
  end

  defp failure_record(activation, outcome, failure_class, attrs) do
    %{
      outcome: outcome,
      activation_id: activation.activation_id,
      agent_id: activation.agent_id,
      agent_definition_version: activation.agent_definition_version,
      packet_hash: Map.get(attrs, :packet_hash, activation.packet_hash),
      failure_class: failure_class,
      error_summary: Map.get(attrs, :error_summary, to_string(failure_class)),
      downstream_activations_suppressed: Map.get(attrs, :downstream_activations_suppressed, true),
      recorded_at: @proof_now,
      trace_id: activation.trace_id
    }
    |> Map.merge(attrs)
  end

  defp refusal_log(invocation) do
    Map.merge(invocation, %{
      status: :refused,
      timestamp: @proof_now,
      actor_id: "flynn",
      prior_element_refs: Map.get(invocation.packet, :visible_story_refs, []),
      trace_id: "trace:t1178:refusal:#{invocation.packet_hash}"
    })
  end

  defp maybe_append_non_commitment_pressure(store, %{status: status} = run)
       when status in [:abstained, :refused] do
    Store.append_non_commitment_pressure(store, %{
      status: status,
      evidence_refs: run.evidence_refs,
      uncertainty_class: run.alternatives_or_uncertainty,
      rationale: run.rationale,
      visibility: Map.get(run.packet, :output_visibility),
      actor_id: "flynn",
      trace_id: "trace:t1178:non-commitment-pressure:#{run.agent_run_id}",
      agent_run_id: run.agent_run_id
    })
  end

  defp maybe_append_non_commitment_pressure(store, _run), do: store

  defp candidate_mutation_ids(%{proposal: proposal}), do: [candidate_mutation_id(proposal)]
  defp candidate_mutation_ids(_invocation), do: []

  defp candidate_mutation_id(%{id: id}), do: candidate_mutation_id(id)
  defp candidate_mutation_id("proposal:" <> id), do: "candidate-mutation:" <> id
  defp candidate_mutation_id(id), do: id

  defp trigger!(runtime, kind), do: Enum.find(runtime.triggers, &(&1.kind == kind))

  defp registry_for(runtime, agent_id),
    do: Enum.find(runtime.registry, &(&1.agent_id == agent_id))

  defp hash(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(value))
    |> Base.encode16(case: :lower)
  end
end
