defmodule Primeradiant.BackfillTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Resolution.Backfill
  alias Primeradiant.Ingestion.SourceRegistry

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    ResolutionAttempt,
    ResolutionEvidence,
    ResolvedSourceField,
    ResolutionOutcome
  }

  @tenant "80000000-0000-0000-0000-000000001656"
  @source "backfill-fixture"
  @at ~U[2026-07-10 18:00:00.000000Z]

  defmodule Adapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter

    def to_candidate(raw_envelope, _ctx) do
      {:ok,
       %{
         raw_refs: [raw_envelope.id],
         declared_identity: raw_envelope.source_event_external_id,
         declared_cursor: raw_envelope.integrity_metadata["source_position"],
         raw_fields: Jason.decode!(raw_envelope.retained_bytes),
         visibility: raw_envelope.visibility,
         adapter_provenance: %{"adapter" => "backfill-fixture", "version" => "adapter-v1"}
       }}
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "backfill-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    db = Path.join(root, "soup.sqlite3")
    register(db)
    %{db_path: db}
  end

  test "SER-V8 approved apply is append-only, admits eligible material, and proves completion", %{
    db_path: db
  } do
    historical = historical_case(db, 1, "historical")
    pending = receive(db, 2, "pending")

    retry =
      receive(db, 3, "retry")
      |> set_case(
        db,
        "retry_scheduled",
        "retry_scheduled:fixture",
        DateTime.add(@at, 60, :second)
      )

    resolution_counts = resolution_counts(db)
    historical_snapshot = historical_snapshot(db, historical)
    cursor_before = DurableSoupDb.source_registration(db, @tenant, @source).cursor
    gaps_before = DurableSoupDb.source_gap_records(db, @tenant, @source)

    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant, source_key: @source})

    assert Enum.map(plan.candidates, & &1["resolution_case_id"]) == [
             historical.resolution_case_id
           ]

    assert resolution_counts(db) == resolution_counts
    assert DurableSoupDb.table_count(db, "resolution_backfill_plans", @tenant) == 1

    assert Backfill.apply(db, plan.id, run_id: "run-1", at: @at) ==
             {:error, :backfill_plan_not_approved}

    assert {:ok, _approval} = approve(db, plan)
    assert {:ok, report} = Backfill.apply(db, plan.id, run_id: "run-1", at: @at)
    assert report.counts["planned"] == 1
    assert report.counts["attempted"] == 1
    assert report.counts["eligible"] == 1
    assert report.counts["admitted"] == 1
    assert report.duplicate_count == 0
    assert report.residual_proof == %{"zero_pending" => true, "explicit_residuals" => []}

    assert [%{"historical_case_id" => historical_case_id, "admission_ref" => admission_ref}] =
             report.dispositions

    assert historical_case_id == historical.resolution_case_id
    assert is_binary(admission_ref)

    assert historical_snapshot(db, historical) == historical_snapshot

    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) ==
             resolution_counts.resolution_cases + 1

    assert DurableSoupDb.table_count(db, "inputs", @tenant) == 1

    assert {:ok, replay} = Backfill.apply(db, plan.id, run_id: "run-1", at: @at)
    assert replay == report

    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) ==
             resolution_counts.resolution_cases + 1

    assert {:ok, completion} = Backfill.completion_report(db, @tenant, "run-1")
    assert completion == report

    assert DurableSoupDb.resolution_case(db, @tenant, pending.resolution_case_id).state ==
             "received"

    assert DurableSoupDb.resolution_case(db, @tenant, retry.resolution_case_id).state ==
             "retry_scheduled"

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor == cursor_before
    assert DurableSoupDb.source_gap_records(db, @tenant, @source) == gaps_before
  end

  test "explicit runtime cases are refused and broad discovery excludes them", %{db_path: db} do
    pending = receive(db, 1, "pending")

    retry =
      receive(db, 2, "retry")
      |> set_case(
        db,
        "retry_scheduled",
        "retry_scheduled:fixture",
        DateTime.add(@at, 60, :second)
      )

    assert Backfill.build_plan(db, %{
             tenant_id: @tenant,
             case_ids: [pending.resolution_case_id]
           }) == {:error, :non_historical_backfill_candidate}

    assert Backfill.build_plan(db, %{
             tenant_id: @tenant,
             case_ids: [retry.resolution_case_id]
           }) == {:error, :non_historical_backfill_candidate}

    historical = historical_case(db, 3, "historical")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})

    assert Enum.map(plan.candidates, & &1["resolution_case_id"]) == [
             historical.resolution_case_id
           ]
  end

  test "approval binds the exact immutable plan content", %{db_path: db} do
    historical_case(db, 1, "historical")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})
    assert {:ok, _} = approve(db, plan)

    sql =
      "UPDATE resolution_backfill_plans SET selection = '{\"tenant_id\":\"tampered\"}' WHERE id = '#{plan.id}';"

    assert {"", 0} = System.cmd("sqlite3", [db, sql])

    assert Backfill.apply(db, plan.id, run_id: "tampered-run") ==
             {:error, :backfill_plan_hash_mismatch}

    assert DurableSoupDb.table_count(db, "resolution_backfill_runs", @tenant) == 0
  end

  test "approval rejects empty actors and apply rejects resolver/config-only staleness", %{
    db_path: db
  } do
    historical_case(db, 1, "historical")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})

    assert Backfill.approve(db, plan.id, plan.content_hash, nil) ==
             {:error, :invalid_approval_actor}

    assert Backfill.approve(db, plan.id, plan.content_hash, %{kind: "", id: "operator"}) ==
             {:error, :invalid_approval_actor}

    assert {:ok, approval} = approve(db, plan)
    assert approval.actor_kind == "operator"
    assert approval.actor_id == "operator-1"

    registration = DurableSoupDb.source_registration(db, @tenant, @source)
    changed_config = Map.put(registration.config, "resolvers", [%{"version" => "resolver-v2"}])

    registration
    |> ChangesetStore.update!(%{config: changed_config})
    |> then(&DurableSoupDb.put_source_registration!(db, &1))

    assert Backfill.apply(db, plan.id, run_id: "stale-config") == {:error, :approved_plan_stale}
    assert DurableSoupDb.table_count(db, "resolution_backfill_applications", @tenant) == 0
    assert DurableSoupDb.table_count(db, "resolution_backfill_runs", @tenant) == 0
  end

  test "per-candidate claim records retry and eligible transitions as typed stale residuals", %{
    db_path: db
  } do
    for {run_id, state, code} <- [
          {"stale-retry", "retry_scheduled", "retry_scheduled:changed"},
          {"stale-eligible", "eligible", "eligible:changed"}
        ] do
      historical = historical_case(db, if(state == "retry_scheduled", do: 1, else: 2), run_id)

      assert {:ok, plan} =
               Backfill.build_plan(db, %{
                 tenant_id: @tenant,
                 case_ids: [historical.resolution_case_id]
               })

      assert {:ok, _} = approve(db, plan)

      hook = fn _candidate ->
        current = DurableSoupDb.resolution_case(db, @tenant, historical.resolution_case_id)

        current
        |> ChangesetStore.update!(%{
          state: state,
          outcome_code: code,
          next_retry_at:
            if(state == "retry_scheduled", do: DateTime.add(@at, 60, :second), else: nil)
        })
        |> then(&DurableSoupDb.put_resolution_case!(db, &1))

        if state == "eligible" do
          ChangesetStore.insert!(ResolutionOutcome, %{
            tenant_id: @tenant,
            resolution_case_id: historical.resolution_case_id,
            outcome_code: code,
            reason: "changed",
            retryable: false,
            validator_version: "fixture",
            admission_material_ref: "input:already-admitted"
          })
          |> then(&DurableSoupDb.insert_resolution_outcome!(db, &1))
        end
      end

      assert {:ok, report} =
               Backfill.apply(db, plan.id, run_id: run_id, before_candidate_claim: hook)

      assert report.status == "completed_with_residuals"
      assert report.residual_proof["zero_pending"] == false

      assert [
               %{
                 "outcome_family" => "stale_candidate",
                 "failure" => %{"code" => "stale_candidate"}
               }
             ] = report.dispositions

      assert DurableSoupDb.resolution_backfill_applications_for_run(db, @tenant, run_id) == []
    end
  end

  test "running applications resume after claim and after eligible case execution", %{db_path: db} do
    historical_case(db, 1, "claim-crash")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})
    assert {:ok, _} = approve(db, plan)

    assert_raise RuntimeError, "claim crash", fn ->
      Backfill.apply(db, plan.id,
        run_id: "claim-crash",
        at: @at,
        after_application_claim: fn _ -> raise "claim crash" end
      )
    end

    assert DurableSoupDb.resolution_backfill_run(db, @tenant, "claim-crash").status == "running"

    assert length(
             DurableSoupDb.resolution_backfill_applications_for_run(db, @tenant, "claim-crash")
           ) == 1

    case_count = DurableSoupDb.table_count(db, "resolution_cases", @tenant)
    assert {:ok, resumed} = Backfill.apply(db, plan.id, run_id: "claim-crash", at: @at)
    assert resumed.status == "completed"
    assert resumed.duplicate_count == 1
    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) == case_count

    historical2 = historical_case(db, 2, "admission-crash")

    assert {:ok, plan2} =
             Backfill.build_plan(db, %{
               tenant_id: @tenant,
               case_ids: [historical2.resolution_case_id]
             })

    assert {:ok, _} = approve(db, plan2)

    assert_raise RuntimeError, "admission crash", fn ->
      Backfill.apply(db, plan2.id,
        run_id: "admission-crash",
        at: @at,
        after_case_run: fn resolution_case ->
          if resolution_case.state == "eligible", do: raise("admission crash")
        end
      )
    end

    assert DurableSoupDb.table_count(db, "inputs", @tenant) == 1
    case_count2 = DurableSoupDb.table_count(db, "resolution_cases", @tenant)
    assert {:ok, resumed2} = Backfill.apply(db, plan2.id, run_id: "admission-crash", at: @at)
    assert resumed2.counts["admitted"] == 1
    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) == case_count2
    assert DurableSoupDb.table_count(db, "inputs", @tenant) == 2
  end

  test "claimed work resumes before historical staleness while unclaimed work becomes stale", %{
    db_path: db
  } do
    first = historical_case(db, 1, "claimed-historical")
    second = historical_case(db, 2, "unclaimed-historical")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})
    assert {:ok, _} = approve(db, plan)

    assert_raise RuntimeError, "post-claim historical change", fn ->
      Backfill.apply(db, plan.id,
        run_id: "post-claim-historical",
        at: @at,
        after_application_claim: fn _application ->
          move_case(db, first.resolution_case_id, "eligible", "eligible:post_claim", nil)

          move_case(
            db,
            second.resolution_case_id,
            "retry_scheduled",
            "retry_scheduled:post_claim",
            DateTime.add(@at, 60, :second)
          )

          raise "post-claim historical change"
        end
      )
    end

    assert length(
             DurableSoupDb.resolution_backfill_applications_for_run(
               db,
               @tenant,
               "post-claim-historical"
             )
           ) == 1

    assert {:ok, report} = Backfill.apply(db, plan.id, run_id: "post-claim-historical", at: @at)
    assert Enum.at(report.dispositions, 0)["resolution_case_id"]
    assert Enum.at(report.dispositions, 1)["outcome_family"] == "stale_candidate"

    assert length(
             DurableSoupDb.resolution_backfill_applications_for_run(
               db,
               @tenant,
               "post-claim-historical"
             )
           ) == 1
  end

  test "claimed work resumes before config staleness while unclaimed work becomes stale", %{
    db_path: db
  } do
    historical_case(db, 1, "claimed-config")
    historical_case(db, 2, "unclaimed-config")
    assert {:ok, plan} = Backfill.build_plan(db, %{tenant_id: @tenant})
    assert {:ok, _} = approve(db, plan)

    assert_raise RuntimeError, "post-claim config change", fn ->
      Backfill.apply(db, plan.id,
        run_id: "post-claim-config",
        at: @at,
        after_application_claim: fn _application ->
          registration = DurableSoupDb.source_registration(db, @tenant, @source)

          registration
          |> ChangesetStore.update!(%{
            config: Map.put(registration.config, "post_claim_marker", "changed")
          })
          |> then(&DurableSoupDb.put_source_registration!(db, &1))

          raise "post-claim config change"
        end
      )
    end

    assert length(
             DurableSoupDb.resolution_backfill_applications_for_run(
               db,
               @tenant,
               "post-claim-config"
             )
           ) == 1

    assert {:ok, report} = Backfill.apply(db, plan.id, run_id: "post-claim-config", at: @at)
    assert Enum.at(report.dispositions, 0)["resolution_case_id"]
    assert Enum.at(report.dispositions, 1)["failure"]["reason"] == "approved_plan_stale"

    assert length(
             DurableSoupDb.resolution_backfill_applications_for_run(
               db,
               @tenant,
               "post-claim-config"
             )
           ) == 1
  end

  test "stored report is stable and a run id cannot be reused for another plan", %{db_path: db} do
    first = historical_case(db, 1, "first")

    assert {:ok, plan1} =
             Backfill.build_plan(db, %{tenant_id: @tenant, case_ids: [first.resolution_case_id]})

    assert {:ok, _} = approve(db, plan1)
    assert {:ok, report} = Backfill.apply(db, plan1.id, run_id: "stable", at: @at)
    assert Backfill.apply(db, plan1.id, run_id: "stable", at: @at) == {:ok, report}
    assert Backfill.completion_report(db, @tenant, "stable") == {:ok, report}

    second = historical_case(db, 2, "second")

    assert {:ok, plan2} =
             Backfill.build_plan(db, %{tenant_id: @tenant, case_ids: [second.resolution_case_id]})

    assert {:ok, _} = approve(db, plan2)
    assert Backfill.apply(db, plan2.id, run_id: "stable", at: @at) == {:error, :run_plan_mismatch}
  end

  test "admission failure is an explicit residual and non-eligible terminal results are counted",
       %{db_path: db} do
    registration = DurableSoupDb.source_registration(db, @tenant, @source)

    config = Map.delete(registration.config, "admission_material")

    policy_hash =
      SourceRegistry.policy_hash(%{
        "resolution_policy" => registration.resolution_policy,
        "config" => config
      })

    registration
    |> ChangesetStore.update!(%{config: config, policy_hash: policy_hash})
    |> then(&DurableSoupDb.put_source_registration!(db, &1))

    eligible_history = historical_case(db, 1, "no-admission")

    assert {:ok, plan} =
             Backfill.build_plan(db, %{
               tenant_id: @tenant,
               case_ids: [eligible_history.resolution_case_id]
             })

    assert {:ok, _} = approve(db, plan)
    assert {:ok, report} = Backfill.apply(db, plan.id, run_id: "admission-failure", at: @at)
    assert report.status == "completed_with_residuals"
    assert report.residual_proof["zero_pending"] == false

    assert [%{"failure" => %{"code" => "admission_failed"}, "admission_ref" => nil}] =
             report.dispositions

    register(db)
    unresolved_history = historical_case_with_fields(db, 2, "terminal-unresolved", %{})

    assert {:ok, plan2} =
             Backfill.build_plan(db, %{
               tenant_id: @tenant,
               case_ids: [unresolved_history.resolution_case_id]
             })

    assert {:ok, _} = approve(db, plan2)
    assert {:ok, terminal} = Backfill.apply(db, plan2.id, run_id: "terminal-unresolved", at: @at)
    assert terminal.counts["unresolved"] == 1
    assert terminal.counts["admitted"] == 0
    assert terminal.residual_proof["zero_pending"]
  end

  defp historical_case(db, position, identity) do
    historical_case_with_fields(db, position, identity, %{
      "publisher_label" => "Backfill Publisher",
      "publisher_domain" => "publisher.example",
      "public_url" => "https://publisher.example/article/#{identity}"
    })
  end

  defp historical_case_with_fields(db, position, identity, fields) do
    receipt = receive_fields(db, position, identity, fields)
    resolution_case = DurableSoupDb.resolution_case(db, @tenant, receipt.resolution_case_id)

    resolution_case
    |> ChangesetStore.update!(%{state: "unresolved", outcome_code: "unresolved:historical"})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))

    ChangesetStore.insert!(ResolutionOutcome, %{
      tenant_id: @tenant,
      resolution_case_id: resolution_case.id,
      outcome_code: "unresolved:historical",
      reason: "historical",
      retryable: false,
      validator_version: "historical-fixture"
    })
    |> then(&DurableSoupDb.insert_resolution_outcome!(db, &1))

    evidence =
      ChangesetStore.insert!(ResolutionEvidence, %{
        tenant_id: @tenant,
        resolution_case_id: resolution_case.id,
        kind: "historical_fixture",
        value: "bounded historical evidence",
        source: "fixture",
        locator: %{"raw_envelope_id" => receipt.raw_envelope_id},
        digest: ChangesetStore.hash("bounded historical evidence"),
        retrieved_at: @at,
        visibility: "public",
        provenance: %{"fixture" => true},
        transformation_chain: []
      })
      |> then(&DurableSoupDb.insert_resolution_evidence!(db, &1))

    ChangesetStore.insert!(ResolvedSourceField, %{
      tenant_id: @tenant,
      resolution_case_id: resolution_case.id,
      field_name: "publisher_label",
      normalized_value: "Historical Publisher",
      confidence: Decimal.new("0.8"),
      evidence_refs: [evidence.id],
      derivation_evidence_ref: evidence.id,
      resolver_provenance: [%{"resolver" => "historical"}],
      transform: "fixture",
      contradiction_status: "none",
      selected: false
    })
    |> then(&DurableSoupDb.put_resolved_source_field!(db, &1))

    ChangesetStore.insert!(ResolutionAttempt, %{
      tenant_id: @tenant,
      resolution_case_id: resolution_case.id,
      raw_envelope_id: receipt.raw_envelope_id,
      raw_envelope_digest:
        DurableSoupDb.raw_envelope(db, @tenant, receipt.raw_envelope_id).content_digest,
      attempt_key: "historical:#{resolution_case.id}",
      stage: "resolver",
      resolver: "historical",
      input_hash: ChangesetStore.hash(identity),
      attempt_ordinal: 1,
      budgets_consumed: %{"time_ms" => 1},
      outcome: "unresolved",
      response_evidence_refs: [evidence.id],
      started_at: @at,
      ended_at: @at
    })
    |> then(&DurableSoupDb.insert_resolution_attempt!(db, &1))

    receipt
  end

  defp approve(db, plan),
    do: Backfill.approve(db, plan.id, plan.content_hash, %{kind: "operator", id: "operator-1"})

  defp receive(db, position, identity) do
    fields = %{
      "publisher_label" => "Backfill Publisher",
      "publisher_domain" => "publisher.example",
      "public_url" => "https://publisher.example/article/#{identity}"
    }

    receive_fields(db, position, identity, fields)
  end

  defp receive_fields(db, position, identity, fields) do
    {:ok, receipt} =
      SourceRegistry.receive_envelope(db, %{
        tenant_id: @tenant,
        source_key: @source,
        source_event_external_id: identity,
        source_position: position,
        received_at: DateTime.add(@at, position, :second),
        content_digest: ChangesetStore.hash(Jason.encode!(fields)),
        retained_bytes: Jason.encode!(fields),
        visibility: "public",
        correlation_id: "correlation-#{identity}"
      })

    receipt
  end

  defp set_case(receipt, db, state, code, retry_at) do
    db
    |> DurableSoupDb.resolution_case(@tenant, receipt.resolution_case_id)
    |> ChangesetStore.update!(%{state: state, outcome_code: code, next_retry_at: retry_at})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))

    receipt
  end

  defp move_case(db, case_id, state, code, retry_at) do
    db
    |> DurableSoupDb.resolution_case(@tenant, case_id)
    |> ChangesetStore.update!(%{state: state, outcome_code: code, next_retry_at: retry_at})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))
  end

  defp historical_snapshot(db, receipt) do
    %{
      raw: DurableSoupDb.raw_envelope(db, @tenant, receipt.raw_envelope_id),
      case: DurableSoupDb.resolution_case(db, @tenant, receipt.resolution_case_id),
      evidence:
        DurableSoupDb.resolution_evidence_for_case(db, @tenant, receipt.resolution_case_id),
      fields:
        DurableSoupDb.resolved_source_fields_for_case(db, @tenant, receipt.resolution_case_id),
      attempts:
        DurableSoupDb.resolution_attempts_for_case(db, @tenant, receipt.resolution_case_id),
      outcomes:
        DurableSoupDb.resolution_outcomes_for_case(db, @tenant, receipt.resolution_case_id)
    }
  end

  defp resolution_counts(db) do
    %{
      raw_envelopes: DurableSoupDb.table_count(db, "raw_envelopes", @tenant),
      resolution_cases: DurableSoupDb.table_count(db, "resolution_cases", @tenant),
      resolution_evidence: DurableSoupDb.table_count(db, "resolution_evidence", @tenant),
      resolved_source_fields: DurableSoupDb.table_count(db, "resolved_source_fields", @tenant),
      resolution_attempts: DurableSoupDb.table_count(db, "resolution_attempts", @tenant),
      resolution_outcomes: DurableSoupDb.table_count(db, "resolution_outcomes", @tenant)
    }
  end

  defp register(db) do
    {:ok, _} =
      SourceRegistry.register_source(db, %{
        tenant_id: @tenant,
        source_key: @source,
        adapter_module: Adapter,
        adapter_version: "adapter-v1",
        mode: "enabled",
        resolution_policy: %{"version" => "v1", "source_class" => "public_article"},
        policy_version: "v1",
        budgets: %{
          "max_attempts" => 2,
          "total_case_ms" => 10_000,
          "retry_backoff_ms" => 1,
          "per_source_concurrency" => 1,
          "adapter" => %{"time_ms" => 100},
          "normalizer" => %{"time_ms" => 100},
          "resolvers" => %{}
        },
        config: %{
          "resolvers" => [],
          "admission_material" => %{
            "source_type" => "news_article",
            "acl" => %{"privacy" => "public", "participants" => []}
          }
        },
        initial_cursor: 0
      })
  end
end
