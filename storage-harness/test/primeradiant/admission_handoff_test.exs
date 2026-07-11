defmodule Primeradiant.AdmissionHandoffTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Resolution.{AdmissionHandoff, Case}
  alias Primeradiant.Ingestion.SourceRegistry

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    RealIngestion,
    ResolutionOutcome,
    ResolvedSourceField
  }

  @tenant "60000000-0000-0000-0000-000000001656"
  @source "handoff-fixture"
  @received ~U[2026-07-10 18:00:00.000000Z]
  @resolved ~U[2026-07-10 18:00:10.000000Z]
  @admitted ~U[2026-07-10 18:00:20.000000Z]

  defmodule FixtureAdapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter

    def to_candidate(raw_envelope, _ctx) do
      {:ok,
       %{
         raw_refs: [raw_envelope.id],
         declared_identity: raw_envelope.source_event_external_id,
         declared_cursor: raw_envelope.integrity_metadata["source_position"],
         raw_fields: Jason.decode!(raw_envelope.retained_bytes),
         visibility: raw_envelope.visibility,
         adapter_provenance: %{"adapter" => "fixture", "version" => "adapter-v1"}
       }}
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "admission-handoff-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    db = Path.join(root, "soup.sqlite3")
    registration = register(db)
    receipt = receive(db, 1, "article-1")
    %{db_path: db, receipt: receipt, policy_hash: registration.policy_hash}
  end

  test "SER-V9 normal pipeline admits truthful material and replay keeps durable identity", ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    assert {:ok, admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id,
               tenant_id: @tenant,
               at: @admitted
             )

    first = DurableSoupDb.load_tenant(ctx.db_path, @tenant).inputs |> List.first()
    assert first.title == nil
    assert first.body_text == nil
    assert first.object_uri == "raw-envelope:#{ctx.receipt.raw_envelope_id}"
    assert first.normalized["source_name"] == "Fixture Publisher"
    assert first.normalized["canonical_uri"] == "https://publisher.example/article"
    assert first.normalized["metadata"]["resolution_case_id"] == eligible.id
    assert first.normalized["metadata"]["raw_envelope_id"] == ctx.receipt.raw_envelope_id
    assert first.normalized["metadata"]["policy_hash"] == ctx.policy_hash
    assert first.normalized["metadata"]["evidence_ref_ids"] != []
    assert first.normalized["metadata"]["provenance"] != []

    assert {:ok, ^admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)

    replay = DurableSoupDb.load_tenant(ctx.db_path, @tenant).inputs |> List.first()
    assert replay.id == first.id
    assert DurableSoupDb.table_count(ctx.db_path, "inputs", @tenant) == 1

    assert {:ok, health} = SourceRegistry.health(ctx.db_path, scope())
    assert health.last_received_at == @received
    assert health.last_resolution_terminal_at != nil
    assert health.last_resolution_terminal_at != health.last_received_at
    assert health.last_resolution_terminal_at != health.last_admission_at
    assert health.last_admission_at != nil

    for {position, state} <-
          Enum.with_index(~w(unresolved quarantined refused failed_terminal), 2)
          |> Enum.map(fn {state, position} -> {position, state} end) do
      receipt = receive(ctx.db_path, position, "#{state}-#{position}")
      put_outcome(ctx.db_path, receipt.resolution_case_id, state)
    end

    assert {:ok, health} = SourceRegistry.health(ctx.db_path, scope())
    assert health.unresolved_count == 1
    assert health.quarantine_count == 1
    assert health.refusal_count == 1

    codes =
      DurableSoupDb.load_tenant(ctx.db_path, @tenant).resolution_outcomes
      |> Enum.map(& &1.outcome_code)

    for family <- ~w(eligible unresolved quarantined refused failed_terminal) do
      assert Enum.any?(codes, &String.starts_with?(&1, family))
    end
  end

  test "all non-eligible and mid-pipeline states are rejected and eligible outcome is required",
       ctx do
    resolution_case =
      DurableSoupDb.resolution_case(ctx.db_path, @tenant, ctx.receipt.resolution_case_id)

    for state <-
          ~w(unresolved retry_scheduled quarantined refused failed_terminal normalizing resolving interpreting validating) do
      resolution_case
      |> ChangesetStore.update!(%{state: state})
      |> then(&DurableSoupDb.put_resolution_case!(ctx.db_path, &1))

      assert AdmissionHandoff.admit(ctx.db_path, resolution_case.id, tenant_id: @tenant) ==
               {:error, {:resolution_case_not_eligible, state}}
    end

    resolution_case
    |> ChangesetStore.update!(%{state: "eligible", outcome_code: "eligible"})
    |> then(&DurableSoupDb.put_resolution_case!(ctx.db_path, &1))

    assert AdmissionHandoff.admit(ctx.db_path, resolution_case.id, tenant_id: @tenant) ==
             {:error, :eligible_outcome_pending}
  end

  test "split-commit replay repairs input identity and admission health", ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    assert {:ok, admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id,
               tenant_id: @tenant,
               at: @admitted
             )

    input_id =
      DurableSoupDb.load_tenant(ctx.db_path, @tenant).inputs |> List.first() |> Map.fetch!(:id)

    outcome = eligible_outcome(ctx.db_path, eligible.id)

    outcome
    |> ChangesetStore.update!(%{admission_material_ref: nil})
    |> then(&DurableSoupDb.insert_resolution_outcome!(ctx.db_path, &1))

    assert {:ok, ^admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)

    assert DurableSoupDb.load_tenant(ctx.db_path, @tenant).inputs
           |> List.first()
           |> Map.fetch!(:id) ==
             input_id

    registration = DurableSoupDb.source_registration(ctx.db_path, @tenant, @source)

    registration
    |> ChangesetStore.update!(%{last_admission_at: nil})
    |> then(&DurableSoupDb.put_source_registration!(ctx.db_path, &1))

    stored = eligible_outcome(ctx.db_path, eligible.id)

    assert {:ok, ^admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)

    repaired = DurableSoupDb.source_registration(ctx.db_path, @tenant, @source)
    assert repaired.last_admission_at == stored.updated_at
  end

  test "between-read competing admission proves revision-first guard and preserves identity",
       ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    test_pid = self()

    assert_raise RuntimeError, fn ->
      AdmissionHandoff.admit(ctx.db_path, eligible.id,
        tenant_id: @tenant,
        before_state_load: fn ->
          prior = DurableSoupDb.load_tenant(ctx.db_path, @tenant)
          revision = DurableSoupDb.tenant_revision(ctx.db_path, @tenant)

          item = competing_item(ctx, eligible.id)

          {:ok, competing_state, _report} =
            RealIngestion.ingest_items(prior, [item], "competitor")

          DurableSoupDb.persist_delta!(ctx.db_path, prior, competing_state, %{
            source_kind: "competing-source-evidence-admission",
            source_db_path: "competing:#{eligible.id}",
            source_row_count: 1,
            expected_tenant_revision: revision
          })

          competing_id = competing_state.inputs |> List.first() |> Map.fetch!(:id)
          send(test_pid, {:competing_input_id, competing_id, revision})
        end,
        before_persist: fn _item, loaded_state ->
          loaded_id = loaded_state.inputs |> List.first() |> Map.fetch!(:id)
          send(test_pid, {:loaded_input_id, loaded_id})
        end
      )
    end

    assert_received {:competing_input_id, competing_id, sampled_revision}
    assert_received {:loaded_input_id, ^competing_id}
    assert DurableSoupDb.tenant_revision(ctx.db_path, @tenant) > sampled_revision
    inputs = DurableSoupDb.load_tenant(ctx.db_path, @tenant).inputs
    assert [%{id: ^competing_id}] = inputs
    assert DurableSoupDb.table_count(ctx.db_path, "inputs", @tenant) == 1
  end

  test "health replay advances stale timestamps and preserves later timestamps", ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    assert {:ok, admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id,
               tenant_id: @tenant,
               at: @admitted
             )

    outcome = eligible_outcome(ctx.db_path, eligible.id)
    stale = DateTime.add(outcome.updated_at, -10, :second)
    put_admission_health(ctx.db_path, stale)

    assert {:ok, ^admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)

    assert DurableSoupDb.source_registration(ctx.db_path, @tenant, @source).last_admission_at ==
             outcome.updated_at

    later = DateTime.add(outcome.updated_at, 10, :second)
    put_admission_health(ctx.db_path, later)

    assert {:ok, ^admission_ref} =
             AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)

    assert DurableSoupDb.source_registration(ctx.db_path, @tenant, @source).last_admission_at ==
             later
  end

  test "forbidden selected field key still reaches Admission label rejection", ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    insert_selected_field(ctx.db_path, eligible.id, "fixture_id", "forbidden")

    assert_raise ArgumentError, "real admission rejects trusted label fixture_id", fn ->
      AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant)
    end
  end

  test "legacy eligible case without declared admission material returns typed error", ctx do
    assert {:outcome, "eligible", eligible} =
             Case.run(ctx.db_path, ctx.receipt.resolution_case_id,
               tenant_id: @tenant,
               at: @resolved
             )

    eligible
    |> ChangesetStore.update!(%{policy_snapshot: Map.delete(eligible.policy_snapshot, "config")})
    |> then(&DurableSoupDb.put_resolution_case!(ctx.db_path, &1))

    assert AdmissionHandoff.admit(ctx.db_path, eligible.id, tenant_id: @tenant) ==
             {:error, :admission_material_not_declared}
  end

  defp receive(db, position, identity) do
    fields = %{
      "publisher_label" => "Fixture Publisher",
      "publisher_domain" => "publisher.example",
      "public_url" => "https://publisher.example/article"
    }

    {:ok, receipt} =
      SourceRegistry.receive_envelope(db, %{
        tenant_id: @tenant,
        source_key: @source,
        source_event_external_id: identity,
        source_position: position,
        received_at: @received,
        content_digest: ChangesetStore.hash(Jason.encode!(fields)),
        retained_bytes: Jason.encode!(fields),
        visibility: "public",
        correlation_id: "correlation-#{identity}"
      })

    receipt
  end

  defp insert_selected_field(db, case_id, name, value) do
    fields = DurableSoupDb.resolved_source_fields_for_case(db, @tenant, case_id)
    evidence_ref = fields |> List.first() |> Map.fetch!(:evidence_refs) |> List.first()

    ChangesetStore.insert!(ResolvedSourceField, %{
      tenant_id: @tenant,
      resolution_case_id: case_id,
      field_name: name,
      normalized_value: value,
      confidence: Decimal.new("1.0"),
      evidence_refs: [evidence_ref],
      derivation_evidence_ref: evidence_ref,
      resolver_provenance: [%{"resolver" => "fixture"}],
      transform: "identity",
      contradiction_status: "none",
      selected: true
    })
    |> then(&DurableSoupDb.put_resolved_source_field!(db, &1))
  end

  defp eligible_outcome(db, case_id) do
    db
    |> DurableSoupDb.resolution_outcomes_for_case(@tenant, case_id)
    |> Enum.find(&(&1.outcome_code == "eligible"))
  end

  defp put_outcome(db, case_id, state) do
    resolution_case = DurableSoupDb.resolution_case(db, @tenant, case_id)

    resolution_case
    |> ChangesetStore.update!(%{state: state, outcome_code: state})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))

    ChangesetStore.insert!(ResolutionOutcome, %{
      tenant_id: @tenant,
      resolution_case_id: case_id,
      outcome_code: state,
      reason: state,
      retryable: false,
      validator_version: "fixture"
    })
    |> then(&DurableSoupDb.insert_resolution_outcome!(db, &1))
  end

  defp put_admission_health(db, at) do
    db
    |> DurableSoupDb.source_registration(@tenant, @source)
    |> ChangesetStore.update!(%{last_admission_at: at})
    |> then(&DurableSoupDb.put_source_registration!(db, &1))
  end

  defp competing_item(ctx, case_id) do
    selected =
      ctx.db_path
      |> DurableSoupDb.resolved_source_fields_for_case(@tenant, case_id)
      |> Enum.filter(& &1.selected)
      |> Map.new(&{&1.field_name, &1.normalized_value})

    %{
      tenant_id: @tenant,
      source_mode: "source_evidence_resolution_v1",
      source_type: "news_article",
      external_id: "article-1",
      observed_at: DateTime.to_iso8601(@received),
      retrieved_at: DateTime.to_iso8601(@received),
      canonical_uri: selected["public_url"],
      raw_object_uri: "raw-envelope:#{ctx.receipt.raw_envelope_id}",
      source_name: selected["publisher_label"],
      acl: %{"privacy" => "public", "participants" => []},
      ingestion_run_key: "resolution-case:#{case_id}",
      metadata: %{}
    }
  end

  defp register(db) do
    {:ok, registration} =
      SourceRegistry.register_source(db, %{
        tenant_id: @tenant,
        source_key: @source,
        adapter_module: FixtureAdapter,
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

    registration
  end

  defp scope, do: %{tenant_id: @tenant, source_key: @source}
end
