defmodule Primeradiant.SourceRegistryTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb}

  @tenant "20000000-0000-0000-0000-000000001656"
  @source "fixture-source"
  @received_at ~U[2026-07-10 18:00:00.000000Z]

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-source-registry-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "registration records canonical policy hash and owns policy without an override seam", %{
    db_path: db_path
  } do
    policy = %{
      "version" => "policy-v1",
      "source_class" => "public_article",
      "thresholds" => %{"publisher_domain" => 0.9, "publisher_label" => 0.8}
    }

    assert {:ok, registration} =
             SourceRegistry.register_source(db_path, %{
               tenant_id: @tenant,
               source_key: @source,
               adapter_module: FixtureAdapter,
               adapter_version: "adapter-v1",
               resolution_policy: policy,
               policy_version: "policy-v1",
               budgets: valid_budgets(1_000),
               config: %{"resolvers" => []},
               initial_cursor: 7
             })

    assert registration.mode == "disabled"

    assert registration.policy_hash ==
             ChangesetStore.hash(
               ~s({"source_class":"public_article","thresholds":{"publisher_domain":0.9,"publisher_label":0.8},"version":"policy-v1"})
             )

    assert registration.cursor["contiguous_position"] == 7

    assert {:ok,
            %{
              resolution_policy: ^policy,
              policy_version: "policy-v1",
              policy_hash: policy_hash
            }} = SourceRegistry.policy_for(db_path, source_scope())

    assert policy_hash == registration.policy_hash
    refute function_exported?(SourceRegistry, :policy_for, 3)

    updated_policy = Map.put(policy, "version", "policy-v2")

    assert {:ok, updated_registration} =
             SourceRegistry.register_source(db_path, %{
               tenant_id: @tenant,
               source_key: @source,
               adapter_module: FixtureAdapter,
               adapter_version: "adapter-v2",
               mode: "shadow",
               resolution_policy: updated_policy,
               policy_version: "policy-v2",
               budgets: valid_budgets(2_000),
               config: %{"resolvers" => []}
             })

    assert updated_registration.id == registration.id
    assert updated_registration.policy_hash != registration.policy_hash
    assert updated_registration.cursor["contiguous_position"] == 7
    assert DurableSoupDb.table_count(db_path, "source_registrations", @tenant) == 1

    assert SourceRegistry.policy_for(
             db_path,
             Map.put(source_scope(), :source_key, "unregistered")
           ) == {:error, :policy_not_registered}
  end

  test "same identity and digest replay returns the same envelope and case refs", %{
    db_path: db_path
  } do
    register_source(db_path)
    descriptor = envelope_descriptor(1, "event-1", "digest-1")

    assert {:ok, first} = SourceRegistry.receive_envelope(db_path, descriptor)
    assert first.duplicate == false
    assert first.state == "received"

    assert {:ok, replay} =
             SourceRegistry.receive_envelope(db_path, %{
               descriptor
               | raw_object_ref: "object://changed-on-replay",
                 correlation_id: "changed-correlation"
             })

    assert replay.duplicate == true
    assert replay.raw_envelope_id == first.raw_envelope_id
    assert replay.resolution_case_id == first.resolution_case_id
    assert DurableSoupDb.table_count(db_path, "raw_envelopes", @tenant) == 1
    assert DurableSoupDb.table_count(db_path, "resolution_cases", @tenant) == 1
  end

  test "same identity with a different digest is quarantined and remains non-contiguous", %{
    db_path: db_path
  } do
    register_source(db_path)

    assert {:ok, first} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(1, "event-1", "digest-1")
             )

    assert {:ok, conflict} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(1, "event-1", "digest-2")
             )

    assert conflict.raw_envelope_id != first.raw_envelope_id
    assert conflict.resolution_case_id != first.resolution_case_id
    assert conflict.state == "quarantined"
    assert conflict.outcome_code == "quarantined:source_identity_digest_conflict"
    assert is_binary(conflict.outcome_ref)
    assert DurableSoupDb.table_count(db_path, "raw_envelopes", @tenant) == 2
    assert DurableSoupDb.table_count(db_path, "resolution_cases", @tenant) == 2
    assert DurableSoupDb.table_count(db_path, "resolution_outcomes", @tenant) == 1

    assert SourceRegistry.mark_processed(
             db_path,
             Map.merge(source_scope(), %{source_position: 1, outcome_ref: conflict.outcome_ref})
           ) == {:error, :source_identity_digest_conflict}

    registration = DurableSoupDb.source_registration(db_path, @tenant, @source)
    assert registration.cursor["contiguous_position"] == 0
    assert registration.cursor["conflict_positions"] == [1]

    assert {:ok, health} = SourceRegistry.health(db_path, source_scope())
    assert health.quarantine_count == 1
  end

  test "sequence discontinuity opens a gap and cursor advances only through adjacent processed positions",
       %{
         db_path: db_path
       } do
    register_source(db_path)

    assert {:ok, _second} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(2, "event-2", "digest-2")
             )

    assert [%{source_position: "1", status: "open"}] =
             DurableSoupDb.source_gap_records(db_path, @tenant, @source)

    assert {:ok, registration} =
             SourceRegistry.mark_processed(
               db_path,
               Map.merge(source_scope(), %{source_position: 2, outcome_ref: "outcome-2"})
             )

    assert registration.cursor["contiguous_position"] == 0
    assert registration.cursor["processed_positions"] == [2]

    assert {:ok, _first} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(1, "event-1", "digest-1")
             )

    assert {:ok, registration} =
             SourceRegistry.mark_processed(
               db_path,
               Map.merge(source_scope(), %{source_position: 1, outcome_ref: "outcome-1"})
             )

    assert registration.cursor["contiguous_position"] == 2
    assert registration.cursor["processed_positions"] == []

    assert [%{source_position: "1", status: "closed"}] =
             DurableSoupDb.source_gap_records(db_path, @tenant, @source)

    assert {:ok, _third} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(3, "event-3", "digest-3")
             )

    assert {:ok, registration} =
             SourceRegistry.mark_processed(
               db_path,
               Map.merge(source_scope(), %{source_position: 3, admission_ref: "admission-3"})
             )

    assert registration.cursor["contiguous_position"] == 3
  end

  test "receipt, resolution-terminal, and admission timestamps have distinct writers", %{
    db_path: db_path
  } do
    register_source(db_path)

    assert {:ok, _receipt} =
             SourceRegistry.receive_envelope(
               db_path,
               envelope_descriptor(1, "event-1", "digest-1")
             )

    assert {:ok, health} = SourceRegistry.health(db_path, source_scope())
    assert health.last_received_at == @received_at
    assert health.last_resolution_terminal_at == nil
    assert health.last_admission_at == nil
    assert health.resolution_backlog == 1
    assert health.retry_depth == 0
    assert health.circuit_state == "closed"
    assert health.circuit_states == %{"adapter" => "closed", "normalizer" => "closed"}
    assert health.gap_counts == %{open: 0, closed: 0}

    resolution_at = DateTime.add(@received_at, 10, :second)
    admission_at = DateTime.add(@received_at, 20, :second)

    assert {:ok, _registration} =
             SourceRegistry.record_resolution_terminal(
               db_path,
               Map.put(source_scope(), :at, resolution_at)
             )

    assert {:ok, health} = SourceRegistry.health(db_path, source_scope())
    assert health.last_received_at == @received_at
    assert health.last_resolution_terminal_at == resolution_at
    assert health.last_admission_at == nil

    assert {:ok, _registration} =
             SourceRegistry.record_admission(db_path, Map.put(source_scope(), :at, admission_at))

    assert {:ok, health} = SourceRegistry.health(db_path, source_scope())
    assert health.last_received_at == @received_at
    assert health.last_resolution_terminal_at == resolution_at
    assert health.last_admission_at == admission_at
  end

  defp register_source(db_path) do
    assert {:ok, registration} =
             SourceRegistry.register_source(db_path, %{
               tenant_id: @tenant,
               source_key: @source,
               adapter_module: FixtureAdapter,
               adapter_version: "adapter-v1",
               mode: "shadow",
               resolution_policy: %{
                 "version" => "policy-v1",
                 "source_class" => "public_article"
               },
               policy_version: "policy-v1",
               budgets: valid_budgets(1_000),
               config: %{"resolvers" => []},
               initial_cursor: 0
             })

    registration
  end

  defp envelope_descriptor(position, identity, digest) do
    %{
      tenant_id: @tenant,
      source_key: @source,
      source_event_external_id: identity,
      source_position: position,
      received_at: @received_at,
      content_digest: digest,
      integrity_metadata: %{"algorithm" => "sha256"},
      raw_object_ref: "object://#{identity}",
      visibility: "tenant",
      correlation_id: "correlation-#{identity}"
    }
  end

  defp source_scope, do: %{tenant_id: @tenant, source_key: @source}

  defp valid_budgets(total_case_ms) do
    %{
      "max_attempts" => 2,
      "total_case_ms" => total_case_ms,
      "retry_backoff_ms" => 1,
      "per_source_concurrency" => 1,
      "adapter" => %{"time_ms" => 100},
      "normalizer" => %{"time_ms" => 100},
      "resolvers" => %{}
    }
  end
end
