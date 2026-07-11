defmodule Primeradiant.SourceEvidenceRecordsTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    RawEnvelope,
    ResolutionAttempt,
    ResolutionCase,
    ResolutionEvidence,
    State
  }

  @tenant "10000000-0000-0000-0000-000000001656"

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-source-evidence-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "SER-V1 preserves an immutable envelope and dedupes its case on replay", %{
    db_path: db_path
  } do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    digest = ChangesetStore.hash("preserved source bytes")

    envelope =
      ChangesetStore.insert!(RawEnvelope, %{
        tenant_id: @tenant,
        source_key: "registered-source",
        adapter_version: "adapter-v1",
        source_event_external_id: "event-42",
        received_at: now,
        content_digest: digest,
        integrity_metadata: %{"algorithm" => "sha256"},
        raw_object_ref: "object://source/event-42",
        visibility: "tenant",
        correlation_id: "correlation-42",
        idempotency_key: "registered-source:event-42:#{digest}:adapter-v1"
      })

    persisted_envelope = DurableSoupDb.insert_deduped!(db_path, :raw_envelopes, envelope)

    replayed_envelope =
      ChangesetStore.insert!(RawEnvelope, %{
        tenant_id: @tenant,
        source_key: envelope.source_key,
        adapter_version: envelope.adapter_version,
        source_event_external_id: envelope.source_event_external_id,
        received_at: now,
        content_digest: envelope.content_digest,
        integrity_metadata: %{"algorithm" => "sha256", "replayed" => true},
        raw_object_ref: "object://changed-on-replay",
        visibility: "tenant",
        correlation_id: "different-correlation",
        idempotency_key: envelope.idempotency_key
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :raw_envelopes, &1))

    assert replayed_envelope.id == persisted_envelope.id
    assert replayed_envelope.raw_object_ref == "object://source/event-42"
    assert replayed_envelope.integrity_metadata == %{"algorithm" => "sha256"}
    assert DurableSoupDb.table_count(db_path, "raw_envelopes", @tenant) == 1

    resolution_case =
      ChangesetStore.insert!(ResolutionCase, %{
        tenant_id: @tenant,
        raw_envelope_id: persisted_envelope.id,
        policy_version: "policy-v1",
        state: "received",
        attempt_count: 0,
        config_policy_hash: ChangesetStore.hash("policy-v1"),
        trace_id: "trace-42"
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_cases, &1))

    replayed_case =
      ChangesetStore.insert!(ResolutionCase, %{
        tenant_id: @tenant,
        raw_envelope_id: persisted_envelope.id,
        policy_version: "policy-v1",
        state: "validating",
        attempt_count: 9,
        config_policy_hash: ChangesetStore.hash("policy-v1"),
        trace_id: "different-trace"
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_cases, &1))

    assert replayed_case.id == resolution_case.id
    assert replayed_case.state == "received"
    assert DurableSoupDb.table_count(db_path, "resolution_cases", @tenant) == 1

    evidence =
      ChangesetStore.insert!(ResolutionEvidence, %{
        tenant_id: @tenant,
        resolution_case_id: resolution_case.id,
        kind: "raw_field",
        value: "Observed Publisher",
        source: "raw_envelope",
        locator: %{"path" => "$.publisher"},
        digest: ChangesetStore.hash("Observed Publisher"),
        retrieved_at: now,
        visibility: "tenant",
        provenance: %{"adapter_version" => "adapter-v1"},
        transformation_chain: [%{"transform" => "identity"}]
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_evidence, &1))

    attempt =
      ChangesetStore.insert!(ResolutionAttempt, %{
        tenant_id: @tenant,
        resolution_case_id: resolution_case.id,
        raw_envelope_id: persisted_envelope.id,
        raw_envelope_digest: digest,
        attempt_key: "#{envelope.idempotency_key}:policy-v1:normalizer-v1:1",
        stage: "normalizing",
        input_hash: digest,
        attempt_ordinal: 1,
        budgets_consumed: %{"elapsed_ms" => 1},
        outcome: "evidence_recorded",
        response_evidence_refs: [evidence.id],
        started_at: now,
        ended_at: now
      })
      |> then(&DurableSoupDb.insert_deduped!(db_path, :resolution_attempts, &1))

    State.new(tenant_id: @tenant)
    |> State.append(:raw_envelopes, persisted_envelope)
    |> State.append(:resolution_cases, resolution_case)
    |> State.append(:resolution_evidence, evidence)
    |> State.append(:resolution_attempts, attempt)
    |> then(
      &DurableSoupDb.persist!(db_path, &1, %{
        source_kind: "source_evidence_test",
        source_db_path: "read-only-fixture",
        source_row_count: 1
      })
    )

    loaded = DurableSoupDb.load_tenant(db_path, @tenant)
    loaded_envelope = Enum.find(loaded.raw_envelopes, &(&1.id == attempt.raw_envelope_id))

    loaded_evidence =
      Enum.find(loaded.resolution_evidence, &(&1.id in attempt.response_evidence_refs))

    assert loaded_envelope.content_digest == attempt.raw_envelope_digest
    assert loaded_evidence.locator == %{"path" => "$.publisher"}
  end
end
