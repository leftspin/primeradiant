defmodule Primeradiant.PackageAcknowledgementTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.{PackageAcknowledgement, SourceRegistry}

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    ResolutionOutcome
  }

  @tenant "70000000-0000-0000-0000-000000001656"
  @source "package-fixture"
  @at ~U[2026-07-10 18:00:00.000000Z]

  defmodule Adapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter
    def to_candidate(_raw_envelope, _ctx), do: raise("ack must not invoke adapter")
  end

  setup do
    root = Path.join(System.tmp_dir!(), "package-ack-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    db = Path.join(root, "soup.sqlite3")
    register(db)
    %{db_path: db}
  end

  test "SER-V7/V12 mixed durable outcomes acknowledge in order with truthful counts", %{
    db_path: db
  } do
    admitted = receive(db, 1, "admitted") |> put_outcome(db, "eligible", admission_ref: "input:1")
    refused = receive(db, 2, "refused") |> put_outcome(db, "refused:no_public_article")

    retry =
      receive(db, 3, "retry")
      |> put_outcome(db, "retry_scheduled:timeout", retry_at: DateTime.add(@at, 60, :second))

    manifest =
      manifest("package-mixed", [
        entry("a", 1, admitted),
        entry("b", 2, refused),
        entry("c", 3, retry)
      ])

    attempts_before = DurableSoupDb.table_count(db, "resolution_attempts", @tenant)

    assert {:ok, result} = PackageAcknowledgement.acknowledge_package(db, manifest)
    assert result.status == "durably_processed"
    assert result.counts == %{admitted: 1, terminal_non_admitted: 1, retry_scheduled: 1}
    assert Enum.map(result.envelope_disposition_refs, & &1["envelope_key"]) == ~w(a b c)

    assert Enum.map(result.envelope_disposition_refs, & &1["resolution_case_id"]) ==
             [admitted.resolution_case_id, refused.resolution_case_id, retry.resolution_case_id]

    assert Enum.at(result.envelope_disposition_refs, 0)["admission_ref"] == "input:1"

    assert Enum.at(result.envelope_disposition_refs, 2)["retry_at"] ==
             DateTime.to_iso8601(DateTime.add(@at, 60, :second))

    assert DurableSoupDb.table_count(db, "resolution_attempts", @tenant) == attempts_before
    registration = DurableSoupDb.source_registration(db, @tenant, @source)
    assert registration.cursor["contiguous_position"] == 3

    assert {:ok, replay} = PackageAcknowledgement.acknowledge_package(db, manifest)
    assert replay.replay == true
    assert replay.acknowledgement_id == result.acknowledgement_id
    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 1
    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor == registration.cursor
  end

  test "pending and missing envelopes name keys only and never move cursor", %{db_path: db} do
    pending = receive(db, 1, "private-evidence-value")
    pending_manifest = manifest("package-pending", [entry("pending-key", 1, pending)])

    assert {:error, failure} = PackageAcknowledgement.acknowledge_package(db, pending_manifest)

    assert failure == %{
             code: :package_not_durably_processed,
             missing_or_pending_envelope_keys: ["pending-key"]
           }

    refute inspect(failure) =~ "private-evidence-value"
    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 0

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor["contiguous_position"] ==
             0

    missing_manifest =
      manifest("package-missing", [
        %{envelope_key: "missing-key", source_position: 2, raw_envelope_id: Ecto.UUID.generate()}
      ])

    assert {:error, missing} = PackageAcknowledgement.acknowledge_package(db, missing_manifest)
    assert missing.missing_or_pending_envelope_keys == ["missing-key"]

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor["contiguous_position"] ==
             0
  end

  test "cursor advances only through contiguous acknowledged positions", %{db_path: db} do
    second = receive(db, 2, "second") |> put_outcome(db, "refused:fixture")
    third = receive(db, 3, "third") |> put_outcome(db, "failed_terminal:fixture")

    assert {:ok, _} =
             PackageAcknowledgement.acknowledge_package(
               db,
               manifest("package-later", [entry("second", 2, second), entry("third", 3, third)])
             )

    registration = DurableSoupDb.source_registration(db, @tenant, @source)
    assert registration.cursor["contiguous_position"] == 0
    assert registration.cursor["processed_positions"] == [2, 3]

    first = receive(db, 1, "first") |> put_outcome(db, "unresolved:fixture")

    assert {:ok, _} =
             PackageAcknowledgement.acknowledge_package(
               db,
               manifest("package-first", [entry("first", 1, first)])
             )

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor["contiguous_position"] ==
             3
  end

  test "package digest conflict is durable and does not move cursor", %{db_path: db} do
    receipt = receive(db, 1, "one") |> put_outcome(db, "refused:fixture")
    original = manifest("package-conflict", [entry("one", 1, receipt)])
    assert {:ok, _} = PackageAcknowledgement.acknowledge_package(db, original)

    changed = manifest("package-conflict", [entry("changed-key", 1, receipt)])
    cursor_before = DurableSoupDb.source_registration(db, @tenant, @source).cursor

    assert {:error, %{code: "refused:package_identity_digest_conflict", refusal_ref: ref}} =
             PackageAcknowledgement.acknowledge_package(db, changed)

    assert is_binary(ref)
    rows = DurableSoupDb.package_acknowledgements_for_package(db, @tenant, "package-conflict")
    assert Enum.any?(rows, &(&1.status == "refused:package_identity_digest_conflict"))
    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor == cursor_before

    assert {:error, %{refusal_ref: ^ref}} =
             PackageAcknowledgement.acknowledge_package(db, changed)

    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 2
  end

  test "manifest count and digest mismatches are typed and write nothing", %{db_path: db} do
    receipt = receive(db, 1, "one") |> put_outcome(db, "refused:fixture")
    valid = manifest("package-invalid", [entry("one", 1, receipt)])

    assert PackageAcknowledgement.acknowledge_package(db, %{valid | expected_count: 2}) ==
             {:error, :malformed_manifest}

    assert PackageAcknowledgement.acknowledge_package(db, %{valid | manifest_digest: "wrong"}) ==
             {:error, :manifest_digest_mismatch}

    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 0
  end

  test "replay repairs zero and partially applied cursor marks without duplicate rows", %{
    db_path: db
  } do
    first = receive(db, 1, "first") |> put_outcome(db, "refused:first")
    second = receive(db, 2, "second") |> put_outcome(db, "refused:second")
    manifest = manifest("package-repair", [entry("first", 1, first), entry("second", 2, second)])
    assert {:ok, original} = PackageAcknowledgement.acknowledge_package(db, manifest)

    put_cursor(db, %{
      "contiguous_position" => 0,
      "processed_positions" => [],
      "conflict_positions" => []
    })

    assert {:ok, replay} = PackageAcknowledgement.acknowledge_package(db, manifest)
    assert replay.replay
    assert replay.acknowledgement_id == original.acknowledgement_id

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor["contiguous_position"] ==
             2

    put_cursor(db, %{
      "contiguous_position" => 1,
      "processed_positions" => [],
      "conflict_positions" => []
    })

    assert {:ok, _} = PackageAcknowledgement.acknowledge_package(db, manifest)

    assert DurableSoupDb.source_registration(db, @tenant, @source).cursor["contiguous_position"] ==
             2

    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 1
  end

  test "manifest positions bind to durable raw positions and ambiguous identities fail key-only",
       %{db_path: db} do
    receipt = receive(db, 1, "identity") |> put_outcome(db, "refused:fixture")
    wrong = manifest("package-wrong-position", [entry("wrong-key", 2, receipt)])

    assert {:error, wrong_failure} = PackageAcknowledgement.acknowledge_package(db, wrong)
    assert wrong_failure.missing_or_pending_envelope_keys == ["wrong-key"]
    assert DurableSoupDb.table_count(db, "package_acknowledgements", @tenant) == 0

    {:ok, conflict} =
      SourceRegistry.receive_envelope(db, %{
        tenant_id: @tenant,
        source_key: @source,
        source_event_external_id: "identity",
        source_position: 1,
        received_at: @at,
        content_digest: "different-digest",
        retained_bytes: "private:conflict",
        visibility: "private",
        correlation_id: "correlation-conflict"
      })

    assert conflict.state == "quarantined"

    ambiguous =
      manifest("package-ambiguous", [
        %{envelope_key: "ambiguous-key", source_position: 1, source_event_external_id: "identity"}
      ])

    assert {:error, ambiguous_failure} =
             PackageAcknowledgement.acknowledge_package(db, ambiguous)

    assert ambiguous_failure.missing_or_pending_envelope_keys == ["ambiguous-key"]
    refute inspect(ambiguous_failure) =~ "private:"
  end

  test "only the latest exact current outcome can qualify", %{db_path: db} do
    stale_eligible = receive(db, 1, "stale-eligible")
    insert_outcome(db, stale_eligible.resolution_case_id, "eligible", admission_ref: "old-input")
    latest_retry = insert_outcome(db, stale_eligible.resolution_case_id, "retry_scheduled:again")

    set_case(
      db,
      stale_eligible.resolution_case_id,
      "retry_scheduled",
      "retry_scheduled:again",
      DateTime.add(@at, 60, :second)
    )

    retry_manifest = manifest("package-current-retry", [entry("retry-key", 1, stale_eligible)])
    assert {:ok, retry_result} = PackageAcknowledgement.acknowledge_package(db, retry_manifest)
    assert hd(retry_result.envelope_disposition_refs)["outcome_ref"] == latest_retry.id
    assert hd(retry_result.envelope_disposition_refs)["admission_ref"] == nil

    pending = receive(db, 2, "pending-with-stale-terminal")
    insert_outcome(db, pending.resolution_case_id, "refused:old")
    set_case(db, pending.resolution_case_id, "resolving", nil, nil)
    pending_manifest = manifest("package-stale-terminal", [entry("pending-key", 2, pending)])

    assert {:error, %{missing_or_pending_envelope_keys: ["pending-key"]}} =
             PackageAcknowledgement.acknowledge_package(db, pending_manifest)

    repeated = receive(db, 3, "repeated-retry")
    insert_outcome(db, repeated.resolution_case_id, "retry_scheduled:same")
    latest = insert_outcome(db, repeated.resolution_case_id, "retry_scheduled:same")

    set_case(
      db,
      repeated.resolution_case_id,
      "retry_scheduled",
      "retry_scheduled:same",
      DateTime.add(@at, 60, :second)
    )

    repeated_manifest = manifest("package-repeated-retry", [entry("repeated-key", 3, repeated)])

    assert {:ok, repeated_result} =
             PackageAcknowledgement.acknowledge_package(db, repeated_manifest)

    assert hd(repeated_result.envelope_disposition_refs)["outcome_ref"] == latest.id

    nil_retry = receive(db, 4, "nil-retry")
    insert_outcome(db, nil_retry.resolution_case_id, "eligible", admission_ref: "stale-input")
    insert_outcome(db, nil_retry.resolution_case_id, "retry_scheduled:nil")
    set_case(db, nil_retry.resolution_case_id, "retry_scheduled", "retry_scheduled:nil", nil)
    nil_manifest = manifest("package-nil-retry", [entry("nil-key", 4, nil_retry)])

    assert {:error, %{missing_or_pending_envelope_keys: ["nil-key"]}} =
             PackageAcknowledgement.acknowledge_package(db, nil_manifest)
  end

  test "concurrent first claims produce one ack and one stable refusal", %{db_path: db} do
    first = receive(db, 1, "race-first") |> put_outcome(db, "refused:first")
    second = receive(db, 2, "race-second") |> put_outcome(db, "refused:second")
    first_manifest = manifest("package-race", [entry("first", 1, first)])
    second_manifest = manifest("package-race", [entry("second", 2, second)])

    [first_result, second_result] =
      [first_manifest, second_manifest]
      |> Enum.map(&Task.async(fn -> PackageAcknowledgement.acknowledge_package(db, &1) end))
      |> Enum.map(&Task.await(&1, 10_000))

    assert Enum.count([first_result, second_result], &match?({:ok, _}, &1)) == 1

    assert Enum.count(
             [first_result, second_result],
             &match?({:error, %{code: "refused:package_identity_digest_conflict"}}, &1)
           ) == 1

    rows = DurableSoupDb.package_acknowledgements_for_package(db, @tenant, "package-race")
    assert Enum.count(rows, &(&1.status == "durably_processed")) == 1
    assert Enum.count(rows, &(&1.status == "refused:package_identity_digest_conflict")) == 1
    ack = Enum.find(rows, &(&1.status == "durably_processed"))
    refused = Enum.find(rows, &(&1.status == "refused:package_identity_digest_conflict"))

    refused_manifest =
      if refused.manifest_digest == first_manifest.manifest_digest,
        do: first_manifest,
        else: second_manifest

    assert {:error, %{refusal_ref: refusal_ref}} =
             PackageAcknowledgement.acknowledge_package(db, refused_manifest)

    assert refusal_ref == refused.id
    registration = DurableSoupDb.source_registration(db, @tenant, @source)
    acknowledged_key = ack.envelope_disposition_refs |> hd() |> Map.fetch!("envelope_key")

    case acknowledged_key do
      "first" ->
        assert registration.cursor["contiguous_position"] == 1
        refute 2 in registration.cursor["processed_positions"]

      "second" ->
        assert registration.cursor["contiguous_position"] == 0
        assert registration.cursor["processed_positions"] == [2]
    end
  end

  test "persisted member shape is exact and envelope order changes the digest", %{db_path: db} do
    first = receive(db, 1, "shape-first") |> put_outcome(db, "refused:first")
    second = receive(db, 2, "shape-second") |> put_outcome(db, "refused:second")
    entries = [entry("first", 1, first), entry("second", 2, second)]
    forward = manifest("package-shape", entries)
    reversed = manifest("package-shape", Enum.reverse(entries))
    refute forward.manifest_digest == reversed.manifest_digest

    assert {:ok, result} = PackageAcknowledgement.acknowledge_package(db, forward)

    exact_keys =
      ~w(envelope_key raw_envelope_id resolution_case_id outcome_code outcome_ref admission_ref retry_at policy_hash)
      |> Enum.sort()

    assert Enum.all?(result.envelope_disposition_refs, fn ref ->
             Map.keys(ref) |> Enum.sort() == exact_keys
           end)
  end

  defp receive(db, position, identity) do
    {:ok, receipt} =
      SourceRegistry.receive_envelope(db, %{
        tenant_id: @tenant,
        source_key: @source,
        source_event_external_id: identity,
        source_position: position,
        received_at: @at,
        content_digest: ChangesetStore.hash(identity),
        retained_bytes: "private:#{identity}",
        visibility: "private",
        correlation_id: "correlation-#{identity}"
      })

    receipt
  end

  defp put_outcome(receipt, db, code, opts \\ []) do
    resolution_case = DurableSoupDb.resolution_case(db, @tenant, receipt.resolution_case_id)
    family = code |> String.split(":", parts: 2) |> hd()
    retry_at = Keyword.get(opts, :retry_at)

    resolution_case
    |> ChangesetStore.update!(%{state: family, outcome_code: code, next_retry_at: retry_at})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))

    outcome =
      ChangesetStore.insert!(ResolutionOutcome, %{
        tenant_id: @tenant,
        resolution_case_id: resolution_case.id,
        outcome_code: code,
        reason: code,
        retryable: family == "retry_scheduled",
        validator_version: "fixture",
        admission_material_ref: Keyword.get(opts, :admission_ref)
      })
      |> then(&DurableSoupDb.insert_resolution_outcome!(db, &1))

    Map.merge(receipt, %{outcome_id: outcome.id})
  end

  defp insert_outcome(db, case_id, code, opts \\ []) do
    ChangesetStore.insert!(ResolutionOutcome, %{
      tenant_id: @tenant,
      resolution_case_id: case_id,
      outcome_code: code,
      reason: code,
      retryable: String.starts_with?(code, "retry_scheduled:"),
      validator_version: "fixture",
      admission_material_ref: Keyword.get(opts, :admission_ref)
    })
    |> then(&DurableSoupDb.insert_resolution_outcome!(db, &1))
  end

  defp set_case(db, case_id, state, code, retry_at) do
    db
    |> DurableSoupDb.resolution_case(@tenant, case_id)
    |> ChangesetStore.update!(%{state: state, outcome_code: code, next_retry_at: retry_at})
    |> then(&DurableSoupDb.put_resolution_case!(db, &1))
  end

  defp put_cursor(db, cursor) do
    db
    |> DurableSoupDb.source_registration(@tenant, @source)
    |> ChangesetStore.update!(%{cursor: cursor})
    |> then(&DurableSoupDb.put_source_registration!(db, &1))
  end

  defp entry(key, position, receipt),
    do: %{envelope_key: key, source_position: position, raw_envelope_id: receipt.raw_envelope_id}

  defp manifest(package_id, envelopes) do
    manifest = %{
      package_id: package_id,
      manifest_digest: nil,
      source_key: @source,
      tenant_id: @tenant,
      envelopes: envelopes,
      expected_count: length(envelopes)
    }

    %{manifest | manifest_digest: PackageAcknowledgement.manifest_digest(manifest)}
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
            "acl" => %{"privacy" => "private", "participants" => ["participant"]}
          }
        },
        initial_cursor: 0
      })
  end
end
