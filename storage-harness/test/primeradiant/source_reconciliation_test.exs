defmodule Primeradiant.SourceReconciliationTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb}

  @tenant "50000000-0000-0000-0000-000000001656"
  @source "reconcile-fixture"
  @at ~U[2026-07-10 18:00:00.000000Z]

  defmodule Reader do
    @behaviour Primeradiant.Ingestion.SourceDeliveryReader
    def events_after(cursor, head), do: send_and_get({:after, cursor, head})
    def events_at(positions), do: send_and_get({:at, positions})

    defp send_and_get(message) do
      send(Process.get(:test_pid), message)
      Process.get({__MODULE__, message}) || []
    end
  end

  defmodule Adapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter

    def to_candidate(raw_envelope, _ctx) do
      {:ok,
       %{
         raw_refs: [raw_envelope.id],
         declared_identity: raw_envelope.source_event_external_id,
         declared_cursor: raw_envelope.integrity_metadata["source_position"],
         raw_fields: %{},
         visibility: raw_envelope.visibility,
         adapter_provenance: %{"adapter" => "fixture", "version" => "adapter-v1"}
       }}
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "source-reconcile-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    Process.put(:test_pid, self())
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "SER-V11 reads only beyond cursor and open gaps, then replays idempotently", %{db_path: db} do
    register(db)
    event = descriptor(2, "event-2", "digest-2")
    Process.put({Reader, {:after, 0, 2}}, [event])
    Process.put({Reader, {:at, []}}, [])

    assert {:ok, %{receipts: [{:ok, %{duplicate: false}}]}} =
             SourceRegistry.reconcile(db, scope(2), Reader)

    assert_received {:after, 0, 2}
    assert_received {:at, []}
    assert DurableSoupDb.table_count(db, "raw_envelopes", @tenant) == 1

    assert [%{source_position: "1", status: "open"}] =
             DurableSoupDb.source_gap_records(db, @tenant, @source)

    Process.put({Reader, {:at, [1]}}, [])
    assert {:ok, %{receipts: []}} = SourceRegistry.reconcile(db, scope(2), Reader)
    assert DurableSoupDb.table_count(db, "raw_envelopes", @tenant) == 1
    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) == 1
  end

  test "open-gap receipt closes through processed transition and advances contiguously", %{
    db_path: db
  } do
    register(db)
    event2 = descriptor(2, "event-2", "digest-2")
    Process.put({Reader, {:after, 0, 2}}, [event2])
    Process.put({Reader, {:at, []}}, [])
    assert {:ok, %{receipts: [{:ok, second}]}} = SourceRegistry.reconcile(db, scope(2), Reader)

    assert {:ok, registration} =
             SourceRegistry.mark_processed(
               db,
               Map.merge(scope(2), %{source_position: 2, outcome_ref: "outcome-2"})
             )

    assert registration.cursor["contiguous_position"] == 0
    event1 = descriptor(1, "event-1", "digest-1")
    Process.put({Reader, {:after, 0, 2}}, [event2])
    Process.put({Reader, {:at, [1]}}, [event1])
    assert {:ok, %{receipts: [{:ok, first}]}} = SourceRegistry.reconcile(db, scope(2), Reader)
    assert first.resolution_case_id != second.resolution_case_id

    assert {:ok, registration} =
             SourceRegistry.mark_processed(
               db,
               Map.merge(scope(2), %{source_position: 1, outcome_ref: "outcome-1"})
             )

    assert registration.cursor["contiguous_position"] == 2

    assert [%{source_position: "1", status: "closed"}] =
             DurableSoupDb.source_gap_records(db, @tenant, @source)
  end

  test "terminal cases stay untouched while only a due retry runs", %{db_path: db} do
    register(db)

    cases =
      [
        {1, "unresolved", nil},
        {2, "quarantined", nil},
        {3, "refused", nil},
        {4, "failed_terminal", nil},
        {5, "retry_scheduled", DateTime.add(@at, -1, :second)},
        {6, "retry_scheduled", DateTime.add(@at, 60, :second)},
        {7, "received", nil}
      ]
      |> Enum.map(fn {position, state, retry_at} ->
        receipt = receive_direct(db, position, "event-#{position}", "digest-#{position}")
        resolution_case = DurableSoupDb.resolution_case(db, @tenant, receipt.resolution_case_id)

        resolution_case
        |> ChangesetStore.update!(%{
          state: state,
          next_retry_at: retry_at,
          outcome_code: if(state == "retry_scheduled", do: "retry_scheduled:fixture", else: state)
        })
        |> then(&DurableSoupDb.put_resolution_case!(db, &1))
      end)

    before_counts = counts(db)
    Process.put({Reader, {:after, 0, 0}}, [])
    Process.put({Reader, {:at, [1, 2, 3, 4]}}, [])
    assert {:ok, %{receipts: [], retries: [_]}} = SourceRegistry.reconcile(db, scope(0), Reader)

    reloaded = Map.new(cases, &{&1.id, DurableSoupDb.resolution_case(db, @tenant, &1.id)})

    for resolution_case <- Enum.take(cases, 4) do
      assert reloaded[resolution_case.id].state == resolution_case.state
      assert reloaded[resolution_case.id].updated_at == resolution_case.updated_at
    end

    assert reloaded[Enum.at(cases, 4).id].state != "retry_scheduled"
    assert reloaded[Enum.at(cases, 5).id].state == "retry_scheduled"
    assert reloaded[Enum.at(cases, 6).id].state == "received"
    assert counts(db).raw_envelopes == before_counts.raw_envelopes
    assert counts(db).resolution_cases == before_counts.resolution_cases
  end

  test "digest conflict and distinct identity at a permitted position are durable and replay-safe",
       %{db_path: db} do
    register(db)
    receive_direct(db, 1, "event-1", "digest-original")
    conflict = descriptor(1, "event-1", "digest-conflict")
    distinct = descriptor(1, "event-distinct", "digest-distinct")
    Process.put({Reader, {:after, 0, 1}}, [conflict, distinct])
    Process.put({Reader, {:at, []}}, [])

    assert {:ok, %{receipts: receipts}} = SourceRegistry.reconcile(db, scope(1), Reader)

    assert Enum.any?(
             receipts,
             &match?({:ok, %{outcome_code: "quarantined:source_identity_digest_conflict"}}, &1)
           )

    assert Enum.any?(receipts, &match?({:ok, %{state: "received"}}, &1))
    assert DurableSoupDb.table_count(db, "raw_envelopes", @tenant) == 3
    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) == 3
    assert DurableSoupDb.table_count(db, "resolution_outcomes", @tenant) == 1
    registration = DurableSoupDb.source_registration(db, @tenant, @source)
    assert registration.cursor["conflict_positions"] == [1]

    assert {:ok, %{receipts: []}} = SourceRegistry.reconcile(db, scope(1), Reader)
    assert DurableSoupDb.table_count(db, "raw_envelopes", @tenant) == 3
  end

  test "mixed valid and wrong-typed reader events preserve valid receipt and return typed entries",
       %{
         db_path: db
       } do
    register(db)
    valid = descriptor(1, "event-1", "digest-1")
    wrong_position = descriptor(2, "event-2", "digest-2") |> Map.put(:source_position, "2")
    Process.put({Reader, {:after, 0, 2}}, [valid, wrong_position, :not_an_event])
    Process.put({Reader, {:at, []}}, [])

    assert {:ok, %{receipts: receipts}} = SourceRegistry.reconcile(db, scope(2), Reader)

    assert Enum.count(receipts, &match?({:error, {:malformed_event, _}}, &1)) == 2
    assert {:error, {:malformed_event, "2"}} in receipts
    assert {:error, {:malformed_event, nil}} in receipts
    assert Enum.any?(receipts, &match?({:ok, %{duplicate: false}}, &1))
    assert DurableSoupDb.table_count(db, "raw_envelopes", @tenant) == 1
    assert DurableSoupDb.table_count(db, "resolution_cases", @tenant) == 1
  end

  defp register(db) do
    {:ok, _} =
      SourceRegistry.register_source(db, %{
        tenant_id: @tenant,
        source_key: @source,
        adapter_module: Adapter,
        adapter_version: "adapter-v1",
        mode: "shadow",
        resolution_policy: %{"version" => "v1", "source_class" => "public_article"},
        policy_version: "v1",
        budgets: %{
          "max_attempts" => 2,
          "total_case_ms" => 1_000,
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

  defp descriptor(position, identity, digest) do
    %{
      source_key: @source,
      source_event_external_id: identity,
      source_position: position,
      received_at: @at,
      content_digest: digest,
      raw_object_ref: "object://#{identity}",
      retained_bytes: nil,
      visibility: "public",
      correlation_id: "correlation-#{identity}",
      integrity_metadata: %{}
    }
  end

  defp receive_direct(db, position, identity, digest) do
    {:ok, receipt} =
      SourceRegistry.receive_envelope(
        db,
        descriptor(position, identity, digest) |> Map.put(:tenant_id, @tenant)
      )

    receipt
  end

  defp counts(db) do
    %{
      raw_envelopes: DurableSoupDb.table_count(db, "raw_envelopes", @tenant),
      resolution_cases: DurableSoupDb.table_count(db, "resolution_cases", @tenant)
    }
  end

  defp scope(head), do: %{tenant_id: @tenant, source_key: @source, head: head, now: @at}
end
