defmodule Primeradiant.ResolutionPipelineTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.SourceRegistry
  alias Primeradiant.Ingestion.Resolution.{Case, Inference, ResolverPlanner}
  alias Primeradiant.StorageHarness.{ChangesetStore, DurableSoupDb}

  @tenant "20000000-0000-0000-0000-000000001656"
  @received_at ~U[2026-07-10 18:00:00.000000Z]

  defmodule FixtureAdapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter

    @impl true
    def to_candidate(raw_envelope, _ctx) do
      fields = Jason.decode!(raw_envelope.retained_bytes)

      if fields["malformed"] do
        {:ok, %{adapter_provenance: nil}}
      else
        {:ok,
         %{
           raw_refs: [raw_envelope.id],
           declared_identity: raw_envelope.source_event_external_id,
           declared_cursor: raw_envelope.integrity_metadata["source_position"],
           raw_fields: fields,
           visibility: raw_envelope.visibility,
           adapter_provenance: %{"adapter" => "fixture", "version" => "adapter-v1"}
         }}
      end
    end
  end

  defmodule HealthyResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget), do: {:ok, [], %{requests: 1, bytes: 0, redirects: 0}}
  end

  defmodule CanonicalResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "fetched_response_canonical_url",
           value: "https://publisher.example/canonical",
           normalized_value: "https://publisher.example/canonical",
           field_name: "public_url",
           confidence: 0.9,
           transform: "copy",
           locator: %{"response_url" => "https://publisher.example/final"},
           provenance: %{"adapter" => "untrusted-claim"}
         }
       ], %{requests: 1, bytes: 20, redirects: 1}}
    end
  end

  defmodule MalformedCanonicalResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "fetched_response_canonical_url",
           value: "https://publisher.example/canonical",
           normalized_value: "https://publisher.example/canonical",
           field_name: "public_url",
           confidence: 0.9,
           transform: "copy",
           locator: %{}
         }
       ], %{requests: 1, bytes: 20, redirects: 0}}
    end
  end

  defmodule ViolatingAcceptedFinalResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "fetched_response_accepted_final_url",
           value: "https://publisher.example/not-final",
           normalized_value: "https://publisher.example/not-final",
           field_name: "public_url",
           confidence: 0.9,
           transform: "copy",
           locator: %{"response_url" => "https://publisher.example/final"}
         }
       ], %{requests: 1, bytes: 20, redirects: 1}}
    end
  end

  defmodule AcceptedFinalResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "fetched_response_accepted_final_url",
           value: "https://publisher.example/final",
           normalized_value: "https://publisher.example/final",
           field_name: "public_url",
           confidence: 0.9,
           transform: "copy",
           locator: %{"response_url" => "https://publisher.example/final"}
         }
       ], %{requests: 1, bytes: 20, redirects: 1}}
    end
  end

  defmodule OgSiteNameResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "fetched_response_og_site_name",
           value: "Fetched Publisher",
           normalized_value: "Fetched Publisher",
           field_name: "publisher_label",
           confidence: 1.0,
           transform: "copy",
           locator: %{"response_url" => "https://publisher.example/article"}
         }
       ], %{requests: 1, bytes: 20, redirects: 0}}
    end
  end

  defmodule ResolverNonPublicAccess do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    def resolve(_packet, _budget) do
      {:ok,
       [
         %{
           kind: "non_public_access",
           value: "declared_non_public",
           normalized_value: "no_public_url",
           field_name: "public_url",
           confidence: 1.0,
           transform: "explicit_no_public_url",
           locator: %{"non_public_reason" => "source_declared"}
         }
       ], %{requests: 1, bytes: 0, redirects: 0}}
    end
  end

  defmodule SlowResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget) do
      Process.sleep(50)
      {:ok, [], %{requests: 1, bytes: 0, redirects: 0}}
    end
  end

  defmodule BlockingResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget) do
      test = Process.whereis(:t1656_interleaving_test)
      send(test, {:blocking_resolver_entered, self()})

      receive do
        :release -> {:ok, [], %{requests: 1, bytes: 0, redirects: 0}}
      end
    end
  end

  defmodule ByteBudgetResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget) do
      {:ok, [%{kind: "response", value: "oversized", locator: %{"response" => "body"}}],
       %{requests: 1, bytes: 101, redirects: 0}}
    end
  end

  defmodule RedirectBudgetResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, budget) do
      url = "https://publisher.example/loop"

      hops =
        Enum.map(0..budget.redirects, fn hop ->
          %{kind: "redirect", value: url, locator: %{"hop" => hop, "location" => url}}
        end)

      {:ok, hops, %{requests: 1, bytes: 20, redirects: length(hops)}}
    end
  end

  defmodule CrashReplayResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget) do
      send(Process.whereis(:t1656_crash_replay_test), :resolver_entered)
      Process.sleep(1_000)
      {:ok, [], %{requests: 1, bytes: 0, redirects: 0}}
    end
  end

  defmodule RateLimitedResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget), do: {:outcome, :rate_limited}
  end

  defmodule CrashingResolver do
    @behaviour Primeradiant.Ingestion.Resolution.EvidenceResolver

    @impl true
    def resolve(_packet, _budget), do: raise("fixture resolver crash")
  end

  defmodule GuessingInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference

    @impl true
    def interpret(%{evidence: [evidence | _]}, _budget) do
      {:ok,
       [
         %{
           field_name: "publisher_domain",
           normalized_value: "invented.example",
           confidence: 0.99,
           evidence_refs: [evidence.id],
           resolver_provenance: [%{"inference" => "guessing"}],
           transform: "copy"
         }
       ], %{tokens: 1, time_ms: 1}}
    end
  end

  defmodule LegacyInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference

    @impl true
    def interpret(_packet, _budget), do: {:ok, []}
  end

  defmodule MissingTokenInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference
    @impl true
    def interpret(_packet, _budget), do: {:ok, [], %{time_ms: 1}}
  end

  defmodule NegativeTokenInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference
    @impl true
    def interpret(_packet, _budget), do: {:ok, [], %{tokens: -1, time_ms: 1}}
  end

  defmodule NonIntegerTokenInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference
    @impl true
    def interpret(_packet, _budget), do: {:ok, [], %{tokens: 1.5, time_ms: 1}}
  end

  defmodule WithinTokenInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference
    @impl true
    def interpret(_packet, _budget), do: {:ok, [], %{tokens: 100, time_ms: 1}}
  end

  defmodule OverTokenInference do
    @behaviour Primeradiant.Ingestion.Resolution.Inference
    @impl true
    def interpret(_packet, _budget), do: {:ok, [], %{tokens: 101, time_ms: 1}}
  end

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-resolution-pipeline-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "resolver persistence stamps privileged canonical provenance and selects same-domain canonical",
       %{db_path: db_path} do
    register(db_path, "canonical-trust", CanonicalResolver)
    receipt = receive(db_path, "canonical-trust", supplied_fields())

    assert {:outcome, "eligible", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    evidence =
      DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, resolution_case.id)
      |> Enum.find(&(&1.kind == "fetched_response_canonical_url"))

    assert evidence.source == "resolver_response"
    assert evidence.provenance["resolver"] == "canonical-trust-resolver"

    selected = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, resolution_case.id)
    assert Enum.any?(selected, &(&1.selected and &1.normalized_value == evidence.value))
  end

  test "malformed privileged resolver artifact is absent and raw URL wins", %{db_path: db_path} do
    register(db_path, "malformed-canonical", MalformedCanonicalResolver)
    receipt = receive(db_path, "malformed-canonical", supplied_fields())

    assert {:outcome, "eligible", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    selected = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, resolution_case.id)

    assert Enum.any?(selected, fn field ->
             field.selected and field.field_name == "public_url" and
               field.normalized_value == "https://publisher.example/article"
           end)
  end

  test "accepted-final resolver relation violation is a field conflict", %{db_path: db_path} do
    register(db_path, "accepted-final-violation", ViolatingAcceptedFinalResolver)
    receipt = receive(db_path, "accepted-final-violation", supplied_fields())

    assert {:outcome, "unresolved:field_conflict", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)
  end

  test "accepted-final resolver evidence selects the rank-two URL at exact confidence",
       %{db_path: db_path} do
    register(db_path, "accepted-final", AcceptedFinalResolver)
    receipt = receive(db_path, "accepted-final", supplied_fields())

    assert {:outcome, "eligible", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    evidence =
      DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, resolution_case.id)
      |> Enum.find(&(&1.kind == "fetched_response_accepted_final_url"))

    fields = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, resolution_case.id)
    selected_url = Enum.find(fields, &(&1.field_name == "public_url" and &1.selected))

    assert evidence.source == "resolver_response"
    assert evidence.value == evidence.locator["response_url"]
    assert selected_url.normalized_value == "https://publisher.example/final"
    assert Decimal.equal?(selected_url.confidence, Decimal.new("0.90"))
    assert selected_url.derivation_evidence_ref == evidence.id

    assert Enum.all?(fields, fn field ->
             field.field_name != "public_url" or field.id == selected_url.id or not field.selected
           end)

    assert resolution_case.state == "eligible"
    assert resolution_case.outcome_code == "eligible"
  end

  test "OG site-name resolver evidence selects the label on the selected URL domain",
       %{db_path: db_path} do
    register(db_path, "og-site-name", OgSiteNameResolver)
    receipt = receive(db_path, "og-site-name", supplied_fields())

    assert {:outcome, "eligible", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    evidence =
      DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, resolution_case.id)
      |> Enum.find(&(&1.kind == "fetched_response_og_site_name"))

    fields = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, resolution_case.id)
    selected_label = Enum.find(fields, &(&1.field_name == "publisher_label" and &1.selected))
    selected_url = Enum.find(fields, &(&1.field_name == "public_url" and &1.selected))

    assert evidence.source == "resolver_response"
    assert evidence.locator["response_url"] == selected_url.normalized_value
    assert selected_label.normalized_value == "Fetched Publisher"
    assert selected_label.derivation_evidence_ref == evidence.id

    assert Enum.all?(fields, fn field ->
             field.field_name != "publisher_label" or field.id == selected_label.id or
               not field.selected
           end)

    assert resolution_case.state == "eligible"
    assert resolution_case.outcome_code == "eligible"
  end

  test "resolver-authored source-declared non-public access is refused", %{db_path: db_path} do
    register(db_path, "resolver-non-public", ResolverNonPublicAccess,
      policy: %{
        "version" => "policy-v1",
        "source_class" => "source_record_no_public_url",
        "allow_no_public_url" => true
      }
    )

    receipt =
      receive(db_path, "resolver-non-public", %{
        "publisher_label" => "Fixture Publisher",
        "publisher_domain" => "Publisher.Example"
      })

    assert {:outcome, "refused:no_public_article", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    evidence =
      DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, resolution_case.id)
      |> Enum.find(&(&1.kind == "non_public_access"))

    fields = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, resolution_case.id)
    no_public_url = Enum.find(fields, &(&1.normalized_value == "no_public_url"))

    assert evidence.source == "resolver_response"
    assert evidence.value == "declared_non_public"
    assert evidence.locator["non_public_reason"] == "source_declared"
    assert no_public_url.derivation_evidence_ref == evidence.id
    refute no_public_url.selected
    refute Enum.any?(fields, & &1.selected)
    assert resolution_case.state == "refused"
    assert resolution_case.outcome_code == "refused:no_public_article"
  end

  test "SER-V4 contains malformed, slow, byte, redirect, and rate-limit failures while another source proceeds",
       %{db_path: db_path} do
    cases = [
      {"malformed", nil, %{"malformed" => true}, "quarantined:malformed_envelope"},
      {"slow", SlowResolver, supplied_fields(), "retry_scheduled:resolver_timeout"},
      {"bytes", ByteBudgetResolver, supplied_fields(), "failed_terminal:budget_exhausted"},
      {"redirects", RedirectBudgetResolver, supplied_fields(),
       "failed_terminal:budget_exhausted"},
      {"rate", RateLimitedResolver, supplied_fields(), "retry_scheduled:rate_limited"},
      {"crash", CrashingResolver, supplied_fields(), "failed_terminal:resolver_crash"}
    ]

    Enum.each(cases -- [Enum.at(cases, 1)], fn {source, resolver, fields, expected} ->
      register(db_path, source, resolver)
      receipt = receive(db_path, source, fields)

      assert {:outcome, ^expected, durable_case} =
               Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

      assert durable_case.outcome_code == expected
      assert DurableSoupDb.resolution_outcomes_for_case(db_path, @tenant, durable_case.id) != []

      attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, durable_case.id)
      assert Enum.all?(attempts, &(&1.ended_at >= &1.started_at))
      assert Enum.all?(attempts, &is_binary(&1.input_hash))
    end)

    Process.register(self(), :t1656_interleaving_test)

    on_exit(fn ->
      if Process.whereis(:t1656_interleaving_test),
        do: Process.unregister(:t1656_interleaving_test)
    end)

    {"slow", _resolver, slow_fields, _slow_expected} = Enum.at(cases, 1)
    register(db_path, "slow", BlockingResolver)
    slow = receive(db_path, "slow", slow_fields)
    parent = self()

    slow_pid =
      spawn(fn ->
        send(
          parent,
          {:slow_result, Case.run(db_path, slow.resolution_case_id, tenant_id: @tenant)}
        )
      end)

    assert is_pid(slow_pid)
    assert_receive {:blocking_resolver_entered, worker_pid}, 1_000
    worker_ref = Process.monitor(worker_pid)

    register(db_path, "healthy", HealthyResolver)
    healthy = receive(db_path, "healthy", supplied_fields())

    assert {:outcome, "eligible", validating} =
             Case.run(db_path, healthy.resolution_case_id, tenant_id: @tenant)

    assert validating.state == "eligible"
    assert validating.outcome_code == "eligible"

    Process.exit(slow_pid, :kill)
    assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, :killed}, 1_000

    assert {:outcome, "failed_terminal:interrupted_attempt", _slow_case} =
             Case.run(db_path, slow.resolution_case_id, tenant_id: @tenant)

    assert {:ok, health} =
             SourceRegistry.health(db_path, %{tenant_id: @tenant, source_key: "healthy"})

    assert health.circuit_state == "closed"
    assert health.circuit_states["healthy-resolver"] == "closed"
    assert health.resolution_backlog == 0

    assert {:ok, slow_health} =
             SourceRegistry.health(db_path, %{tenant_id: @tenant, source_key: "slow"})

    assert slow_health.circuit_states["slow-resolver"] == "resolution_stalled"
  end

  test "SER-V5 retry and replay keep attempt keys stable, bound attempts, and expose health depth",
       %{db_path: db_path} do
    register(db_path, "retrying", SlowResolver)
    receipt = receive(db_path, "retrying", supplied_fields())

    assert {:outcome, "retry_scheduled:resolver_timeout", scheduled} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    first_attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, scheduled.id)
    assert Enum.count(first_attempts, &(&1.stage == "resolver")) == 1

    assert {:ok, health} =
             SourceRegistry.health(db_path, %{tenant_id: @tenant, source_key: "retrying"})

    assert health.retry_depth == 1
    assert health.circuit_state == "degraded"
    assert health.circuit_states["retrying-resolver"] == "degraded"

    assert {:outcome, "retry_scheduled:resolver_timeout", replay} =
             Case.run(db_path, receipt.resolution_case_id,
               tenant_id: @tenant,
               at: DateTime.add(scheduled.next_retry_at, -1, :microsecond)
             )

    assert replay.id == scheduled.id

    assert DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, scheduled.id)
           |> Enum.map(& &1.attempt_key)
           |> Enum.sort() == Enum.map(first_attempts, & &1.attempt_key) |> Enum.sort()

    assert {:outcome, "failed_terminal:attempt_budget_exhausted", terminal} =
             Case.run(db_path, receipt.resolution_case_id,
               tenant_id: @tenant,
               at: scheduled.next_retry_at
             )

    attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, terminal.id)
    assert Enum.count(attempts, &(&1.stage == "resolver")) == 2
    assert attempts |> Enum.map(& &1.attempt_key) |> Enum.uniq() |> length() == length(attempts)

    assert {:outcome, "failed_terminal:attempt_budget_exhausted", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert DurableSoupDb.table_count(db_path, "resolution_attempts", @tenant) == length(attempts)

    assert {:ok, health} =
             SourceRegistry.health(db_path, %{tenant_id: @tenant, source_key: "retrying"})

    assert health.retry_depth == 0
    assert health.circuit_state == "resolution_stalled"
    assert health.last_resolution_terminal_at != nil
  end

  test "crash replay durably finalizes the interrupted attempt without duplicate rows", %{
    db_path: db_path
  } do
    Process.register(self(), :t1656_crash_replay_test)

    on_exit(fn ->
      if Process.whereis(:t1656_crash_replay_test),
        do: Process.unregister(:t1656_crash_replay_test)
    end)

    register(db_path, "crash-replay", CrashReplayResolver)
    receipt = receive(db_path, "crash-replay", supplied_fields())
    parent = self()

    runner =
      spawn(fn ->
        send(
          parent,
          {:unexpected_result, Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)}
        )
      end)

    assert_receive :resolver_entered, 1_000
    Process.exit(runner, :kill)

    assert {:outcome, "failed_terminal:interrupted_attempt", recovered} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    counts = resolution_counts(db_path)

    attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, recovered.id)
    assert Enum.count(attempts, &(&1.outcome == "failed_terminal:interrupted_attempt")) == 1
    refute Enum.any?(attempts, &(&1.outcome == "running"))

    assert {:outcome, "failed_terminal:interrupted_attempt", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert resolution_counts(db_path) == counts
    refute_receive {:unexpected_result, _}
  end

  test "crash replay synthesizes one interrupted attempt when the mid-stage attempt was finalized",
       %{
         db_path: db_path
       } do
    register(db_path, "finalized-window", nil)
    receipt = receive(db_path, "finalized-window", supplied_fields())

    assert {:outcome, "eligible", validating} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    validating
    |> ChangesetStore.update!(%{state: "resolving"})
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "failed_terminal:interrupted_attempt", recovered} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    counts = resolution_counts(db_path)
    attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, recovered.id)
    assert Enum.count(attempts, &(&1.outcome == "failed_terminal:interrupted_attempt")) == 1

    assert {:outcome, "failed_terminal:interrupted_attempt", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert resolution_counts(db_path) == counts
  end

  test "pipeline crash recovery finalizes a claimed attempt and is idempotent", %{
    db_path: db_path
  } do
    register(db_path, "pipeline-crash-window", nil)
    receipt = receive(db_path, "pipeline-crash-window", supplied_fields())

    assert {:outcome, "eligible", validating} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    [attempt | _] = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, validating.id)

    attempt
    |> ChangesetStore.update!(%{outcome: "running", error_class: nil})
    |> then(&DurableSoupDb.put_resolution_attempt!(db_path, &1))

    validating
    |> ChangesetStore.update!(%{state: "retry_scheduled", next_retry_at: nil})
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "failed_terminal:pipeline_crash", recovered} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert {:outcome, "failed_terminal:pipeline_crash", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, recovered.id)
    outcomes = DurableSoupDb.resolution_outcomes_for_case(db_path, @tenant, recovered.id)
    refute Enum.any?(attempts, &(&1.outcome == "running"))
    assert Enum.count(outcomes, &(&1.outcome_code == "failed_terminal:pipeline_crash")) == 1
  end

  test "receive envelope replay creates zero rows in every resolution table", %{db_path: db_path} do
    register(db_path, "receipt-replay", nil)
    receipt = receive(db_path, "receipt-replay", supplied_fields())

    assert {:outcome, "eligible", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    counts = resolution_counts(db_path)

    replay = receive(db_path, "receipt-replay", supplied_fields())
    assert replay.duplicate
    assert replay.resolution_case_id == receipt.resolution_case_id
    assert resolution_counts(db_path) == counts
  end

  test "normalization persists exact raw locators and deterministic direct candidates", %{
    db_path: db_path
  } do
    register(db_path, "normalized", nil)
    receipt = receive(db_path, "normalized", supplied_fields())

    assert {:outcome, "eligible", validating} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    evidence = DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, validating.id)
    fields = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, validating.id)

    assert Enum.any?(
             evidence,
             &(&1.locator == %{"attribute_path" => ["raw_fields", "publisher_label"]})
           )

    assert Enum.all?(evidence, &(&1.source == "raw_envelope"))
    assert Enum.all?(evidence, &(&1.digest == ChangesetStore.hash(&1.value)))
    assert Enum.all?(fields, &Decimal.equal?(&1.confidence, Decimal.new("1.0")))
    assert Enum.all?(fields, &(&1.evidence_refs != [] and &1.selected == true))

    attempt_count = DurableSoupDb.table_count(db_path, "resolution_attempts", @tenant)
    evidence_count = DurableSoupDb.table_count(db_path, "resolution_evidence", @tenant)

    assert {:outcome, "eligible", replay} = Case.run(db_path, validating.id, tenant_id: @tenant)
    assert replay.id == validating.id
    assert DurableSoupDb.table_count(db_path, "resolution_attempts", @tenant) == attempt_count
    assert DurableSoupDb.table_count(db_path, "resolution_evidence", @tenant) == evidence_count
  end

  test "planner uses only registration routes and evidence types and exposes registered budgets" do
    registration = %{
      config: %{
        "resolvers" => [
          %{
            "id" => "domain",
            "module" => HealthyResolver,
            "version" => "resolver-v2",
            "evidence_types" => ["publisher_domain"]
          },
          %{
            "id" => "url",
            "module" => SlowResolver,
            "version" => "resolver-v1",
            "evidence_types" => ["public_url"]
          }
        ]
      },
      budgets: %{
        "resolvers" => %{
          "domain" => %{
            "time_ms" => 9,
            "requests" => 1,
            "bytes" => 10,
            "redirects" => 0,
            "result_count" => 1
          }
        }
      }
    }

    assert [plan] = ResolverPlanner.plan(registration, ["publisher_domain"])
    assert plan.id == "domain"
    assert plan.module == HealthyResolver
    assert plan.budget.time_ms == 9
    assert plan.why == "registered evidence type: publisher_domain"
  end

  test "uncited or invented inference values are rejected by the pipeline", %{db_path: db_path} do
    register(db_path, "inference", nil, GuessingInference)
    receipt = receive(db_path, "inference", supplied_fields())

    assert {:outcome, "quarantined:provenance_validation_failed", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert resolution_case.state == "quarantined"

    evidence = DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, resolution_case.id)

    assert {:error, :invented_or_uncited_value} =
             Inference.validate_candidates(
               [
                 %{
                   field_name: "publisher_domain",
                   normalized_value: "invented.example",
                   confidence: 0.99,
                   evidence_refs: [hd(evidence).id],
                   resolver_provenance: [%{"inference" => "guessing"}],
                   transform: "copy"
                 }
               ],
               evidence
             )
  end

  test "inference explanation cites only field-kind evidence" do
    field_evidence = %{id: "field", kind: "publisher_label", value: "Fixture Publisher"}
    unrelated_evidence = %{id: "other", kind: "response", value: "body"}

    candidate = %{
      field_name: "explanation",
      normalized_value: "The publisher label is directly evidenced.",
      confidence: 0.8,
      evidence_refs: ["field"],
      resolver_provenance: [%{"inference" => "fixture"}],
      transform: "interpretation"
    }

    assert {:ok, [_]} = Inference.validate_candidates([candidate], [field_evidence])

    assert {:error, :invented_or_uncited_value} =
             Inference.validate_candidates(
               [%{candidate | evidence_refs: ["field", "other"]}],
               [field_evidence, unrelated_evidence]
             )
  end

  test "legacy two-element inference result is terminally invalid", %{db_path: db_path} do
    register(db_path, "legacy-inference", nil, LegacyInference)
    receipt = receive(db_path, "legacy-inference", supplied_fields())

    assert {:outcome, "failed_terminal:inference_invalid_result", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert resolution_case.state == "failed_terminal"
  end

  test "terminal replay restores a missing deterministic outcome only once", %{db_path: db_path} do
    register(db_path, "terminal-outcome-window", nil, LegacyInference)
    receipt = receive(db_path, "terminal-outcome-window", supplied_fields())

    assert {:outcome, "failed_terminal:inference_invalid_result", resolution_case} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    {_, 0} =
      System.cmd("sqlite3", [
        db_path,
        "DELETE FROM resolution_outcomes WHERE resolution_case_id = '#{resolution_case.id}';"
      ])

    assert {:outcome, "failed_terminal:inference_invalid_result", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    assert {:outcome, "failed_terminal:inference_invalid_result", _} =
             Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)

    outcomes = DurableSoupDb.resolution_outcomes_for_case(db_path, @tenant, resolution_case.id)
    attempts = DurableSoupDb.resolution_attempts_for_case(db_path, @tenant, resolution_case.id)
    assert length(outcomes) == 1
    refute Enum.any?(attempts, &(&1.outcome == "running"))
  end

  test "inference token consumption is required, typed, and budgeted", %{db_path: db_path} do
    for {source, inference} <- [
          {"missing-token", MissingTokenInference},
          {"negative-token", NegativeTokenInference},
          {"noninteger-token", NonIntegerTokenInference}
        ] do
      register(db_path, source, nil, inference)
      receipt = receive(db_path, source, supplied_fields())

      assert {:outcome, "failed_terminal:inference_invalid_result", _} =
               Case.run(db_path, receipt.resolution_case_id, tenant_id: @tenant)
    end

    register(db_path, "within-token", nil, WithinTokenInference)
    within = receive(db_path, "within-token", supplied_fields())

    assert {:outcome, "eligible", %{state: "eligible"}} =
             Case.run(db_path, within.resolution_case_id, tenant_id: @tenant)

    register(db_path, "over-token", nil, OverTokenInference)
    over = receive(db_path, "over-token", supplied_fields())

    assert {:outcome, "failed_terminal:budget_exhausted", _} =
             Case.run(db_path, over.resolution_case_id, tenant_id: @tenant)
  end

  defp register(db_path, source, resolver, inference_or_options \\ nil) do
    {inference, options} =
      if is_list(inference_or_options),
        do: {nil, inference_or_options},
        else: {inference_or_options, []}

    time_ms =
      case resolver do
        SlowResolver -> 5
        BlockingResolver -> 10_000
        _ -> 100
      end

    route =
      if resolver do
        [
          %{
            "id" => source <> "-resolver",
            "module" => resolver,
            "version" => "resolver-v1",
            "evidence_types" => ["public_url"]
          }
        ]
      else
        []
      end

    inference_config =
      if inference,
        do: %{"id" => "fixture-inference", "module" => inference, "version" => "inference-v1"}

    assert {:ok, _registration} =
             SourceRegistry.register_source(db_path, %{
               tenant_id: @tenant,
               source_key: source,
               adapter_module: FixtureAdapter,
               adapter_version: "adapter-v1",
               mode: "shadow",
               resolution_policy:
                 Keyword.get(options, :policy, %{
                   "version" => "policy-v1",
                   "source_class" => "public_article"
                 }),
               policy_version: "policy-v1",
               budgets: %{
                 "max_attempts" => 2,
                 "total_case_ms" => 10_000,
                 "retry_backoff_ms" => 1,
                 "per_source_concurrency" => 1,
                 "adapter" => %{"time_ms" => 100},
                 "normalizer" => %{"time_ms" => 100},
                 "inference" => %{"time_ms" => 100, "tokens" => 100},
                 "resolvers" => %{
                   (source <> "-resolver") => %{
                     "time_ms" => time_ms,
                     "requests" => 1,
                     "bytes" => 100,
                     "redirects" => 3,
                     "result_count" => 2
                   }
                 }
               },
               config: %{
                 "evidence_types" => ["public_url"],
                 "resolvers" => route,
                 "inference" => inference_config,
                 "admission_material" => %{
                   "source_type" => "news_article",
                   "acl" => %{"privacy" => "public", "participants" => []}
                 }
               },
               initial_cursor: 0
             })
  end

  defp receive(db_path, source, fields) do
    identity = "event-#{source}"

    assert {:ok, receipt} =
             SourceRegistry.receive_envelope(db_path, %{
               tenant_id: @tenant,
               source_key: source,
               source_event_external_id: identity,
               source_position: 1,
               received_at: @received_at,
               content_digest: ChangesetStore.hash(Jason.encode!(fields)),
               integrity_metadata: %{"algorithm" => "sha256"},
               retained_bytes: Jason.encode!(fields),
               visibility: "tenant",
               correlation_id: "correlation-#{source}"
             })

    receipt
  end

  defp supplied_fields do
    %{
      "publisher_label" => "Fixture Publisher",
      "publisher_domain" => "Publisher.Example",
      "public_url" => "https://publisher.example/article"
    }
  end

  defp resolution_counts(db_path) do
    ~w(raw_envelopes resolution_cases resolution_attempts resolution_evidence resolved_source_fields resolution_outcomes)
    |> Map.new(&{&1, DurableSoupDb.table_count(db_path, &1, @tenant)})
  end
end
