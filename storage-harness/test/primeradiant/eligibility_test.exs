defmodule Primeradiant.EligibilityTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Resolution.{Case, Eligibility, Inference}
  alias Primeradiant.Ingestion.SourceRegistry

  alias Primeradiant.StorageHarness.{
    ChangesetStore,
    DurableSoupDb,
    ResolutionEvidence,
    ResolvedSourceField
  }

  @tenant "20000000-0000-0000-0000-000000001657"
  @at ~U[2026-07-10 12:00:00.000000Z]

  defmodule EmptyAdapter do
    @behaviour Primeradiant.Ingestion.SourceAdapter

    def to_candidate(raw, _ctx) do
      {:ok,
       %{
         declared_identity: raw.source_event_external_id,
         raw_refs: [raw.id],
         visibility: raw.visibility,
         adapter_provenance: %{"adapter" => "empty-v1"}
       }}
    end
  end

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-eligibility-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{db_path: Path.join(root, "soup.sqlite3")}
  end

  test "SER-V10 public_article accepts every URL and label precedence rank", %{db_path: db_path} do
    for {name, url_kind, label_kind, url_locator, label_locator} <- [
          {"canonical", "fetched_response_canonical_url", "fetched_response_og_site_name",
           %{"response_url" => "https://www.publisher.example/redirected"},
           %{"response_url" => "https://publisher.example/article"}},
          {"final", "fetched_response_accepted_final_url", "fetched_response_og_site_name",
           %{"response_url" => "https://publisher.example/article"},
           %{"response_url" => "https://publisher.example/article"}},
          {"feed", "public_url", "publisher_label", %{}, %{}}
        ] do
      case_id = prepared_case(db_path, name, public_policy())

      material(db_path, case_id,
        url_kind: url_kind,
        label_kind: label_kind,
        url_locator: url_locator,
        label_locator: label_locator
      )

      assert {:outcome, "eligible", eligible} = Case.run(db_path, case_id, tenant_id: @tenant)
      assert eligible.config_policy_hash == registration(db_path, name).policy_hash
      assert eligible.state == "eligible"
      [outcome] = DurableSoupDb.resolution_outcomes_for_case(db_path, @tenant, eligible.id)
      assert outcome.admission_material_ref == nil

      selected = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, case_id)
      assert Enum.count(selected, & &1.selected) == 4
    end
  end

  test "SER-V10 confidence classes are exact and every lower confidence is insufficient",
       %{
         db_path: db_path
       } do
    for field <- ~w(publisher_label public_url) do
      at_id = prepared_case(db_path, "at-#{field}", public_policy())
      material(db_path, at_id)
      assert {:outcome, "eligible", _} = Case.run(db_path, at_id, tenant_id: @tenant)

      below_id = prepared_case(db_path, "below-#{field}", public_policy())
      material(db_path, below_id, confidences: %{field => assigned_confidence(field) - 0.01})

      assert {:outcome, "unresolved:insufficient_evidence", _} =
               Case.run(db_path, below_id, tenant_id: @tenant)
    end

    below_domain_id = prepared_case(db_path, "below-publisher_domain", public_policy())
    material(db_path, below_domain_id, confidences: %{"publisher_domain" => 0.89})
    assert {:outcome, "eligible", _} = Case.run(db_path, below_domain_id, tenant_id: @tenant)

    fallback_id = prepared_case(db_path, "below-explanation", public_policy())
    material(db_path, fallback_id, confidences: %{"explanation" => 0.79})
    assert {:outcome, "eligible", _} = Case.run(db_path, fallback_id, tenant_id: @tenant)
  end

  test "SER-V2 required fields reject absent provenance, non-normalized values, and absent evidence refs",
       %{db_path: db_path} do
    for field <- ~w(publisher_label publisher_domain public_url explanation) do
      no_provenance = base_field_attrs("case", field, "value", 1.0, ["evidence"])

      assert %{valid?: false} =
               ResolvedSourceField.changeset(%{no_provenance | resolver_provenance: []})

      assert %{valid?: false} =
               ResolvedSourceField.changeset(%{no_provenance | evidence_refs: []})
    end

    for {field, value} <- [
          {"publisher_label", " Publisher"},
          {"public_url", "ftp://publisher.example/article"}
        ] do
      case_id = prepared_case(db_path, "normalization-#{field}", public_policy())
      material(db_path, case_id, overrides: %{field => value})

      assert {:outcome, "unresolved:insufficient_evidence", _} =
               Case.run(db_path, case_id, tenant_id: @tenant)
    end

    invalid_domain_id = prepared_case(db_path, "normalization-publisher_domain", public_policy())

    material(db_path, invalid_domain_id,
      overrides: %{"publisher_domain" => "www.Publisher.Example"}
    )

    assert {:outcome, "eligible", _} = Case.run(db_path, invalid_domain_id, tenant_id: @tenant)

    explanation_id = prepared_case(db_path, "normalization-explanation", public_policy())
    material(db_path, explanation_id, overrides: %{"explanation" => " explanation "})
    assert {:outcome, "eligible", _} = Case.run(db_path, explanation_id, tenant_id: @tenant)
  end

  test "SER-V10 same-rank, cross-domain canonical, and supplied-domain mismatch are conflicts", %{
    db_path: db_path
  } do
    same_rank = prepared_case(db_path, "same-rank", public_policy())
    material(db_path, same_rank)
    add_url(db_path, same_rank, "public_url", "https://publisher.example/other", %{})
    assert conflict(db_path, same_rank)

    cross = prepared_case(db_path, "cross", public_policy())

    material(db_path, cross,
      url_kind: "fetched_response_canonical_url",
      url_locator: %{"response_url" => "https://other.example/redirected"}
    )

    assert conflict(db_path, cross)

    mismatch = prepared_case(db_path, "mismatch", public_policy())
    material(db_path, mismatch, overrides: %{"publisher_domain" => "other.example"})
    assert conflict(db_path, mismatch)
  end

  test "SER-V10 valid higher rank ignores lower-rank disagreement", %{db_path: db_path} do
    case_id = prepared_case(db_path, "lower-rank", public_policy())

    material(db_path, case_id,
      url_kind: "fetched_response_canonical_url",
      url_locator: %{"response_url" => "https://publisher.example/final"}
    )

    add_url(db_path, case_id, "public_url", "https://lower.example/item", %{})
    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)
  end

  test "SER-V10 explicit no_public_url requires the exact class, flag, and non-public evidence",
       %{
         db_path: db_path
       } do
    accepted = prepared_case(db_path, "no-url-ok", no_url_policy(true))
    no_url_material(db_path, accepted)
    assert {:outcome, "eligible", _} = Case.run(db_path, accepted, tenant_id: @tenant)

    missing_flag = prepared_case(db_path, "no-url-flag", no_url_policy(false))
    no_url_material(db_path, missing_flag)

    assert {:outcome, "unresolved:policy_not_registered", _} =
             Case.run(db_path, missing_flag, tenant_id: @tenant)

    inferred_absence = prepared_case(db_path, "no-url-absence", no_url_policy(true))
    material(db_path, inferred_absence)

    assert {:outcome, "refused:no_public_article", _} =
             Case.run(db_path, inferred_absence, tenant_id: @tenant)

    wrong_class =
      prepared_case(
        db_path,
        "no-url-wrong-class",
        Map.put(public_policy(), "allow_no_public_url", true)
      )

    no_url_material(db_path, wrong_class)

    assert {:outcome, "unresolved:insufficient_evidence", _} =
             Case.run(db_path, wrong_class, tenant_id: @tenant)
  end

  test "SER-V10 omitted or unregistered policy is never eligible", %{db_path: db_path} do
    for {name, policy} <- [{"omitted", %{}}, {"unknown", %{"source_class" => "future_class"}}] do
      case_id = prepared_case(db_path, name, policy)
      material(db_path, case_id)

      assert {:outcome, "unresolved:policy_not_registered", _} =
               Case.run(db_path, case_id, tenant_id: @tenant)
    end
  end

  test "SER-V3 inference identity cannot outrank deterministic evidence; cited interpretation can explain",
       %{db_path: db_path} do
    case_id = prepared_case(db_path, "inference", public_policy())
    refs = material(db_path, case_id)
    evidence = DurableSoupDb.resolution_evidence_for_case(db_path, @tenant, case_id)

    guessed = %{
      field_name: "publisher_domain",
      normalized_value: "guessed.example",
      confidence: 1.0,
      evidence_refs: [refs.domain],
      resolver_provenance: [%{"inference" => "fixture"}],
      transform: "copy"
    }

    assert {:error, :invented_or_uncited_value} =
             Inference.validate_candidates([guessed], evidence)

    copied = %{guessed | normalized_value: "publisher.example"}
    assert {:ok, [candidate]} = Inference.validate_candidates([copied], evidence)
    insert_field(db_path, case_id, candidate)

    explanation = %{
      field_name: "explanation",
      normalized_value: "The selected identity is supported by the cited evidence.",
      confidence: 0.8,
      evidence_refs: Enum.sort([refs.label, refs.domain, refs.url]),
      resolver_provenance: [%{"inference" => "fixture"}],
      transform: "interpretation"
    }

    assert {:ok, [candidate]} = Inference.validate_candidates([explanation], evidence)
    insert_field(db_path, case_id, candidate)

    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)

    selected =
      DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, case_id)
      |> Enum.filter(& &1.selected)

    assert Enum.find(selected, &(&1.field_name == "publisher_domain")).normalized_value ==
             "publisher.example"

    assert Enum.find(selected, &(&1.field_name == "explanation")).evidence_refs |> Enum.sort() ==
             explanation.evidence_refs
  end

  test "registrable-domain transform lowercases and strips only leading www." do
    assert Eligibility.registrable_domain("https://WWW.Example.COM/path") == "example.com"
    assert Eligibility.registrable_domain("sub.example.com") == "sub.example.com"
  end

  test "invalid higher-rank URL is absent and a valid lower rank remains eligible", %{
    db_path: db_path
  } do
    case_id = prepared_case(db_path, "invalid-higher-rank", public_policy())
    material(db_path, case_id)

    ref =
      add_evidence(
        db_path,
        case_id,
        "fetched_response_canonical_url",
        "https://publisher.example/canonical",
        %{"response_url" => "https://publisher.example/final"}
      )

    insert_field(
      db_path,
      case_id,
      field("public_url", "https://publisher.example/canonical", 0.89, [ref])
    )

    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)
  end

  test "derivation evidence must be cited and privileged provenance must name its resolver", %{
    db_path: db_path
  } do
    laundering = prepared_case(db_path, "unrelated-ref-laundering", public_policy())

    refs =
      material(db_path, laundering,
        url_kind: "fetched_response_canonical_url",
        url_locator: %{"response_url" => "https://publisher.example/final"}
      )

    url_field =
      DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, laundering)
      |> Enum.find(&(&1.field_name == "public_url"))

    url_field
    |> ChangesetStore.update!(%{derivation_evidence_ref: refs.label})
    |> then(&DurableSoupDb.put_resolved_source_field!(db_path, &1))

    assert {:outcome, "unresolved:insufficient_evidence", _} =
             Case.run(db_path, laundering, tenant_id: @tenant)

    adapter_only = prepared_case(db_path, "adapter-only-privileged", public_policy())

    material(db_path, adapter_only,
      url_kind: "fetched_response_canonical_url",
      url_locator: %{"response_url" => "https://publisher.example/final"},
      url_provenance: %{"adapter" => "fixture"}
    )

    assert {:outcome, "unresolved:insufficient_evidence", _} =
             Case.run(db_path, adapter_only, tenant_id: @tenant)
  end

  test "wrong exact confidence classes are absent", %{db_path: db_path} do
    canonical = prepared_case(db_path, "wrong-canonical-confidence", public_policy())

    material(db_path, canonical,
      url_kind: "fetched_response_canonical_url",
      url_locator: %{"response_url" => "https://publisher.example/final"},
      confidences: %{"public_url" => 1.0}
    )

    assert {:outcome, "unresolved:insufficient_evidence", _} =
             Case.run(db_path, canonical, tenant_id: @tenant)

    domain = prepared_case(db_path, "wrong-domain-confidence", public_policy())
    material(db_path, domain, confidences: %{"publisher_domain" => 0.9})
    assert {:outcome, "eligible", _} = Case.run(db_path, domain, tenant_id: @tenant)

    selected = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, domain)

    assert Enum.find(selected, &(&1.field_name == "publisher_domain" and &1.selected)).transform ==
             "registrable_domain"
  end

  test "same-rank label conflict and feed-label domain failure are terminal", %{db_path: db_path} do
    conflict_case = prepared_case(db_path, "same-rank-label", public_policy())
    material(db_path, conflict_case)
    ref = add_evidence(db_path, conflict_case, "publisher_label", "Other Publisher", %{})
    insert_field(db_path, conflict_case, field("publisher_label", "Other Publisher", 1.0, [ref]))
    assert conflict(db_path, conflict_case)

    domain_failure = prepared_case(db_path, "feed-label-domain-failure", public_policy())
    material(db_path, domain_failure, domain_provenance: %{"adapter" => "different-scope"})

    assert {:outcome, "unresolved:field_conflict", _} =
             Case.run(db_path, domain_failure, tenant_id: @tenant)
  end

  test "unmarked interpretation is absent and deterministic explanation is selected", %{
    db_path: db_path
  } do
    case_id = prepared_case(db_path, "unmarked-interpretation", public_policy())
    refs = material(db_path, case_id, confidences: %{"explanation" => 0.79})

    insert_field(
      db_path,
      case_id,
      field(
        "explanation",
        "Unmarked interpretation",
        0.8,
        Enum.sort([refs.label, refs.domain, refs.url]),
        "interpretation"
      )
      |> Map.put(:resolver_provenance, [%{"inference" => "fixture"}])
    )

    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)

    selected = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, case_id)

    assert Enum.find(selected, &(&1.field_name == "explanation" and &1.selected)).transform ==
             "deterministic_template"
  end

  test "case policy snapshot hash mismatch is durably policy_not_registered", %{db_path: db_path} do
    case_id = prepared_case(db_path, "snapshot-mismatch", public_policy())
    material(db_path, case_id)
    resolution_case = DurableSoupDb.resolution_case(db_path, @tenant, case_id)

    resolution_case
    |> ChangesetStore.update!(%{
      policy_snapshot: Map.put(resolution_case.policy_snapshot, "policy_hash", "stale")
    })
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "unresolved:policy_not_registered", _} =
             Case.run(db_path, case_id, tenant_id: @tenant)

    assert Enum.any?(
             DurableSoupDb.resolution_outcomes_for_case(db_path, @tenant, case_id),
             &(&1.outcome_code == "unresolved:policy_not_registered")
           )
  end

  test "snapshot content tampering is durably policy_not_registered", %{db_path: db_path} do
    case_id = prepared_case(db_path, "snapshot-content-tamper", public_policy())
    material(db_path, case_id)
    resolution_case = DurableSoupDb.resolution_case(db_path, @tenant, case_id)

    resolution_case
    |> ChangesetStore.update!(%{
      policy_snapshot:
        Map.put(resolution_case.policy_snapshot, "resolution_policy", %{
          "version" => "sr01-v1",
          "source_class" => "source_record_no_public_url"
        })
    })
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "unresolved:policy_not_registered", _} =
             Case.run(db_path, case_id, tenant_id: @tenant)
  end

  test "valid snapshot survives current registration mutation", %{db_path: db_path} do
    source = "snapshot-current-mutation"
    case_id = prepared_case(db_path, source, public_policy())
    material(db_path, case_id)
    register_policy(db_path, source, no_url_policy(true))

    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)
  end

  test "guarded legacy no-snapshot fallback uses matching current policy", %{db_path: db_path} do
    case_id = prepared_case(db_path, "legacy-no-snapshot", public_policy())
    material(db_path, case_id)
    resolution_case = DurableSoupDb.resolution_case(db_path, @tenant, case_id)

    resolution_case
    |> ChangesetStore.update!(%{policy_snapshot: nil})
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "eligible", _} = Case.run(db_path, case_id, tenant_id: @tenant)
  end

  test "first terminal return clears stale selected flags on non-eligible validation", %{
    db_path: db_path
  } do
    case_id = prepared_case(db_path, "stale-selection", public_policy())
    material(db_path, case_id)
    resolution_case = DurableSoupDb.resolution_case(db_path, @tenant, case_id)
    [field | _] = DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, case_id)

    field
    |> ChangesetStore.update!(%{selected: true})
    |> then(&DurableSoupDb.put_resolved_source_field!(db_path, &1))

    resolution_case
    |> ChangesetStore.update!(%{policy_snapshot: %{}})
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    assert {:outcome, "unresolved:policy_not_registered", _} =
             Case.run(db_path, case_id, tenant_id: @tenant)

    refute Enum.any?(
             DurableSoupDb.resolved_source_fields_for_case(db_path, @tenant, case_id),
             & &1.selected
           )
  end

  defp prepared_case(db_path, source, policy) do
    register_policy(db_path, source, policy)

    assert {:ok, receipt} =
             SourceRegistry.receive_envelope(db_path, %{
               tenant_id: @tenant,
               source_key: source,
               source_event_external_id: "event-#{source}",
               source_position: 1,
               received_at: @at,
               content_digest: ChangesetStore.hash(source),
               retained_bytes: "{}",
               visibility: "tenant",
               correlation_id: "correlation-#{source}"
             })

    resolution_case = DurableSoupDb.resolution_case(db_path, @tenant, receipt.resolution_case_id)

    resolution_case
    |> ChangesetStore.update!(%{state: "validating", attempt_count: 1})
    |> then(&DurableSoupDb.put_resolution_case!(db_path, &1))

    receipt.resolution_case_id
  end

  defp register_policy(db_path, source, policy) do
    assert {:ok, _} =
             SourceRegistry.register_source(db_path, %{
               tenant_id: @tenant,
               source_key: source,
               adapter_module: EmptyAdapter,
               adapter_version: "adapter-v1",
               mode: "shadow",
               resolution_policy: policy,
               policy_version: "sr01-v1",
               budgets: budgets(),
               config: %{"resolvers" => []},
               initial_cursor: 0
             })
  end

  defp material(db_path, case_id, opts \\ []) do
    url_kind = Keyword.get(opts, :url_kind, "public_url")
    label_kind = Keyword.get(opts, :label_kind, "publisher_label")
    overrides = Keyword.get(opts, :overrides, %{})
    confidences = Keyword.get(opts, :confidences, %{})
    url = Map.get(overrides, "public_url", "https://publisher.example/article")
    domain = Map.get(overrides, "publisher_domain", "publisher.example")
    label = Map.get(overrides, "publisher_label", "Publisher")

    url_ref =
      add_evidence(
        db_path,
        case_id,
        url_kind,
        url,
        Keyword.get(opts, :url_locator, %{}),
        Keyword.get(opts, :url_provenance)
      )

    domain_ref =
      add_evidence(
        db_path,
        case_id,
        "publisher_domain",
        domain,
        %{},
        Keyword.get(opts, :domain_provenance)
      )

    label_ref =
      add_evidence(db_path, case_id, label_kind, label, Keyword.get(opts, :label_locator, %{}))

    insert_field(
      db_path,
      case_id,
      field(
        "public_url",
        url,
        Map.get(
          confidences,
          "public_url",
          if(String.starts_with?(url_kind, "fetched_"), do: 0.9, else: 1.0)
        ),
        [url_ref]
      )
      |> Map.put(:resolver_provenance, provenance_for_kind(url_kind))
    )

    insert_field(
      db_path,
      case_id,
      field("publisher_domain", domain, Map.get(confidences, "publisher_domain", 1.0), [
        domain_ref
      ])
    )

    insert_field(
      db_path,
      case_id,
      field(
        "publisher_label",
        label,
        Map.get(
          confidences,
          "publisher_label",
          if(String.starts_with?(label_kind, "fetched_"), do: 1.0, else: 1.0)
        ),
        [label_ref]
      )
      |> Map.put(:resolver_provenance, provenance_for_kind(label_kind))
    )

    explanation = Map.get(overrides, "explanation", "Evidence explains the selected identity.")

    insert_field(
      db_path,
      case_id,
      field(
        "explanation",
        explanation,
        Map.get(confidences, "explanation", 0.8),
        [label_ref, domain_ref, url_ref],
        "interpretation"
      )
      |> Map.put(:resolver_provenance, [
        %{"interpretation" => true},
        %{"adapter" => "fixture"},
        %{"resolver" => "fixture-resolver"}
      ])
    )

    %{url: url_ref, domain: domain_ref, label: label_ref}
  end

  defp no_url_material(db_path, case_id) do
    no_url_ref =
      add_evidence(db_path, case_id, "non_public_access", "declared_non_public", %{
        "non_public_reason" => "source_declared"
      })

    domain_ref = add_evidence(db_path, case_id, "publisher_domain", "publisher.example", %{})
    label_ref = add_evidence(db_path, case_id, "publisher_label", "Publisher", %{})

    insert_field(
      db_path,
      case_id,
      field("publisher_domain", "publisher.example", 1.0, [domain_ref])
    )

    insert_field(db_path, case_id, field("publisher_label", "Publisher", 1.0, [label_ref]))

    insert_field(
      db_path,
      case_id,
      field("public_url", "no_public_url", 1.0, [no_url_ref], "explicit_no_public_url")
    )

    insert_field(
      db_path,
      case_id,
      field(
        "explanation",
        "Evidence explains non-public access.",
        0.8,
        [label_ref, domain_ref, no_url_ref],
        "interpretation"
      )
    )

    %{label: label_ref, domain: domain_ref, url: no_url_ref}
  end

  defp add_url(db_path, case_id, kind, url, locator) do
    ref = add_evidence(db_path, case_id, kind, url, locator)
    confidence = if String.starts_with?(kind, "fetched_"), do: 0.9, else: 1.0
    insert_field(db_path, case_id, field("public_url", url, confidence, [ref]))
  end

  defp add_evidence(db_path, case_id, kind, value, locator, provenance \\ nil) do
    id = ChangesetStore.hash(Enum.join([case_id, kind, value, Jason.encode!(locator)], "|"))

    ChangesetStore.insert!(ResolutionEvidence, %{
      id: id,
      tenant_id: @tenant,
      resolution_case_id: case_id,
      kind: kind,
      value: value,
      source:
        if(String.starts_with?(kind, "fetched_"), do: "resolver_response", else: "raw_envelope"),
      locator: locator,
      digest: ChangesetStore.hash(value),
      retrieved_at: @at,
      visibility: "tenant",
      provenance:
        provenance ||
          if(String.starts_with?(kind, "fetched_"),
            do: %{"resolver" => "fixture-resolver", "resolver_version" => "resolver-v1"},
            else: %{"adapter" => "fixture"}
          ),
      transformation_chain: []
    })
    |> then(&DurableSoupDb.insert_resolution_evidence!(db_path, &1))

    id
  end

  defp insert_field(db_path, case_id, attrs) do
    attrs =
      Map.drop(attrs, [:__meta__, :inserted_at, :updated_at, :id, :tenant_id, :resolution_case_id])

    ChangesetStore.insert!(
      ResolvedSourceField,
      Map.merge(attrs, %{
        id:
          ChangesetStore.hash(
            Enum.join(
              [
                case_id,
                attrs.field_name,
                attrs.normalized_value,
                inspect(attrs.evidence_refs),
                inspect(attrs.resolver_provenance)
              ],
              "|"
            )
          ),
        tenant_id: @tenant,
        resolution_case_id: case_id
      })
    )
    |> then(&DurableSoupDb.put_resolved_source_field!(db_path, &1))
  end

  defp field(name, value, confidence, refs, transform \\ "copy"),
    do: base_field_attrs("case", name, value, confidence, refs) |> Map.put(:transform, transform)

  defp provenance_for_kind(kind) do
    if String.starts_with?(kind, "fetched_"),
      do: [%{"resolver" => "fixture-resolver"}],
      else: [%{"adapter" => "fixture"}]
  end

  defp base_field_attrs(case_id, name, value, confidence, refs),
    do: %{
      tenant_id: @tenant,
      resolution_case_id: case_id,
      field_name: name,
      normalized_value: value,
      confidence: confidence,
      evidence_refs: refs,
      derivation_evidence_ref: hd(refs),
      resolver_provenance:
        if(name == "explanation",
          do: [%{"interpretation" => true}, %{"adapter" => "fixture"}],
          else: [%{"adapter" => "fixture"}]
        ),
      transform: "copy",
      contradiction_status: "none",
      selected: false
    }

  defp conflict(db_path, case_id) do
    match?(
      {:outcome, "unresolved:field_conflict", _},
      Case.run(db_path, case_id, tenant_id: @tenant)
    )
  end

  defp registration(db_path, source),
    do: DurableSoupDb.source_registration(db_path, @tenant, source)

  defp public_policy, do: %{"version" => "sr01-v1", "source_class" => "public_article"}

  defp no_url_policy(flag),
    do: %{
      "version" => "sr01-v1",
      "source_class" => "source_record_no_public_url",
      "allow_no_public_url" => flag
    }

  defp assigned_confidence("explanation"), do: 0.8
  defp assigned_confidence(_), do: 1.0

  defp budgets do
    %{
      "max_attempts" => 2,
      "total_case_ms" => 10_000,
      "retry_backoff_ms" => 1,
      "per_source_concurrency" => 1,
      "adapter" => %{"time_ms" => 100},
      "normalizer" => %{"time_ms" => 100},
      "resolvers" => %{}
    }
  end
end
