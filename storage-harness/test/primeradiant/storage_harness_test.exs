defmodule Primeradiant.StorageHarnessTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Projections.StoryClassifier
  alias Primeradiant.Proposals.Builder
  alias Primeradiant.Soup.Store

  alias Primeradiant.StorageHarness.{
    AuthoredOutputUnit,
    Edge,
    EvidenceRef,
    FixtureImporter,
    ProposalOp
  }

  @fixture_path Path.expand("../../../proof-harness/priv/fixtures/primeradiant_golden", __DIR__)

  setup_all do
    {:ok, state, report} = FixtureImporter.import_fixture_corpus(@fixture_path)
    {:ok, state: state, report: report}
  end

  test "golden corpus artifacts are importable without seeding expected graph truth", %{
    state: state,
    report: report
  } do
    assert length(report.expected_artifacts) == 9
    assert length(state.inputs) == 13
    assert length(state.watches) == 2
    assert report.persistence_mode == :changeset_validated
    refute report.db_backed?
    assert report.source_behavior == :proof_harness_semantics
    assert Enum.all?(state.proposals, &(&1.status == "accepted"))
  end

  test "proof-gate classifications persist on proposals", %{state: state} do
    by_fixture = Map.new(state.proposals, &{&1.fixture_id, &1.classification})

    assert by_fixture["public_story_001_initial_report"] == "new_story"
    assert by_fixture["public_story_002_followup_update"] == "substantive_update"
    assert by_fixture["public_story_003_duplicate_reinforcement"] == "repeated_noop_input"
    assert by_fixture["public_story_004_correction_or_contradiction"] == "conflict_correction"
    assert by_fixture["public_story_005_framing_spin"] == "color_spin_without_structural_change"
    assert by_fixture["public_story_006_near_duplicate_overlap"] == "repeated_noop_input"
    assert by_fixture["public_story_007_adjacent_distinct_terminal_walkout"] == "new_story"
    assert by_fixture["private_thread_001_request"] == "new_story"
    assert by_fixture["private_thread_002_reply_attach"] == "attach_to_existing_story"
    assert by_fixture["private_thread_003_unresolved_question"] == "stale_background_state"
    assert by_fixture["private_thread_004_later_update"] == "substantive_update"
    assert by_fixture["private_thread_005_low_global_rank_personal_watch"] == "new_story"
    assert by_fixture["stale_case_001_related_no_material_change"] == "stale_background_state"
  end

  test "normalization persistence records structural, background, question, color, tokens, and hashes",
       %{state: state} do
    roof_wait =
      Enum.find(state.inputs, &(&1.fixture_id == "private_thread_003_unresolved_question"))

    public_update =
      Enum.find(state.inputs, &(&1.fixture_id == "public_story_002_followup_update"))

    assert roof_wait.background == %{"status" => "waiting_for_board_packet"}
    assert roof_wait.questions == %{"blocker" => "hoa_approval"}
    assert public_update.facts["negotiations"] == "scheduled_17_00"
    assert "city says tourists should use the tunnel" in public_update.colors
    assert is_binary(public_update.content_sha256)
    assert "harbor" in public_update.topic_tokens
  end

  test "fixture import rejects trusted directive labels and keeps expected artifacts as assertions",
       %{state: state} do
    refute Enum.any?(state.raw_inputs, fn input ->
             String.match?(input["body_text"], ~r/(^|\n)(FACT|COLOR|BACKGROUND|QUESTION)\s/)
           end)
  end

  test "proposal-backed graph mutations require accepted decisions, commits, evidence, confidence, and concrete edges",
       %{state: state} do
    node_ids = MapSet.new(Enum.map(state.soup_nodes, & &1.id))
    proposal_ids = MapSet.new(Enum.map(state.proposals, & &1.id))
    op_ids = MapSet.new(Enum.map(state.proposal_ops, & &1.id))
    commit_ids = MapSet.new(Enum.map(state.graph_commits, & &1.id))

    assert MapSet.new(Enum.map(state.soup_nodes, & &1.node_type)) ==
             MapSet.new(~w(input story claim entity user_watch authored_output))

    assert length(state.proposal_decisions) == length(state.proposals)
    assert Enum.all?(state.proposal_decisions, &(&1.from_status == "pending"))

    assert Enum.any?(
             state.audit_events,
             &(&1.event == :proposal_submitted and &1.status == "pending")
           )

    assert Enum.any?(
             state.audit_events,
             &(&1.event == :proposal_decided and &1.to_status == "accepted")
           )

    assert Enum.any?(state.audit_events, &(&1.event == :graph_commit_created))

    assert Enum.all?(
             state.proposal_ops,
             &(&1.evidence_refs != [] and Decimal.compare(&1.confidence, 0) != :lt)
           )

    assert Enum.all?(
             state.graph_commits,
             &(&1.evidence_refs != [] and MapSet.member?(op_ids, &1.proposal_op_id))
           )

    for row <-
          state.soup_nodes ++
            state.edges ++ state.story_fact_versions ++ state.conflicts ++ state.story_events do
      assert MapSet.member?(proposal_ids, row.proposal_id)
      assert MapSet.member?(op_ids, row.proposal_op_id)
      assert MapSet.member?(commit_ids, row.graph_commit_id)
    end

    assert Enum.all?(
             state.edges,
             &(&1.edge_type in ~w(supports updates duplicates contradicts adds_color part_of watch_applies_to))
           )

    refute Enum.any?(state.edges, &(&1.edge_type == "related"))

    assert Enum.all?(
             state.edges,
             &(MapSet.member?(node_ids, &1.from_node_id) and
                 MapSet.member?(node_ids, &1.to_node_id))
           )

    assert state.evidence_refs != []
  end

  test "arbitration and changesets reject missing evidence, already-decided proposals, generic related edges, and ACL misses",
       %{state: state} do
    [raw | _] = state.raw_inputs
    normalized = Normalizer.normalize(raw)
    proposal = Builder.build(normalized, StoryClassifier.decide(Store.new(), normalized))

    assert_raise ArgumentError, "proposal requires evidence refs", fn ->
      FixtureImporter.validate_proposal_for_commit!(state, %{
        proposal
        | evidence_refs: [],
          ops: []
      })
    end

    assert_raise ArgumentError, "proposal must be pending before arbitration", fn ->
      FixtureImporter.validate_proposal_for_commit!(state, %{proposal | status: :accepted})
    end

    related_op = proposal.ops |> hd() |> Map.put(:edge_type, :related)

    assert_raise ArgumentError, "unsupported edge type", fn ->
      FixtureImporter.validate_proposal_for_commit!(state, %{proposal | ops: [related_op]})
    end

    private_raw = Enum.find(state.raw_inputs, &(&1["fixture_id"] == "private_thread_001_request"))
    private_normalized = Normalizer.normalize(private_raw)

    private_proposal =
      Builder.build(private_normalized, StoryClassifier.decide(Store.new(), private_normalized))

    assert_raise ArgumentError, "proposal evidence is not accessible to actor", fn ->
      FixtureImporter.validate_proposal_for_commit!(state, %{
        private_proposal
        | actor_id: "not-flynn"
      })
    end

    refute ProposalOp.changeset(%ProposalOp{}, %{
             tenant_id: state.tenant_id,
             proposal_id: Ecto.UUID.generate(),
             position: 0,
             op_type: "attach_input",
             payload: %{},
             evidence_refs: [],
             confidence: Decimal.new("0.9"),
             status: "pending"
           }).valid?

    refute Edge.changeset(%Edge{}, %{
             tenant_id: state.tenant_id,
             from_node_id: Ecto.UUID.generate(),
             to_node_id: Ecto.UUID.generate(),
             edge_type: "related",
             status: "committed",
             confidence: Decimal.new("0.9"),
             proposal_id: Ecto.UUID.generate(),
             proposal_op_id: Ecto.UUID.generate(),
             graph_commit_id: Ecto.UUID.generate(),
             attrs: %{}
           }).valid?

    hostile_private_create = %{
      proposal
      | actor_id: "stranger",
        evidence_refs: ["private_thread_001_request"],
        ops: [
          %{
            op: :create_input,
            input_id: "private_thread_001_request",
            title: "private",
            source_type: "fixture",
            observed_at: "2026-05-01T00:00:00-07:00",
            acl: %{"privacy" => "private", "participants" => ["flynn"]},
            evidence_refs: ["private_thread_001_request"],
            confidence: 0.9
          }
        ]
    }

    assert_raise ArgumentError, "proposal evidence is not accessible to actor", fn ->
      FixtureImporter.validate_proposal_for_commit!(state, hostile_private_create)
    end
  end

  test "near-collision identity persists without destructive merge", %{state: state} do
    decisions = Map.new(state.decisions, &{&1.fixture_id, &1})

    assert decisions["public_story_006_near_duplicate_overlap"].story_key ==
             decisions["public_story_001_initial_report"].story_key

    refute decisions["public_story_007_adjacent_distinct_terminal_walkout"].story_key ==
             decisions["public_story_001_initial_report"].story_key

    assert Enum.any?(state.edges, fn edge ->
             edge.edge_type == "duplicates" and
               node_key(state, edge.from_node_id) == "public_story_006_near_duplicate_overlap"
           end)

    assert Enum.any?(state.edges, &(&1.edge_type == "part_of"))

    assert Enum.any?(
             state.story_fact_versions,
             &(&1.fact_key == "labor_action" and &1.fact_value == "cafe_walkout")
           )

    assert Enum.any?(
             state.conflicts,
             &(&1.fact_key == "crossings" and &1.incoming_value == "normal")
           )
  end

  test "grounded watch-aware authoring persists verified units and seen refs", %{
    state: state,
    report: report
  } do
    assert report.first_briefing.verified
    assert Enum.any?(report.first_briefing.bullets, &String.starts_with?(&1, "[watch]"))
    assert report.first_briefing.text =~ "Basement humidity sensor battery reminder"

    assert report.second_briefing.text =~
             "changed for Flynn since last seen because structural facts moved"

    assert report.second_briefing.text =~ "quoted_amount=14800"
    refute report.second_briefing.text =~ "deadline=friday"

    assert Enum.all?(state.authored_outputs, & &1.verified)

    assert Enum.all?(
             state.authored_output_units,
             &(&1.evidence_refs != [] and &1.claim_refs != [])
           )

    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "input"))
    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "claim"))
    assert Enum.any?(state.seen_state_refs, &(&1.ref_kind == "authored_output"))
  end

  test "authoring cannot satisfy seen-state with ungrounded units", %{state: state} do
    refute AuthoredOutputUnit.changeset(%AuthoredOutputUnit{}, %{
             tenant_id: state.tenant_id,
             authored_output_id: Ecto.UUID.generate(),
             position: 0,
             unit_type: "bullet",
             content: "ungrounded",
             evidence_refs: [],
             claim_refs: []
           }).valid?

    assert state.seen_states != []

    unverified = %{
      verified: false,
      user_id: "flynn",
      evidence_refs: ["private_thread_001_request"],
      sentence_evidence: [
        %{
          evidence_refs: ["private_thread_001_request"],
          claim_refs: ["claim:roof-repair-quote:project"]
        }
      ]
    }

    assert_raise ArgumentError, "cannot mark seen for unverified output", fn ->
      FixtureImporter.validate_authored_output_for_seen!(state, unverified)
    end

    inaccessible = %{
      verified: true,
      user_id: "stranger",
      evidence_refs: ["private_thread_001_request"],
      sentence_evidence: [
        %{
          evidence_refs: ["private_thread_001_request"],
          claim_refs: ["claim:roof-repair-quote:project"]
        }
      ]
    }

    assert_raise ArgumentError, "authored output evidence is not accessible to actor", fn ->
      FixtureImporter.validate_authored_output_for_seen!(state, inaccessible)
    end

    mixed_visibility = %{
      verified: true,
      user_id: "stranger",
      evidence_refs: ["private_thread_001_request"],
      sentence_evidence: [
        %{
          evidence_refs: ["public_story_001_initial_report"],
          claim_refs: ["claim:harbor-ferry-strike:crossings"]
        }
      ]
    }

    assert_raise ArgumentError, "authored output evidence is not accessible to actor", fn ->
      FixtureImporter.validate_authored_output_for_seen!(state, mixed_visibility)
    end
  end

  test "stale background state remains present instead of deleting story rows", %{state: state} do
    assert Enum.any?(state.stories, &(&1.state == "background"))
    assert Enum.any?(state.story_events, &(&1.classification == "stale_background_state"))
  end

  test "storage applies every accepted story op without final proof-store projection copy", %{
    state: state
  } do
    roof = Enum.find(state.stories, &(&1.story_key == "roof-repair-quote"))
    ferry = Enum.find(state.stories, &(&1.story_key == "harbor-ferry-strike"))

    assert roof.background_facts["status"] == "waiting_for_board_packet"
    assert roof.questions["blocker"] == "hoa_approval"
    assert "contractor says the ladder crew is backed up" in roof.colors
    assert roof.structural_facts["quoted_amount"] == "14800"
    assert ferry.state == "background"

    committed_op_types = MapSet.new(Enum.map(state.proposal_ops, & &1.op_type))

    for op_type <-
          ~w(merge_background append_colors add_questions mark_state mark_last_seen_input) do
      assert MapSet.member?(committed_op_types, op_type)
    end

    projection_events =
      state.story_events
      |> Enum.filter(
        &(&1.classification in ~w(merge_background append_colors add_questions mark_state mark_last_seen_input))
      )

    assert length(projection_events) >= 5

    assert Enum.all?(
             projection_events,
             &(&1.proposal_id && &1.proposal_op_id && &1.graph_commit_id)
           )
  end

  test "evidence refs enforce supported subjects, matching direct foreign keys, and committed inputs",
       %{state: state} do
    input_id = state.inputs |> hd() |> Map.fetch!(:id)
    edge_id = state.edges |> hd() |> Map.fetch!(:id)
    graph_commit = hd(state.graph_commits)

    assert EvidenceRef.changeset(%EvidenceRef{}, %{
             tenant_id: state.tenant_id,
             subject_type: "edge",
             subject_id: edge_id,
             input_id: input_id,
             edge_id: edge_id
           }).valid?

    refute EvidenceRef.changeset(%EvidenceRef{}, %{
             tenant_id: state.tenant_id,
             subject_type: "edge",
             subject_id: edge_id,
             input_id: input_id,
             edge_id: Ecto.UUID.generate()
           }).valid?

    refute EvidenceRef.changeset(%EvidenceRef{}, %{
             tenant_id: state.tenant_id,
             subject_type: "unknown",
             subject_id: edge_id,
             input_id: input_id
           }).valid?

    refute EvidenceRef.changeset(%EvidenceRef{}, %{
             tenant_id: state.tenant_id,
             subject_type: "graph_commit",
             subject_id: graph_commit.id,
             input_id: input_id
           }).valid?

    assert EvidenceRef.changeset(%EvidenceRef{}, %{
             tenant_id: state.tenant_id,
             subject_type: "graph_commit",
             subject_id: graph_commit.id,
             input_id: input_id,
             proposal_id: graph_commit.proposal_id,
             proposal_op_id: graph_commit.proposal_op_id
           }).valid?
  end

  test "migration-equivalent SQL carries database-side hardening constraints" do
    sql =
      File.read!(
        Path.expand(
          "../../priv/repo/migrations/20260517000000_create_storage_schema.sql",
          __DIR__
        )
      )

    assert sql =~ "CHECK (jsonb_array_length(evidence_refs) > 0)"
    assert sql =~ "edge_type <> 'related'"
    assert sql =~ "seen_states_verified_output_trigger"

    assert sql =~
             "status IN ('pending', 'accepted', 'accepted_weak', 'rejected', 'needs_more_evidence', 'committed')"

    assert sql =~ "graph_commits_commit_boundary_trigger"
    assert sql =~ "graph commits require an accepted arbitration decision"
    assert sql =~ "evidence_refs_subject_contract_trigger"
    assert sql =~ "subject_type IN ("
    assert sql =~ "graph_commit evidence subject is not committed"
  end

  test "latest output remains user-relative and evidence-backed", %{state: state, report: report} do
    roof = Enum.find(state.stories, &String.contains?(&1.title, "Roof repair"))

    assert DateTime.to_iso8601(roof.last_material_at) == "2026-05-12T23:20:00.000000Z"
    assert report.first_briefing.evidence_refs != []

    assert Enum.all?(
             state.decisions,
             &(is_list(&1.evidence_refs) and &1.evidence_refs != [] and is_float(&1.confidence))
           )

    assert Enum.all?(state.evidence_refs, &is_binary(&1.evidence_label))
  end

  defp node_key(state, id) do
    state.soup_nodes |> Enum.find(&(&1.id == id)) |> Map.fetch!(:node_key)
  end
end
