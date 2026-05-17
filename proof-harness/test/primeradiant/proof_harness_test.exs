defmodule Primeradiant.ProofHarnessTest do
  use ExUnit.Case, async: true

  alias Primeradiant.Ingestion.FixtureLoader
  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.ProofHarness
  alias Primeradiant.Projections.StoryClassifier
  alias Primeradiant.Proposals.Builder
  alias Primeradiant.Arbitration.Engine
  alias Primeradiant.Soup.Store

  test "golden corpus includes inspectable expected artifacts" do
    root =
      :primeradiant_proof_harness
      |> :code.priv_dir()
      |> to_string()
      |> Path.join("fixtures/primeradiant_golden")

    for path <- [
          "expected/nodes.json",
          "expected/edges.json",
          "expected/proposals.json",
          "expected/story_state_after_first_pass.json",
          "expected/first_briefing.html",
          "expected/second_briefing_delta.html",
          "expected/stale_noop_state.json",
          "tests/acceptance_criteria.json",
          "tests/forbidden_failures.json"
        ] do
      assert File.exists?(Path.join(root, path))
    end
  end

  test "proof harness covers every required proof-gate category" do
    result = ProofHarness.run()
    decisions = Map.new(result.decisions, &{&1.fixture_id, &1})

    assert decisions["public_story_001_initial_report"].classification == :new_story
    assert decisions["public_story_002_followup_update"].classification == :substantive_update

    assert decisions["public_story_003_duplicate_reinforcement"].classification ==
             :repeated_noop_input

    assert decisions["public_story_004_correction_or_contradiction"].classification ==
             :conflict_correction

    assert decisions["public_story_005_framing_spin"].classification ==
             :color_spin_without_structural_change

    assert decisions["public_story_006_near_duplicate_overlap"].classification ==
             :repeated_noop_input

    assert decisions["public_story_007_adjacent_distinct_terminal_walkout"].classification ==
             :new_story

    assert decisions["private_thread_005_low_global_rank_personal_watch"].classification ==
             :new_story

    assert decisions["private_thread_001_request"].classification == :new_story

    assert decisions["private_thread_002_reply_attach"].classification ==
             :attach_to_existing_story

    assert decisions["private_thread_003_unresolved_question"].classification ==
             :stale_background_state

    assert decisions["private_thread_004_later_update"].classification == :substantive_update

    assert decisions["stale_case_001_related_no_material_change"].classification ==
             :stale_background_state
  end

  test "input normalization extracts structural, background, question, and color signals" do
    corpus = FixtureLoader.load_corpus()

    roof_wait =
      Enum.find(corpus.inputs, &(&1["fixture_id"] == "private_thread_003_unresolved_question"))

    public_update =
      Enum.find(corpus.inputs, &(&1["fixture_id"] == "public_story_002_followup_update"))

    assert Normalizer.normalize(roof_wait).background == %{"status" => "waiting_for_board_packet"}
    assert Normalizer.normalize(roof_wait).questions == %{"blocker" => "hoa_approval"}
    assert Normalizer.normalize(public_update).facts["negotiations"] == "scheduled_17_00"

    assert "city says tourists should use the tunnel" in Normalizer.normalize(public_update).colors
  end

  test "fixture inputs are prose inputs, not trusted classifier labels" do
    corpus = FixtureLoader.load_corpus()

    refute Enum.any?(corpus.inputs, fn input ->
             String.match?(input["body_text"], ~r/(^|\n)(FACT|COLOR|BACKGROUND|QUESTION)\s/)
           end)
  end

  test "all graph mutations come through proposals and commits with evidence and confidence" do
    result = ProofHarness.run()

    assert length(result.store.proposals) == length(result.decisions)
    assert Enum.all?(result.store.proposals, &match?(%Primeradiant.Proposals.Proposal{}, &1))

    assert Enum.all?(
             result.store.proposals,
             &(is_list(&1.evidence_refs) and &1.evidence_refs != [] and is_float(&1.confidence))
           )

    assert Enum.all?(
             result.store.proposals,
             &(&1.status == :accepted and String.starts_with?(&1.agent_run_id, "agent-run:"))
           )

    assert Enum.all?(result.store.proposals, fn proposal ->
             Enum.all?(
               proposal.ops,
               &(&1.evidence_refs == proposal.evidence_refs and
                   &1.confidence == proposal.confidence)
             )
           end)

    assert length(result.store.commits) > length(result.decisions)

    assert Enum.all?(
             result.store.commits,
             &(String.starts_with?(&1.proposal_id, "proposal:") and &1.status == :committed and
                 &1.evidence_refs != [] and is_float(&1.confidence))
           )

    assert Enum.all?(
             result.store.edges,
             &(&1.edge_type in [
                 :supports,
                 :updates,
                 :duplicates,
                 :contradicts,
                 :adds_color,
                 :part_of,
                 :watch_applies_to
               ])
           )

    assert Enum.all?(
             result.store.edges,
             &(&1.status == :committed and String.starts_with?(&1.proposal_id, "proposal:"))
           )

    assert Enum.all?(result.store.edges, &(&1.evidence_refs != []))
    assert Enum.all?(result.store.edges, &is_float(&1.confidence))

    assert Enum.all?(
             result.store.proposal_decisions,
             &(&1.from == :pending and &1.to == :accepted and &1.evidence_refs != [] and
                 is_float(&1.confidence))
           )

    assert length(result.store.proposal_decisions) == length(result.store.proposals)

    assert MapSet.new(Enum.map(Map.values(result.store.nodes), & &1.node_type)) ==
             MapSet.new([:input, :story, :claim, :entity, :user_watch, :authored_output])

    assert Enum.all?(
             result.store.edges,
             &(Map.has_key?(result.store.nodes, &1.from) and
                 Map.has_key?(result.store.nodes, &1.to))
           )

    assert MapSet.subset?(
             MapSet.new([
               :supports,
               :updates,
               :duplicates,
               :contradicts,
               :adds_color,
               :part_of,
               :watch_applies_to
             ]),
             MapSet.new(Enum.map(result.store.edges, & &1.edge_type))
           )

    refute Enum.any?(result.store.edges, &(&1.edge_type == :related))

    assert Enum.any?(
             result.store.edges,
             &(&1.edge_type == :watch_applies_to and &1.from == "watch:basement_humidity")
           )

    assert Enum.any?(
             result.store.edges,
             &(&1.edge_type == :part_of and &1.from == "harbor-ferry-terminal" and
                 &1.to == "harbor-ferry-strike")
           )
  end

  test "arbitration rejects unevidenced, already-decided, and generic-related proposals before commit" do
    [raw | _] = FixtureLoader.load_corpus().inputs
    normalized = Normalizer.normalize(raw)
    proposal = Builder.build(normalized, StoryClassifier.decide(Store.new(), normalized))

    assert_raise ArgumentError, "proposal requires evidence refs", fn ->
      Engine.commit(Store.new(), %{proposal | evidence_refs: [], ops: []}, normalized)
    end

    assert_raise ArgumentError, "proposal must be pending before arbitration", fn ->
      Engine.commit(Store.new(), %{proposal | status: :accepted}, normalized)
    end

    related_op = proposal.ops |> hd() |> Map.put(:edge_type, :related)

    assert_raise ArgumentError, "unsupported edge type", fn ->
      Engine.commit(Store.new(), %{proposal | ops: [related_op]}, normalized)
    end

    inaccessible_op_proposal =
      FixtureLoader.load_corpus().inputs
      |> Enum.find(&(&1["fixture_id"] == "private_thread_001_request"))
      |> Normalizer.normalize()
      |> then(&Builder.build(&1, StoryClassifier.decide(Store.new(), &1)))

    hostile = %{
      proposal
      | actor_id: "stranger",
        ops: proposal.ops ++ inaccessible_op_proposal.ops
    }

    assert_raise ArgumentError, "proposal evidence is not accessible to actor", fn ->
      Engine.commit(Store.new(), hostile, normalized)
    end

    private_raw =
      FixtureLoader.load_corpus().inputs
      |> Enum.find(&(&1["fixture_id"] == "private_thread_001_request"))

    private_normalized = Normalizer.normalize(private_raw)

    private_proposal =
      Builder.build(private_normalized, StoryClassifier.decide(Store.new(), private_normalized))

    assert_raise ArgumentError, "proposal evidence is not accessible to actor", fn ->
      Engine.commit(Store.new(), %{private_proposal | actor_id: "not-flynn"}, private_normalized)
    end
  end

  test "near-collision pack preserves story identity without destructive merges" do
    result = ProofHarness.run()
    decisions = Map.new(result.decisions, &{&1.fixture_id, &1})

    assert decisions["public_story_006_near_duplicate_overlap"].story_key ==
             decisions["public_story_001_initial_report"].story_key

    assert decisions["public_story_006_near_duplicate_overlap"].classification ==
             :repeated_noop_input

    refute decisions["public_story_007_adjacent_distinct_terminal_walkout"].story_key ==
             decisions["public_story_001_initial_report"].story_key

    ferry_story = result.store.stories[decisions["public_story_001_initial_report"].story_key]

    adjacent_story =
      result.store.stories[
        decisions["public_story_007_adjacent_distinct_terminal_walkout"].story_key
      ]

    assert "public_story_006_near_duplicate_overlap" in ferry_story.inputs
    refute "public_story_007_adjacent_distinct_terminal_walkout" in ferry_story.inputs
    assert adjacent_story.structural_facts["labor_action"] == "cafe_walkout"

    duplicate_edges =
      Enum.filter(
        result.store.edges,
        &(&1.from == "public_story_006_near_duplicate_overlap" and
            &1.to == ferry_story.story_key)
      )

    assert Enum.map(duplicate_edges, & &1.edge_type) == [:duplicates]

    refute Enum.any?(
             result.store.commits,
             &(&1.proposal_id == "proposal:public_story_006_near_duplicate_overlap" and
                 &1.op == :merge_facts)
           )

    assert ferry_story.fact_provenance["crossings"].evidence_refs != []

    assert ferry_story.fact_provenance["crossings"].agent_run_id ==
             "agent-run:fixture-story-seeker"

    assert is_float(ferry_story.fact_provenance["crossings"].confidence)

    assert Enum.any?(ferry_story.conflicts, &(&1.fact == "crossings" and &1.incoming == "normal"))

    assert Enum.any?(
             ferry_story.conflicts,
             &(&1.fact == "strike_status" and &1.incoming == "resolved" and
                 &1.proposal_id == "proposal:public_story_004_correction_or_contradiction" and
                 &1.agent_run_id == "agent-run:fixture-story-seeker" and &1.evidence_refs != [] and
                 is_float(&1.confidence))
           )
  end

  test "authoring output is grounded, user-relative, and watch-aware" do
    result = ProofHarness.run()

    assert result.first_briefing.verified
    assert result.first_briefing.sentence_evidence != []
    assert Enum.all?(result.first_briefing.sentence_evidence, &(&1.evidence_refs != []))
    assert result.first_briefing.text =~ "what changed"
    assert result.first_briefing.text =~ "Roof repair quote needed before Friday"
    assert result.first_briefing.text =~ "Harbor ferry strike halts evening crossings"
    assert Enum.any?(result.first_briefing.bullets, &String.starts_with?(&1, "[watch]"))
    assert result.first_briefing.text =~ "Basement humidity sensor battery reminder"

    assert result.second_briefing.text =~
             "changed for Flynn since last seen because structural facts moved"

    assert result.second_briefing.text =~ "quoted_amount=14800"
    refute result.second_briefing.text =~ "deadline=friday"
  end

  test "authoring respects private ACL evidence boundaries before marking seen-state" do
    result = ProofHarness.run()
    ferry_key = "harbor-ferry-strike"

    mixed_store =
      result.store
      |> put_in([:nodes, "private:ferry-note"], %{
        id: "private:ferry-note",
        node_type: :input,
        title: "Private ferry note",
        state: :active,
        attrs: %{acl: %{"privacy" => "private", "participants" => ["flynn"]}},
        evidence_refs: ["private:ferry-note"]
      })
      |> update_in([:stories, ferry_key, :inputs], &["private:ferry-note" | &1])
      |> put_in([:stories, ferry_key, :structural_facts, "private_code"], "flynn_only")
      |> put_in([:stories, ferry_key, :fact_provenance, "private_code"], %{
        evidence_refs: ["private:ferry-note"],
        proposal_id: "proposal:private-ferry-note",
        agent_run_id: "agent-run:fixture-story-seeker",
        confidence: 0.9
      })

    stranger_seen = Primeradiant.UserContext.SeenState.new("stranger")

    stranger_briefing =
      Primeradiant.Authoring.Briefing.render(mixed_store, stranger_seen, "stranger")

    refute stranger_briefing.text =~ "Roof repair"
    refute stranger_briefing.text =~ "private_code"
    refute stranger_briefing.text =~ "flynn_only"
    refute Enum.any?(stranger_briefing.evidence_refs, &String.starts_with?(&1, "private_thread_"))
    refute "private:ferry-note" in stranger_briefing.evidence_refs

    unverified = %{result.first_briefing | verified: false}

    assert_raise ArgumentError, "cannot mark seen for unverified output", fn ->
      Primeradiant.Authoring.Briefing.mark_seen(stranger_seen, result.store, unverified)
    end

    flynn_seen =
      Primeradiant.UserContext.SeenState.new("flynn")
      |> Primeradiant.Authoring.Briefing.mark_seen(result.store, result.first_briefing)

    seen_roof = flynn_seen.stories["roof-repair-quote"]
    assert seen_roof.output_id == result.first_briefing.output_id
    assert seen_roof.seen_input_refs != []
    assert seen_roof.seen_claim_refs != []
  end

  test "stale detection marks dormant story regions without deleting them" do
    result = ProofHarness.run()

    assert result.stale_story_keys != []
    assert Enum.any?(result.stale_story_keys, &String.contains?(&1, "ferry"))
    assert Enum.any?(Map.values(result.store.stories), &(&1.state == :background))
  end

  test "proof result keeps user-relative grounding on the latest authored output" do
    result = ProofHarness.run()

    roof_story =
      Enum.find(Map.values(result.store.stories), &String.contains?(&1.title, "Roof repair"))

    assert roof_story.last_material_at == "2026-05-12T16:20:00-07:00"
    assert result.first_briefing.evidence_refs != []

    assert Enum.all?(
             result.decisions,
             &(is_list(&1.evidence_refs) and &1.evidence_refs != [] and is_float(&1.confidence))
           )
  end
end
