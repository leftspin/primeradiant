defmodule Primeradiant.StorageHarness.FixtureImporter do
  @moduledoc false

  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Mediation.WriteGate, as: Engine
  alias Primeradiant.Projections.StoryClassifier
  alias Primeradiant.Proposals.Builder
  alias Primeradiant.Soup.Store

  alias Primeradiant.StorageHarness.{
    AgentRun,
    AuthoredOutput,
    AuthoredOutputUnit,
    ChangesetStore,
    Conflict,
    Edge,
    EvidenceRef,
    GraphCommit,
    Input,
    Proposal,
    ProposalDecision,
    ProposalOp,
    SeenState,
    SeenStateRef,
    SoupNode,
    State,
    Story,
    StoryEvent,
    StoryFactVersion,
    Watch
  }

  @expected_artifacts [
    "expected/nodes.json",
    "expected/edges.json",
    "expected/proposals.json",
    "expected/story_state_after_first_pass.json",
    "expected/first_briefing.html",
    "expected/second_briefing_delta.html",
    "expected/stale_noop_state.json",
    "tests/acceptance_criteria.json",
    "tests/forbidden_failures.json"
  ]

  def import_fixture_corpus(path, actor \\ "flynn") do
    corpus = load_corpus!(path)
    assert_prose_inputs!(corpus.inputs)

    state =
      State.new(user_id: actor)
      |> Map.put(:raw_inputs, corpus.inputs)
      |> Map.put(:expected_artifacts, expected_artifacts!(path))
      |> insert_agent_run()
      |> insert_watches(corpus.watches, actor)

    proof_store = Enum.reduce(corpus.watches, Store.new(), &Store.register_watch(&2, &1))
    {first_pass_inputs, second_pass_inputs} = Enum.split_with(corpus.inputs, &first_pass_input?/1)

    {state, proof_store, decisions} =
      ingest_inputs(state, proof_store, first_pass_inputs, [], actor)

    seen_state = storage_seen_state(actor)
    first_briefing = render_storage_briefing(state, seen_state, actor)
    state = record_output!(state, first_briefing)
    seen_state = storage_mark_seen(state, seen_state, first_briefing)
    state = mark_seen!(state, seen_state, first_briefing)

    {state, _proof_store, decisions} =
      ingest_inputs(state, proof_store, second_pass_inputs, decisions, actor)

    second_briefing = render_storage_briefing(state, seen_state, actor)

    state = %{state | decisions: decisions}

    {:ok, state,
     %{
       expected_artifacts: state.expected_artifacts,
       inputs: length(state.inputs),
       proposals: length(state.proposals),
       graph_commits: length(state.graph_commits),
       persistence_mode: :changeset_validated,
       db_backed?: false,
       source_behavior: :proof_harness_semantics,
       first_briefing: first_briefing,
       second_briefing: second_briefing
     }}
  end

  def load_corpus!(path) do
    manifest = read_json!(Path.join(path, "manifest.json"))

    inputs =
      manifest["inputs"]
      |> Enum.map(&read_json!(Path.join([path, "inputs", &1])))
      |> Enum.sort_by(& &1["observed_at"])

    watches =
      manifest["watches"]
      |> Enum.map(&read_json!(Path.join([path, "watches", &1])))

    %{manifest: manifest, inputs: inputs, watches: watches}
  end

  defp storage_seen_state(user_id), do: %{user_id: user_id, stories: %{}}

  defp render_storage_briefing(state, seen_state, user_id) do
    stories =
      state.stories
      |> Enum.filter(&story_visible_to_actor?(state, &1, user_id))
      |> Enum.filter(&story_changed_for_actor?(state, &1, seen_state, user_id))
      |> Enum.sort_by(&story_rank(state, &1), :desc)

    packet = storage_evidence_packet(state, stories, user_id)

    bullet_records =
      Enum.map(stories, &storage_story_bullet(state, &1, seen_state, packet, user_id))

    bullet_points = Enum.map(bullet_records, & &1.text)
    output_id = "storage-authored-output:#{length(state.authored_outputs) + 1}"

    text =
      case bullet_points do
        [] -> ""
        _ -> "what changed for Flynn since last seen, and why\n" <> Enum.join(bullet_points, "\n")
      end

    %{
      output_id: output_id,
      user_id: user_id,
      bullets: bullet_points,
      text: text,
      evidence_packet: packet,
      sentence_evidence: bullet_records,
      evidence_refs: packet.evidence_refs,
      touched_story_keys: Enum.map(stories, & &1.story_key),
      verified: verify_storage_output(bullet_records, packet)
    }
  end

  defp storage_mark_seen(state, seen_state, authored_output) do
    Enum.reduce(authored_output.touched_story_keys, seen_state, fn story_key, acc ->
      story = Enum.find(state.stories, &(&1.story_key == story_key))
      refs = storage_seen_refs_for_story(state, authored_output, story_key)

      put_in(acc, [:stories, story_key], %{
        seen_story_version: story.version,
        output_id: authored_output.output_id,
        seen_input_refs: refs.seen_input_refs,
        seen_claim_refs: refs.seen_claim_refs,
        seen_at: story.updated_at_story
      })
    end)
  end

  defp storage_evidence_packet(state, stories, user_id) do
    story_evidence =
      Map.new(stories, fn story ->
        refs =
          state
          |> story_input_refs(story.story_key)
          |> Enum.reverse()
          |> Enum.filter(fn input_ref ->
            input = Enum.find(state.inputs, &(&1.fixture_id == input_ref))
            input_visible_to_actor?(input, user_id)
          end)
          |> Enum.take(3)

        {story.story_key, refs}
      end)

    %{
      user_id: user_id,
      story_evidence: story_evidence,
      evidence_refs: story_evidence |> Map.values() |> List.flatten() |> Enum.uniq()
    }
  end

  defp storage_story_bullet(state, story, seen_state, packet, user_id) do
    last_seen = get_in(seen_state, [:stories, story.story_key, :seen_story_version]) || 0
    latest_event = latest_visible_story_event(state, story, user_id)
    classification = if latest_event, do: latest_event.classification, else: "new_story"
    why = why_text(classification, last_seen)

    fact_pairs =
      state
      |> visible_fact_pairs(story, user_id, latest_event, last_seen)
      |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
      |> Enum.join(", ")

    evidence_refs = packet.story_evidence[story.story_key] || []
    evidence = evidence_refs |> Enum.take(3) |> Enum.join(", ")
    prefix = if watched_story?(state, story.story_key), do: "[watch] ", else: ""

    %{
      text:
        "#{prefix}#{story.title}: #{why}. Evidence: #{evidence}#{if fact_pairs != "", do: " (#{fact_pairs})", else: ""}",
      evidence_refs: evidence_refs,
      claim_refs: claim_refs_for_story_facts(state, story, latest_event, last_seen, user_id)
    }
  end

  defp verify_storage_output(bullet_records, packet) do
    packet_refs = MapSet.new(packet.evidence_refs)

    Enum.all?(bullet_records, fn bullet ->
      bullet.evidence_refs != [] and bullet.claim_refs != [] and
        Enum.all?(bullet.evidence_refs, &MapSet.member?(packet_refs, &1))
    end)
  end

  defp story_changed_for_actor?(state, story, seen_state, user_id) do
    seen_inputs =
      (get_in(seen_state, [:stories, story.story_key, :seen_input_refs]) || []) |> MapSet.new()

    seen_version = get_in(seen_state, [:stories, story.story_key, :seen_story_version]) || 0

    state
    |> visible_story_events(story, user_id)
    |> Enum.any?(fn event ->
      input_ref = input_fixture_id(state, event.input_id)

      not MapSet.member?(seen_inputs, input_ref) and event.story_version >= seen_version and
        (seen_version == 0 or material_visible_event?(event))
    end)
  end

  defp material_visible_event?(event) do
    event.classification in ["substantive_update", "conflict_correction"] and
      event.changed_facts != %{}
  end

  defp visible_fact_pairs(state, story, user_id, latest_event, last_seen) do
    visible_facts =
      story.structural_facts
      |> Enum.filter(fn {key, _value} ->
        state.story_fact_versions
        |> Enum.filter(&(&1.story_id == story.id and &1.fact_key == key))
        |> Enum.any?(fn fact ->
          input = Enum.find(state.inputs, &(&1.id == fact.input_id))
          input_visible_to_actor?(input, user_id)
        end)
      end)
      |> Map.new()

    facts =
      if (last_seen > 0 and latest_event) && latest_event.changed_facts != %{} do
        Map.take(latest_event.changed_facts, Map.keys(visible_facts))
      else
        visible_facts
      end

    facts
    |> Enum.sort()
    |> Enum.take(3)
  end

  defp claim_refs_for_story_facts(state, story, latest_event, last_seen, user_id) do
    state
    |> visible_fact_pairs(story, user_id, latest_event, last_seen)
    |> Enum.map(fn {key, _value} -> "claim:#{story.story_key}:#{key}" end)
    |> Enum.filter(fn claim_ref -> Map.has_key?(state.source_ids, {:node, claim_ref}) end)
  end

  defp latest_visible_story_event(state, story, user_id) do
    events = visible_story_events(state, story, user_id)
    Enum.find(Enum.reverse(events), &material_visible_event?/1) || List.last(events)
  end

  defp visible_story_events(state, story, user_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.filter(fn event ->
      input = Enum.find(state.inputs, &(&1.id == event.input_id))
      input_visible_to_actor?(input, user_id)
    end)
  end

  defp story_visible_to_actor?(state, story, user_id) do
    state
    |> story_input_refs(story.story_key)
    |> Enum.any?(fn input_ref ->
      input = Enum.find(state.inputs, &(&1.fixture_id == input_ref))
      input_visible_to_actor?(input, user_id)
    end)
  end

  defp story_rank(state, story) do
    base = story.version * 10 + map_size(story.structural_facts) * 5
    if watched_story?(state, story.story_key), do: base + 100, else: base
  end

  defp watched_story?(state, story_key) do
    story_node_id = Map.get(state.source_ids, {:node, story_key})

    Enum.any?(state.edges, fn edge ->
      edge.edge_type == "watch_applies_to" and edge.to_node_id == story_node_id
    end)
  end

  defp story_input_refs(state, story_key) do
    story_node_id = Map.get(state.source_ids, {:node, story_key})

    state.edges
    |> Enum.filter(fn edge ->
      edge.to_node_id == story_node_id and edge.edge_type in ["supports", "updates", "duplicates"]
    end)
    |> Enum.map(fn edge -> node_by_id!(state, edge.from_node_id).node_key end)
  end

  defp input_fixture_id(state, input_id) do
    state.inputs
    |> Enum.find(&(&1.id == input_id))
    |> case do
      nil -> nil
      input -> input.fixture_id
    end
  end

  defp why_text("new_story", _last_seen), do: "new to Flynn since last seen"

  defp why_text("substantive_update", _last_seen),
    do: "changed for Flynn since last seen because structural facts moved"

  defp why_text("conflict_correction", _last_seen),
    do: "changed for Flynn since last seen because prior state was corrected"

  defp why_text("color_spin_without_structural_change", 0),
    do: "new to Flynn and notable mainly for framing"

  defp why_text("color_spin_without_structural_change", _last_seen),
    do: "did not structurally change, but the framing shifted"

  defp why_text("stale_background_state", 0), do: "new to Flynn only as background context"
  defp why_text("stale_background_state", _last_seen), do: "mostly background since last seen"
  defp why_text("attach_to_existing_story", 0), do: "new to Flynn as another connected input"

  defp why_text("attach_to_existing_story", _last_seen),
    do: "same story, with another connected input"

  defp why_text(_classification, 0), do: "new to Flynn since last seen"
  defp why_text(_classification, _last_seen), do: "still worth noting for Flynn"

  def validate_proposal_for_commit!(state, proposal) do
    cond do
      proposal.status != :pending ->
        raise ArgumentError, "proposal must be pending before arbitration"

      proposal.evidence_refs == [] ->
        raise ArgumentError, "proposal requires evidence refs"

      not is_float(proposal.confidence) ->
        raise ArgumentError, "proposal requires confidence"

      true ->
        :ok
    end

    validate_accessible_evidence!(state, proposal)

    Enum.each(proposal.ops, fn op ->
      if Map.get(op, :evidence_refs, []) == [],
        do: raise(ArgumentError, "op requires evidence refs")

      if not is_float(Map.get(op, :confidence)),
        do: raise(ArgumentError, "op requires confidence")

      if Map.get(op, :edge_type) == :related, do: raise(ArgumentError, "unsupported edge type")

      ProposalOp.changeset(%ProposalOp{}, %{
        tenant_id: state.tenant_id,
        proposal_id: Ecto.UUID.generate(),
        position: 0,
        op_type: Atom.to_string(op.op),
        payload: stringify_payload(op),
        evidence_refs: ChangesetStore.evidence_maps(op.evidence_refs),
        confidence: ChangesetStore.decimal(op.confidence),
        status: "pending"
      })
      |> case do
        %{valid?: true} -> :ok
        _ -> raise ArgumentError, "unsupported op"
      end
    end)
  end

  defp ingest_inputs(state, proof_store, inputs, decisions, actor) do
    Enum.reduce(inputs, {state, proof_store, decisions}, fn raw,
                                                            {state, proof_store, decisions} ->
      normalized = Normalizer.normalize(raw)
      state = insert_input(state, raw, normalized)
      decision = StoryClassifier.decide(proof_store, normalized)
      proposal = Builder.build(normalized, decision)

      validate_proposal_for_commit!(state, proposal)
      state = submit_decide_and_commit_proposal(state, proposal, normalized, decision, actor)
      proof_store = Engine.commit(proof_store, proposal, normalized)

      decision_record = %{
        fixture_id: raw["fixture_id"],
        classification: decision.classification,
        story_key: decision.story_key,
        proposal_id: proposal.id,
        evidence_refs: proposal.evidence_refs,
        confidence: proposal.confidence
      }

      {state, proof_store, decisions ++ [decision_record]}
    end)
  end

  defp insert_agent_run(state) do
    agent_run =
      ChangesetStore.insert!(AgentRun, %{
        tenant_id: state.tenant_id,
        agent_run_key: "agent-run:fixture-story-seeker",
        agent_type: "fixture_story_seeker",
        scope: %{"fixture_corpus" => "primeradiant_golden"},
        status: "succeeded"
      })

    state
    |> State.append(:agent_runs, agent_run)
    |> State.put_source_id(:agent_run, "agent-run:fixture-story-seeker", agent_run.id)
  end

  defp insert_watches(state, watches, user_id) do
    Enum.reduce(watches, state, fn raw, state ->
      watch =
        ChangesetStore.insert!(Watch, %{
          tenant_id: state.tenant_id,
          user_id: user_id,
          watch_key: raw["watch_id"],
          intent: raw["intent"] || raw["label"],
          priority: raw["priority"] || 0,
          match_any: raw["match_any"] || [],
          filters: raw["filters"] || %{},
          status: "active",
          attrs: raw
        })

      state
      |> State.append(:watches, watch)
      |> State.put_source_id(:watch, raw["watch_id"], watch.id)
    end)
  end

  defp insert_input(state, raw, normalized) do
    input =
      ChangesetStore.insert!(Input, %{
        tenant_id: state.tenant_id,
        fixture_id: raw["fixture_id"],
        source_type: raw["source_type"],
        external_id: raw["external_id"],
        observed_at: ChangesetStore.iso!(raw["observed_at"]),
        title: raw["title"],
        body_text: raw["body_text"],
        object_uri: raw["object_uri"],
        content_sha256: normalized.fingerprint,
        acl: normalized.acl,
        normalized: %{"fixture_id" => normalized.fixture_id},
        facts: normalized.facts,
        background: normalized.background,
        questions: normalized.questions,
        colors: normalized.colors,
        topic_tokens: normalized.topic_tokens |> MapSet.to_list() |> Enum.sort()
      })

    state
    |> State.append(:inputs, input)
    |> State.put_source_id(:input, raw["fixture_id"], input.id)
  end

  defp submit_decide_and_commit_proposal(state, proposal, normalized, decision, actor) do
    {state, proposal_row, op_rows} = submit_proposal(state, proposal, decision)

    {state, proposal_row, op_rows} =
      decide_proposal(state, proposal_row, op_rows, proposal, actor)

    commit_accepted_ops(state, proposal_row, op_rows, proposal, normalized, actor)
  end

  defp submit_proposal(state, proposal, decision) do
    proposal_row =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: proposal.id,
        agent_run_id: State.source_id!(state, :agent_run, proposal.agent_run_id),
        actor_id: proposal.actor_id,
        story_id: story_id(state, decision.story_key),
        fixture_id: proposal.fixture_id,
        classification: Atom.to_string(proposal.classification),
        confidence: ChangesetStore.decimal(proposal.confidence),
        rationale: proposal.rationale,
        status: "pending"
      })

    state =
      state
      |> State.append(:proposals, proposal_row)
      |> State.put_source_id(:proposal, proposal.id, proposal_row.id)
      |> State.audit(%{
        event: :proposal_submitted,
        proposal_id: proposal_row.id,
        status: proposal_row.status
      })
      |> add_evidence_refs("proposal", proposal_row.id, proposal.evidence_refs, %{
        proposal_id: proposal_row.id
      })

    {state, op_rows} =
      proposal.ops
      |> Enum.with_index()
      |> Enum.reduce({state, []}, fn {op, position}, {state, op_rows} ->
        op_row =
          ChangesetStore.insert!(ProposalOp, %{
            tenant_id: state.tenant_id,
            proposal_id: proposal_row.id,
            position: position,
            op_type: Atom.to_string(op.op),
            payload: stringify_payload(op),
            evidence_refs: ChangesetStore.evidence_maps(op.evidence_refs),
            confidence: ChangesetStore.decimal(op.confidence),
            status: "pending"
          })

        state =
          state
          |> State.append(:proposal_ops, op_row)
          |> State.put_source_id(:proposal_op, {proposal.id, position}, op_row.id)
          |> add_evidence_refs("proposal_op", op_row.id, op.evidence_refs, %{
            proposal_id: proposal_row.id,
            proposal_op_id: op_row.id
          })

        {state, op_rows ++ [{op, op_row}]}
      end)

    {state, proposal_row, op_rows}
  end

  defp decide_proposal(state, proposal_row, op_rows, proposal, actor) do
    decision_row =
      ChangesetStore.insert!(ProposalDecision, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal_row.id,
        from_status: "pending",
        to_status: "accepted",
        actor_type: "arbiter",
        actor_id: actor,
        evidence_refs: ChangesetStore.evidence_maps(proposal.evidence_refs),
        confidence: ChangesetStore.decimal(proposal.confidence),
        rationale: proposal.rationale
      })

    accepted_proposal = ChangesetStore.update!(proposal_row, %{status: "accepted"})

    accepted_ops =
      Enum.map(op_rows, fn {op, op_row} ->
        {op, ChangesetStore.update!(op_row, %{status: "accepted"})}
      end)

    state =
      state
      |> State.append(:proposal_decisions, decision_row)
      |> State.replace(:proposals, proposal_row.id, accepted_proposal)
      |> State.audit(%{
        event: :proposal_decided,
        proposal_id: accepted_proposal.id,
        from_status: decision_row.from_status,
        to_status: decision_row.to_status
      })

    state =
      Enum.reduce(accepted_ops, state, fn {_op, op_row}, state ->
        State.replace(state, :proposal_ops, op_row.id, op_row)
      end)

    {state, accepted_proposal, accepted_ops}
  end

  defp commit_accepted_ops(state, proposal_row, op_rows, _proposal, normalized, actor) do
    Enum.reduce(op_rows, state, fn {op, op_row}, state ->
      committed_op =
        ChangesetStore.update!(op_row, %{
          status: "committed",
          committed_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
        })

      commit_row =
        ChangesetStore.insert!(GraphCommit, %{
          tenant_id: state.tenant_id,
          proposal_id: proposal_row.id,
          proposal_op_id: committed_op.id,
          commit_type: Atom.to_string(op.op),
          committed_by_type: "arbiter",
          committed_by_id: actor,
          evidence_refs: ChangesetStore.evidence_maps(op.evidence_refs),
          confidence: ChangesetStore.decimal(op.confidence)
        })

      state =
        state
        |> State.replace(:proposal_ops, committed_op.id, committed_op)
        |> State.append(:graph_commits, commit_row)
        |> State.audit(%{
          event: :graph_commit_created,
          proposal_id: proposal_row.id,
          proposal_op_id: committed_op.id,
          commit_id: commit_row.id,
          op_type: committed_op.op_type
        })
        |> add_evidence_refs("graph_commit", commit_row.id, op.evidence_refs, %{
          proposal_id: proposal_row.id,
          proposal_op_id: committed_op.id
        })

      apply_committed_op(state, op, normalized, proposal_row, committed_op, commit_row)
    end)
  end

  defp apply_committed_op(
         state,
         %{op: :create_input, input_id: input_ref, title: title, acl: acl},
         _normalized,
         proposal,
         op,
         commit
       ) do
    input_id = State.source_id!(state, :input, input_ref)

    state
    |> insert_node(input_ref, "input", title, %{
      input_id: input_id,
      proposal: proposal,
      op: op,
      commit: commit,
      attrs: %{"acl" => acl}
    })
  end

  defp apply_committed_op(
         state,
         %{op: :create_story, story_key: story_key, title: title, observed_at: observed_at},
         normalized,
         proposal,
         op,
         commit
       ) do
    story =
      ChangesetStore.insert!(Story, %{
        tenant_id: state.tenant_id,
        story_key: story_key,
        title: title,
        state: "active",
        version: 0,
        first_observed_at: ChangesetStore.iso!(observed_at),
        updated_at_story: ChangesetStore.iso!(observed_at),
        last_material_at: ChangesetStore.iso!(observed_at),
        structural_facts: %{},
        background_facts: %{},
        colors: [],
        questions: %{},
        topic_tokens: normalized.topic_tokens |> MapSet.to_list() |> Enum.sort(),
        attrs: %{}
      })

    state
    |> State.append(:stories, story)
    |> State.put_source_id(:story, story_key, story.id)
    |> insert_node(story_key, "story", title, %{
      story_id: story.id,
      proposal: proposal,
      op: op,
      commit: commit
    })
  end

  defp apply_committed_op(
         state,
         %{op: :attach_input, story_key: story_key, input_id: input_ref, edge_type: edge_type},
         _normalized,
         proposal,
         op,
         commit
       ) do
    insert_edge(state, input_ref, story_key, edge_type, proposal, op, commit)
  end

  defp apply_committed_op(
         state,
         %{op: :merge_facts, story_key: story_key, facts: facts},
         normalized,
         proposal,
         op,
         commit
       ) do
    state =
      update_story(state, story_key, fn story ->
        Story.changeset(story, %{
          structural_facts: Map.merge(story.structural_facts, stringify_map(facts)),
          last_material_at: ChangesetStore.iso!(normalized.observed_at)
        })
        |> Ecto.Changeset.apply_changes()
      end)

    Enum.reduce(facts, state, fn {key, value}, state ->
      claim_key = "claim:#{story_key}:#{key}"
      entity_key = "entity:#{value}"

      state =
        state
        |> insert_node(claim_key, "claim", "#{key}=#{value}", %{
          proposal: proposal,
          op: op,
          commit: commit,
          attrs: %{"story_key" => story_key, "fact" => to_string(key), "value" => value}
        })
        |> insert_node(entity_key, "entity", to_string(value), %{
          proposal: proposal,
          op: op,
          commit: commit,
          attrs: %{"value" => value}
        })

      claim_node_id = State.source_id!(state, :node, claim_key)

      fact =
        ChangesetStore.insert!(StoryFactVersion, %{
          tenant_id: state.tenant_id,
          story_id: State.source_id!(state, :story, story_key),
          claim_node_id: claim_node_id,
          fact_key: to_string(key),
          fact_value: to_string(value),
          status: "current",
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          input_id: State.source_id!(state, :input, normalized.fixture_id),
          confidence: op.confidence,
          observed_at: ChangesetStore.iso!(normalized.observed_at)
        })

      state
      |> State.append(:story_fact_versions, fact)
      |> add_evidence_refs("story_fact_version", fact.id, evidence_input_refs(op), %{
        proposal_id: proposal.id,
        proposal_op_id: op.id
      })
    end)
  end

  defp apply_committed_op(
         state,
         %{op: :record_conflicts, story_key: story_key, conflicts: conflicts},
         normalized,
         proposal,
         op,
         commit
       ) do
    Enum.reduce(conflicts, state, fn conflict, state ->
      row =
        ChangesetStore.insert!(Conflict, %{
          tenant_id: state.tenant_id,
          story_id: State.source_id!(state, :story, story_key),
          fact_key: to_string(conflict.fact),
          prior_value: to_string(conflict.prior),
          incoming_value: to_string(conflict.incoming),
          status: "open",
          input_id: State.source_id!(state, :input, normalized.fixture_id),
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          agent_run_id: State.source_id!(state, :agent_run, "agent-run:fixture-story-seeker"),
          confidence: op.confidence
        })

      state
      |> State.append(:conflicts, row)
      |> add_evidence_refs("conflict", row.id, evidence_input_refs(op), %{
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        conflict_id: row.id
      })
    end)
  end

  defp apply_committed_op(
         state,
         %{op: :merge_background, story_key: story_key, background: background},
         normalized,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{
        background_facts: Map.merge(story.background_facts, stringify_map(background))
      })
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_projection_event(
      story_key,
      "merge_background",
      background,
      normalized,
      proposal,
      op,
      commit
    )
  end

  defp apply_committed_op(
         state,
         %{op: :append_colors, story_key: story_key, colors: colors},
         normalized,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{colors: Enum.uniq(story.colors ++ colors)})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_projection_event(
      story_key,
      "append_colors",
      %{"colors" => colors},
      normalized,
      proposal,
      op,
      commit
    )
  end

  defp apply_committed_op(
         state,
         %{op: :add_questions, story_key: story_key, questions: questions},
         normalized,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{questions: Map.merge(story.questions, stringify_map(questions))})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_projection_event(
      story_key,
      "add_questions",
      questions,
      normalized,
      proposal,
      op,
      commit
    )
  end

  defp apply_committed_op(
         state,
         %{op: :mark_state, story_key: story_key, state: story_state},
         normalized,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{state: Atom.to_string(story_state)})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_projection_event(
      story_key,
      "mark_state",
      %{"state" => Atom.to_string(story_state)},
      normalized,
      proposal,
      op,
      commit
    )
  end

  defp apply_committed_op(
         state,
         %{op: :mark_last_seen_input, story_key: story_key, observed_at: observed_at},
         normalized,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{updated_at_story: ChangesetStore.iso!(observed_at)})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_projection_event(
      story_key,
      "mark_last_seen_input",
      %{"input" => normalized.fixture_id},
      normalized,
      proposal,
      op,
      commit
    )
  end

  defp apply_committed_op(
         state,
         %{op: :attach_watch, story_key: story_key, watch_id: watch_key, edge_type: edge_type},
         _normalized,
         proposal,
         op,
         commit
       ) do
    state =
      if Map.has_key?(state.source_ids, {:node, watch_key}) do
        state
      else
        insert_node(state, watch_key, "user_watch", watch_key, %{
          watch_id: State.source_id!(state, :watch, watch_key),
          proposal: proposal,
          op: op,
          commit: commit
        })
      end

    insert_edge(state, watch_key, story_key, edge_type, proposal, op, commit)
  end

  defp apply_committed_op(
         state,
         %{
           op: :attach_story_part_of,
           child_story_key: child,
           parent_story_key: parent,
           edge_type: edge_type
         },
         _normalized,
         proposal,
         op,
         commit
       ) do
    insert_edge(state, child, parent, edge_type, proposal, op, commit)
  end

  defp apply_committed_op(
         state,
         %{
           op: :record_event,
           story_key: story_key,
           classification: classification,
           input_id: input_ref,
           changed_facts: changed_facts
         },
         normalized,
         proposal,
         op,
         commit
       ) do
    event =
      ChangesetStore.insert!(StoryEvent, %{
        tenant_id: state.tenant_id,
        story_id: State.source_id!(state, :story, story_key),
        input_id: State.source_id!(state, :input, input_ref),
        classification: Atom.to_string(classification),
        story_version: story_version(state, story_key),
        changed_facts: stringify_map(changed_facts),
        observed_at: ChangesetStore.iso!(normalized.observed_at),
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        confidence: op.confidence
      })

    state
    |> State.append(:story_events, event)
    |> bump_story(story_key, normalized)
    |> add_evidence_refs("story_event", event.id, evidence_input_refs(op), %{
      proposal_id: proposal.id,
      proposal_op_id: op.id
    })
  end

  defp apply_committed_op(_state, op, _normalized, _proposal, _op_row, _commit),
    do: raise(ArgumentError, "unsupported committed op #{inspect(op)}")

  defp record_projection_event(
         state,
         story_key,
         classification,
         changed_facts,
         normalized,
         proposal,
         op,
         commit
       ) do
    event =
      ChangesetStore.insert!(StoryEvent, %{
        tenant_id: state.tenant_id,
        story_id: State.source_id!(state, :story, story_key),
        input_id: State.source_id!(state, :input, normalized.fixture_id),
        classification: classification,
        story_version: story_version(state, story_key),
        changed_facts: stringify_map(changed_facts),
        observed_at: ChangesetStore.iso!(normalized.observed_at),
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        confidence: op.confidence
      })

    state
    |> State.append(:story_events, event)
    |> bump_story(story_key, normalized)
    |> add_evidence_refs("story_event", event.id, evidence_input_refs(op), %{
      proposal_id: proposal.id,
      proposal_op_id: op.id
    })
  end

  defp insert_node(state, node_key, node_type, title, opts) do
    if Map.has_key?(state.source_ids, {:node, node_key}) do
      state
    else
      proposal = opts.proposal
      op = opts.op
      commit = opts.commit

      node =
        ChangesetStore.insert!(SoupNode, %{
          tenant_id: state.tenant_id,
          node_key: node_key,
          node_type: node_type,
          title: title,
          state: "active",
          input_id: opts[:input_id],
          story_id: opts[:story_id],
          watch_id: opts[:watch_id],
          authored_output_id: opts[:authored_output_id],
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          confidence: op.confidence,
          attrs: opts[:attrs] || %{}
        })

      state
      |> State.append(:soup_nodes, node)
      |> State.put_source_id(:node, node_key, node.id)
      |> add_evidence_refs("soup_node", node.id, evidence_input_refs(op), %{
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        soup_node_id: node.id
      })
    end
  end

  defp insert_edge(state, from_key, to_key, edge_type, proposal, op, commit) do
    from_type = node_type!(state, from_key)
    to_type = node_type!(state, to_key)
    validate_edge_contract!(edge_type, from_type, to_type)

    edge =
      ChangesetStore.insert!(Edge, %{
        tenant_id: state.tenant_id,
        from_node_id: State.source_id!(state, :node, from_key),
        to_node_id: State.source_id!(state, :node, to_key),
        edge_type: Atom.to_string(edge_type),
        status: "committed",
        confidence: op.confidence,
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        attrs: %{}
      })

    state
    |> State.append(:edges, edge)
    |> add_evidence_refs("edge", edge.id, evidence_input_refs(op), %{
      proposal_id: proposal.id,
      proposal_op_id: op.id,
      edge_id: edge.id
    })
  end

  defp record_output!(state, authored_output) do
    validate_authored_output_for_seen!(state, authored_output)

    story_id =
      authored_output.touched_story_keys
      |> List.first()
      |> then(&if &1, do: State.source_id!(state, :story, &1))

    output =
      ChangesetStore.insert!(AuthoredOutput, %{
        tenant_id: state.tenant_id,
        user_id: authored_output.user_id,
        story_id: story_id,
        output_type: "briefing",
        content: authored_output.text,
        evidence_packet: stringify_payload(authored_output.evidence_packet),
        verified: true,
        story_version: 0,
        status: "recorded"
      })

    state =
      state
      |> State.append(:authored_outputs, output)
      |> State.put_source_id(:authored_output, authored_output.output_id, output.id)

    authoring_proposal_key = "proposal:#{authored_output.output_id}"

    proposal =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: authoring_proposal_key,
        agent_run_id: State.source_id!(state, :agent_run, "agent-run:fixture-story-seeker"),
        actor_id: authored_output.user_id,
        story_id: story_id,
        fixture_id: authored_output.output_id,
        classification: "verified_authoring",
        confidence: ChangesetStore.decimal(1.0),
        rationale: "Verified authored output recorded with sentence-level evidence.",
        status: "pending"
      })

    state =
      state
      |> State.append(:proposals, proposal)
      |> State.put_source_id(:proposal, authoring_proposal_key, proposal.id)
      |> State.audit(%{
        event: :proposal_submitted,
        proposal_id: proposal.id,
        status: "pending"
      })
      |> State.audit(%{
        event: :proposal_decided,
        proposal_id: proposal.id,
        from_status: "pending",
        to_status: "accepted"
      })
      |> add_evidence_refs("proposal", proposal.id, authored_output.evidence_refs, %{
        proposal_id: proposal.id
      })

    decision =
      ChangesetStore.insert!(ProposalDecision, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        from_status: "pending",
        to_status: "accepted",
        actor_type: "authoring_verifier",
        actor_id: authored_output.user_id,
        evidence_refs: ChangesetStore.evidence_maps(authored_output.evidence_refs),
        confidence: ChangesetStore.decimal(1.0),
        rationale: "Verified output may become graph-visible and advance seen-state."
      })

    accepted_proposal = ChangesetStore.update!(proposal, %{status: "accepted"})

    op =
      ChangesetStore.insert!(ProposalOp, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        position: 0,
        op_type: "record_event",
        payload: %{
          "authored_output_id" => authored_output.output_id,
          "event" => "verified_authoring"
        },
        evidence_refs: ChangesetStore.evidence_maps(authored_output.evidence_refs),
        confidence: ChangesetStore.decimal(1.0),
        status: "pending"
      })

    committed_op =
      ChangesetStore.update!(op, %{
        status: "committed",
        committed_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })

    commit =
      ChangesetStore.insert!(GraphCommit, %{
        tenant_id: state.tenant_id,
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id,
        commit_type: "verified_authoring",
        committed_by_type: "authoring_verifier",
        committed_by_id: authored_output.user_id,
        evidence_refs: ChangesetStore.evidence_maps(authored_output.evidence_refs),
        confidence: ChangesetStore.decimal(1.0)
      })

    state =
      state
      |> State.append(:proposal_decisions, decision)
      |> State.replace(:proposals, proposal.id, accepted_proposal)
      |> State.append(:proposal_ops, committed_op)
      |> State.append(:graph_commits, commit)
      |> State.audit(%{
        event: :graph_commit_created,
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id,
        commit_id: commit.id,
        op_type: "record_event"
      })
      |> add_evidence_refs("proposal_op", committed_op.id, authored_output.evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> add_evidence_refs("graph_commit", commit.id, authored_output.evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> insert_node(authored_output.output_id, "authored_output", authored_output.output_id, %{
        authored_output_id: output.id,
        proposal: accepted_proposal,
        op: committed_op,
        commit: commit,
        attrs: %{"user_id" => authored_output.user_id}
      })

    units =
      authored_output.sentence_evidence
      |> Enum.with_index()
      |> Enum.map(fn {unit, position} ->
        ChangesetStore.insert!(AuthoredOutputUnit, %{
          tenant_id: state.tenant_id,
          authored_output_id: output.id,
          position: position,
          unit_type: "bullet",
          content: unit.text,
          story_id: story_id_for_unit(state, unit),
          evidence_refs: ChangesetStore.evidence_maps(unit.evidence_refs),
          claim_refs: Enum.map(unit.claim_refs, &%{"claim_ref" => &1})
        })
      end)

    state = Enum.reduce(units, state, &State.append(&2, :authored_output_units, &1))

    Enum.reduce(units, state, fn unit, state ->
      state
      |> add_evidence_refs(
        "authored_output_unit",
        unit.id,
        evidence_labels(unit.evidence_refs),
        %{authored_output_id: output.id, authored_output_unit_id: unit.id}
      )
    end)
  end

  defp mark_seen!(state, seen_state, authored_output) do
    validate_authored_output_for_seen!(state, authored_output)

    if seen_state.user_id != authored_output.user_id,
      do: raise(ArgumentError, "seen-state actor must match authored output actor")

    output_id = State.source_id!(state, :authored_output, authored_output.output_id)

    Enum.reduce(authored_output.touched_story_keys, state, fn story_key, state ->
      story = Enum.find(state.stories, &(&1.story_key == story_key))
      seen = storage_seen_refs_for_story(state, authored_output, story_key)

      row =
        ChangesetStore.insert!(SeenState, %{
          tenant_id: state.tenant_id,
          user_id: seen_state.user_id,
          story_id: State.source_id!(state, :story, story_key),
          seen_story_version: story.version,
          last_authored_output_id: output_id,
          seen_at: story.updated_at_story
        })

      state = State.append(state, :seen_states, row)

      refs =
        Enum.map(seen.seen_input_refs, &{"input", &1}) ++
          Enum.map(seen.seen_claim_refs, &{"claim", &1}) ++
          [{"story", story_key}, {"authored_output", authored_output.output_id}]

      Enum.reduce(refs, state, fn {kind, ref}, state ->
        ref_row =
          ChangesetStore.insert!(SeenStateRef, %{
            tenant_id: state.tenant_id,
            seen_state_id: row.id,
            ref_kind: kind,
            ref_id: ref,
            authored_output_id: output_id
          })

        State.append(state, :seen_state_refs, ref_row)
      end)
    end)
  end

  def validate_authored_output_for_seen!(state, authored_output) do
    if authored_output.verified != true,
      do: raise(ArgumentError, "cannot mark seen for unverified output")

    if Map.get(authored_output, :evidence_refs, []) == [],
      do: raise(ArgumentError, "authored output requires evidence refs")

    if Map.get(authored_output, :sentence_evidence, []) == [],
      do: raise(ArgumentError, "authored output requires grounded units")

    validate_actor_evidence_refs!(state, authored_output.user_id, authored_output.evidence_refs)

    Enum.each(authored_output.sentence_evidence, fn unit ->
      if Map.get(unit, :evidence_refs, []) == [] or Map.get(unit, :claim_refs, []) == [] do
        raise ArgumentError, "authored output requires grounded units"
      end

      validate_actor_evidence_refs!(state, authored_output.user_id, unit.evidence_refs)
    end)

    :ok
  end

  defp validate_actor_evidence_refs!(state, actor_id, evidence_refs) do
    Enum.each(evidence_refs, fn input_ref ->
      input = Enum.find(state.inputs, &(&1.fixture_id == input_ref))

      cond do
        input == nil ->
          raise ArgumentError, "authored output evidence ref is not a committed input"

        input_visible_to_actor?(input, actor_id) ->
          :ok

        true ->
          raise ArgumentError, "authored output evidence is not accessible to actor"
      end
    end)
  end

  defp bump_story(state, story_key, normalized) do
    update_story(state, story_key, fn story ->
      story
      |> Story.changeset(%{
        version: story.version + 1,
        updated_at_story: ChangesetStore.iso!(normalized.observed_at)
      })
      |> Ecto.Changeset.apply_changes()
    end)
  end

  defp update_story(state, story_key, fun) do
    stories =
      Enum.map(state.stories, fn
        %{story_key: ^story_key} = story -> fun.(story)
        story -> story
      end)

    %{state | stories: stories}
  end

  defp story_version(state, story_key) do
    state.stories
    |> Enum.find(&(&1.story_key == story_key))
    |> case do
      nil -> 0
      story -> story.version
    end
  end

  defp story_id(state, story_key) do
    Map.get(state.source_ids, {:story, story_key})
  end

  defp story_id_for_unit(state, unit) do
    story_key_for_evidence_refs(state, unit.evidence_refs)
    |> case do
      story_key when is_binary(story_key) -> State.source_id!(state, :story, story_key)
      nil -> nil
    end
  end

  defp storage_seen_refs_for_story(state, authored_output, story_key) do
    sentence_evidence =
      Enum.filter(authored_output.sentence_evidence, fn unit ->
        story_key_for_evidence_refs(state, unit.evidence_refs) == story_key
      end)

    %{
      seen_input_refs: sentence_evidence |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq(),
      seen_claim_refs: sentence_evidence |> Enum.flat_map(& &1.claim_refs) |> Enum.uniq()
    }
  end

  defp story_key_for_evidence_refs(state, evidence_refs) do
    input_node_ids =
      evidence_refs
      |> Enum.map(&Map.get(state.source_ids, {:node, &1}))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    state.edges
    |> Enum.find(fn edge ->
      MapSet.member?(input_node_ids, edge.from_node_id) and
        node_type(state, edge.to_node_id) == "story"
    end)
    |> case do
      nil -> nil
      edge -> node_by_id!(state, edge.to_node_id).node_key
    end
  end

  defp validate_accessible_evidence!(state, proposal) do
    refs =
      [proposal.evidence_refs | Enum.map(proposal.ops, &Map.get(&1, :evidence_refs, []))]
      |> List.flatten()
      |> Enum.uniq()

    Enum.each(refs, fn ref ->
      input = Enum.find(state.inputs, &(&1.fixture_id == ref))
      acl = input && input.acl

      cond do
        input == nil ->
          pending_input_op =
            Enum.find(proposal.ops, &(&1.op == :create_input and &1.input_id == ref))

          if pending_input_op do
            acl = pending_input_op.acl || %{"privacy" => "public"}

            unless acl["privacy"] == "public" or proposal.actor_id in (acl["participants"] || []) do
              raise ArgumentError, "proposal evidence is not accessible to actor"
            end
          else
            raise ArgumentError, "proposal evidence ref is not a committed or proposed input"
          end

        acl["privacy"] == "public" ->
          :ok

        proposal.actor_id in (acl["participants"] || []) ->
          :ok

        true ->
          raise ArgumentError, "proposal evidence is not accessible to actor"
      end
    end)
  end

  defp validate_edge_contract!(:watch_applies_to, "user_watch", "story"), do: :ok
  defp validate_edge_contract!(:part_of, "story", "story"), do: :ok

  defp validate_edge_contract!(type, from, to)
       when type in [:supports, :updates, :duplicates, :contradicts, :adds_color] and
              from in ["input", "claim"] and to in ["story", "claim"],
       do: :ok

  defp validate_edge_contract!(_type, _from, _to),
    do: raise(ArgumentError, "edge endpoint types do not match edge contract")

  defp node_type!(state, node_key) do
    node_id = State.source_id!(state, :node, node_key)
    Enum.find(state.soup_nodes, &(&1.id == node_id)).node_type
  end

  defp node_type(state, node_id), do: node_by_id!(state, node_id).node_type

  defp node_by_id!(state, node_id), do: Enum.find(state.soup_nodes, &(&1.id == node_id))

  defp evidence_input_refs(op) do
    Enum.map(op.evidence_refs, fn
      %{"input_ref" => ref} -> ref
      ref -> ref
    end)
  end

  defp evidence_labels(refs), do: Enum.map(refs, & &1["input_ref"])

  defp add_evidence_refs(state, subject_type, subject_id, refs, attrs) do
    validate_evidence_subject!(state, subject_type, subject_id)

    Enum.reduce(refs, state, fn ref, state ->
      input_id = Map.get(state.source_ids, {:input, ref})

      if input_id do
        row =
          ChangesetStore.insert!(EvidenceRef, %{
            tenant_id: state.tenant_id,
            subject_type: subject_type,
            subject_id: subject_id,
            input_id: input_id,
            soup_node_id: attrs[:soup_node_id],
            proposal_id: attrs[:proposal_id],
            proposal_op_id: attrs[:proposal_op_id],
            edge_id: attrs[:edge_id],
            conflict_id: attrs[:conflict_id],
            authored_output_id: attrs[:authored_output_id],
            authored_output_unit_id: attrs[:authored_output_unit_id],
            evidence_label: ref,
            evidence_hash: ChangesetStore.hash(ref)
          })

        State.append(state, :evidence_refs, row)
      else
        raise ArgumentError, "evidence ref #{inspect(ref)} is not a committed input"
      end
    end)
  end

  defp validate_evidence_subject!(state, subject_type, subject_id) do
    rows =
      case subject_type do
        "proposal" -> state.proposals
        "proposal_op" -> state.proposal_ops
        "graph_commit" -> state.graph_commits
        "story_fact_version" -> state.story_fact_versions
        "story_event" -> state.story_events
        "soup_node" -> state.soup_nodes
        "edge" -> state.edges
        "conflict" -> state.conflicts
        "authored_output" -> state.authored_outputs
        "authored_output_unit" -> state.authored_output_units
        _ -> raise ArgumentError, "evidence subject #{inspect(subject_type)} is not supported"
      end

    unless Enum.any?(rows, &(&1.id == subject_id)) do
      raise ArgumentError, "evidence subject #{subject_type}:#{subject_id} is not committed"
    end
  end

  defp input_visible_to_actor?(input, actor) do
    acl = input.acl || %{"privacy" => "public"}
    acl["privacy"] == "public" or actor in (acl["participants"] || [])
  end

  defp expected_artifacts!(path) do
    Enum.map(@expected_artifacts, fn artifact ->
      full_path = Path.join(path, artifact)

      if File.exists?(full_path),
        do: artifact,
        else: raise("missing expected artifact #{artifact}")
    end)
  end

  defp assert_prose_inputs!(inputs) do
    if Enum.any?(
         inputs,
         &String.match?(&1["body_text"], ~r/(^|\n)(FACT|COLOR|BACKGROUND|QUESTION)\s/)
       ) do
      raise ArgumentError, "fixture inputs must be prose, not trusted classifier labels"
    end
  end

  defp first_pass_input?(raw) do
    observed_at = raw["observed_at"] |> DateTime.from_iso8601() |> elem(1)
    cutoff = "2026-05-12T00:00:00-07:00" |> DateTime.from_iso8601() |> elem(1)
    DateTime.compare(observed_at, cutoff) == :lt
  end

  defp stringify_payload(%{} = payload),
    do: payload |> Enum.map(fn {k, v} -> {to_string(k), stringify_payload(v)} end) |> Map.new()

  defp stringify_payload(list) when is_list(list), do: Enum.map(list, &stringify_payload/1)
  defp stringify_payload(%MapSet{} = set), do: set |> MapSet.to_list() |> Enum.sort()
  defp stringify_payload(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp stringify_payload(value), do: value

  defp stringify_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp read_json!(path), do: path |> File.read!() |> Jason.decode!()
end
