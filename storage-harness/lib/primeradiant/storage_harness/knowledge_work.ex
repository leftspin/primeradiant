defmodule Primeradiant.StorageHarness.KnowledgeWork do
  @moduledoc false

  alias Primeradiant.StorageHarness.{
    AuthoredOutput,
    AuthoredOutputUnit,
    AgentRun,
    ChangesetStore,
    Edge,
    EvidenceRef,
    GraphCommit,
    Proposal,
    ProposalDecision,
    ProposalOp,
    SeenState,
    SeenStateRef,
    SoupNode,
    StoryReaderDelta,
    State,
    Watch
  }

  def attach_watches(%State{} = state, watch_attrs, actor_id \\ "flynn")
      when is_list(watch_attrs) do
    Enum.reduce(watch_attrs, state, &attach_watch(&2, &1, actor_id))
  end

  def attach_watch(%State{} = state, attrs, actor_id \\ "flynn") when is_map(attrs) do
    attrs = normalize_keys(attrs)
    watch_key = attrs["watch_key"]

    watch =
      ChangesetStore.insert!(Watch, %{
        tenant_id: state.tenant_id,
        user_id: actor_id,
        watch_key: watch_key,
        intent: attrs["intent"],
        priority: attrs["priority"] || 0,
        match_any: attrs["match_any"] || [],
        filters: attrs["filters"] || %{},
        status: "active",
        attrs: %{"source_mode" => "manual_real_watch_v1"}
      })

    state =
      state
      |> State.append(:watches, watch)
      |> State.put_source_id(:watch, watch_key, watch.id)

    state.stories
    |> Enum.filter(&story_visible_to_actor?(state, &1, actor_id))
    |> Enum.filter(&watch_matches_story?(watch, &1))
    |> Enum.reduce(state, fn story, acc ->
      commit_watch_attachment(acc, watch, story, actor_id)
    end)
  end

  def record_verified_delta(%State{} = state, user_id \\ "flynn", opts \\ []) do
    {state, agent_run} = ensure_delta_agent_run(state, user_id)
    output = render_delta(state, user_id) |> Map.put(:agent_run_key, agent_run.agent_run_key)

    if output.bullets == [] do
      {:ok, state, output}
    else
      advance_seen? =
        Keyword.get(opts, :advance_seen?, true) and
          Enum.any?(output.touched_story_keys, &unseen_story?(state, user_id, &1))

      state =
        if Keyword.get(opts, :advance_seen?, true) do
          state
          |> record_output!(output)
          |> record_story_reader_deltas!(output, agent_run)
          |> maybe_mark_seen!(output, advance_seen?)
        else
          record_story_reader_deltas!(state, output, agent_run)
        end

      {:ok, state, output}
    end
  end

  defp maybe_mark_seen!(state, output, true), do: mark_seen!(state, output)
  defp maybe_mark_seen!(state, _output, false), do: state

  defp record_story_reader_deltas!(state, output, agent_run) do
    Enum.reduce(output.touched_story_keys, state, fn story_key, acc ->
      story = Enum.find(acc.stories, &(&1.story_key == story_key))
      card = story && current_story_card_version(acc, story.id)

      if story && card do
        seen =
          Enum.find(acc.seen_states, &(&1.user_id == output.user_id and &1.story_id == story.id))

        bullet_rows = Enum.filter(output.sentence_evidence, &(&1.story_key == story_key))

        delta =
          ChangesetStore.insert!(StoryReaderDelta, %{
            tenant_id: acc.tenant_id,
            user_id: output.user_id,
            story_id: story.id,
            seen_state_id: seen && seen.id,
            prior_seen_story_version: if(seen, do: seen.seen_story_version, else: 0),
            prior_seen_card_version_id: prior_seen_card_version_id(acc, story.id, seen),
            current_story_version: story.version,
            current_card_version_id: card.id,
            material_unseen_deltas: material_delta_rows(bullet_rows),
            nonmaterial_exclusions: nonmaterial_delta_rows(bullet_rows),
            producing_agent_run_id: agent_run.id,
            evidence_refs: output.evidence_refs,
            provenance_refs: [card.field_provenance_manifest_id]
          })

        State.append(acc, :story_reader_deltas, delta)
      else
        acc
      end
    end)
  end

  defp ensure_delta_agent_run(state, user_id) do
    key = "agent-run:flynn-seen-delta.v1.t1274:#{length(state.agent_runs) + 1}"

    run =
      ChangesetStore.insert!(AgentRun, %{
        tenant_id: state.tenant_id,
        agent_run_key: key,
        agent_type: "flynn_seen_delta",
        prompt_version: "flynn-seen-delta.v1.t1274",
        model: "soup-native-delta",
        scope: %{
          "user_id" => user_id,
          "operation_family" => "flynn_relative_seen_delta",
          "prior_seen_states" => Enum.map(state.seen_states, & &1.id),
          "story_count" => length(state.stories)
        },
        status: "succeeded",
        trace_id: "trace:#{key}",
        started_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
        ended_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })

    {state |> State.append(:agent_runs, run) |> State.put_source_id(:agent_run, key, run.id), run}
  end

  def render_delta(%State{} = state, user_id \\ "flynn") do
    stories =
      state.stories
      |> Enum.filter(&story_visible_to_actor?(state, &1, user_id))
      |> Enum.filter(&story_changed_for_actor?(state, &1, user_id))
      |> Enum.sort_by(&story_relevance(state, &1, user_id), :desc)

    bullet_records = Enum.flat_map(stories, &story_bullets(state, &1, user_id))
    bullet_points = Enum.map(bullet_records, & &1.text)
    output_id = "real-authored-output:#{length(state.authored_outputs) + 1}"

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
      evidence_packet: %{
        "story_evidence" =>
          Map.new(bullet_records, &{&1.story_key, ChangesetStore.evidence_maps(&1.evidence_refs)}),
        "evidence_refs" => ChangesetStore.evidence_maps(evidence_refs(bullet_records))
      },
      sentence_evidence: bullet_records,
      evidence_refs: evidence_refs(bullet_records),
      touched_story_keys: stories |> Enum.map(& &1.story_key) |> Enum.uniq(),
      verified: verified_output?(bullet_records)
    }
  end

  defp commit_watch_attachment(state, watch, story, actor_id) do
    story_key = story.story_key
    watch_key = watch.watch_key

    if watch_attached?(state, watch_key, story_key) do
      state
    else
      evidence_refs = latest_story_input_refs(state, story, actor_id) |> Enum.take(1)
      proposal_key = "real-watch-proposal:#{watch_key}:#{story_key}"

      proposal =
        ChangesetStore.insert!(Proposal, %{
          tenant_id: state.tenant_id,
          proposal_key: proposal_key,
          agent_run_id: State.source_id!(state, :agent_run, "agent-run:manual-real-ingest-v1"),
          actor_id: actor_id,
          story_id: story.id,
          fixture_id: nil,
          classification: "attach_watch",
          confidence: ChangesetStore.decimal(0.94),
          rationale: "User watch terms match committed story evidence.",
          status: "pending"
        })

      op =
        ChangesetStore.insert!(ProposalOp, %{
          tenant_id: state.tenant_id,
          proposal_id: proposal.id,
          position: 0,
          op_type: "attach_watch",
          payload: %{
            "watch_key" => watch_key,
            "story_key" => story_key,
            "edge_type" => "watch_applies_to"
          },
          evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
          confidence: ChangesetStore.decimal(0.94),
          status: "pending"
        })

      decision =
        ChangesetStore.insert!(ProposalDecision, %{
          tenant_id: state.tenant_id,
          proposal_id: proposal.id,
          from_status: "pending",
          to_status: "accepted",
          actor_type: "arbiter",
          actor_id: actor_id,
          evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
          confidence: ChangesetStore.decimal(0.94),
          rationale: "Watch attachment accepted for visible story evidence."
        })

      accepted_proposal = ChangesetStore.update!(proposal, %{status: "accepted"})

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
          commit_type: "attach_watch",
          committed_by_type: "arbiter",
          committed_by_id: actor_id,
          evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
          confidence: ChangesetStore.decimal(0.94)
        })

      state
      |> State.append(:proposals, accepted_proposal)
      |> State.put_source_id(:proposal, proposal_key, proposal.id)
      |> State.append(:proposal_ops, committed_op)
      |> State.append(:proposal_decisions, decision)
      |> State.append(:graph_commits, commit)
      |> State.audit(%{event: :proposal_submitted, proposal_id: proposal.id, status: "pending"})
      |> State.audit(%{
        event: :proposal_decided,
        proposal_id: proposal.id,
        from_status: "pending",
        to_status: "accepted"
      })
      |> State.audit(%{
        event: :graph_commit_created,
        proposal_id: proposal.id,
        proposal_op_id: committed_op.id,
        commit_id: commit.id,
        op_type: "attach_watch"
      })
      |> add_evidence_refs("proposal", accepted_proposal.id, evidence_refs, %{
        proposal_id: accepted_proposal.id
      })
      |> add_evidence_refs("proposal_op", committed_op.id, evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> add_evidence_refs("graph_commit", commit.id, evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> insert_watch_node(watch, accepted_proposal, committed_op, commit, evidence_refs)
      |> insert_watch_edge(
        watch_key,
        story_key,
        accepted_proposal,
        committed_op,
        commit,
        evidence_refs
      )
    end
  end

  defp insert_watch_node(state, watch, proposal, op, commit, evidence_refs) do
    if Map.has_key?(state.source_ids, {:node, watch.watch_key}) do
      state
    else
      node =
        ChangesetStore.insert!(SoupNode, %{
          tenant_id: state.tenant_id,
          node_key: watch.watch_key,
          node_type: "user_watch",
          title: watch.intent,
          state: "active",
          watch_id: watch.id,
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          confidence: op.confidence,
          attrs: %{"match_any" => watch.match_any}
        })

      state
      |> State.append(:soup_nodes, node)
      |> State.put_source_id(:node, watch.watch_key, node.id)
      |> add_evidence_refs("soup_node", node.id, evidence_refs, %{
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        soup_node_id: node.id
      })
    end
  end

  defp insert_watch_edge(state, watch_key, story_key, proposal, op, commit, evidence_refs) do
    edge =
      ChangesetStore.insert!(Edge, %{
        tenant_id: state.tenant_id,
        from_node_id: State.source_id!(state, :node, watch_key),
        to_node_id: State.source_id!(state, :node, story_key),
        edge_type: "watch_applies_to",
        status: "committed",
        confidence: op.confidence,
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        attrs: %{}
      })

    state
    |> State.append(:edges, edge)
    |> add_evidence_refs("edge", edge.id, evidence_refs, %{
      proposal_id: proposal.id,
      proposal_op_id: op.id,
      edge_id: edge.id
    })
  end

  defp record_output!(state, authored_output) do
    validate_output!(state, authored_output)

    story_id =
      authored_output.touched_story_keys
      |> List.first()
      |> then(&if(&1, do: State.source_id!(state, :story, &1)))

    output =
      ChangesetStore.insert!(AuthoredOutput, %{
        tenant_id: state.tenant_id,
        user_id: authored_output.user_id,
        story_id: story_id,
        output_type: "briefing",
        content: authored_output.text,
        evidence_packet: authored_output.evidence_packet,
        verified: true,
        story_version: 0,
        status: "recorded"
      })

    state =
      state
      |> State.append(:authored_outputs, output)
      |> State.put_source_id(:authored_output, authored_output.output_id, output.id)

    authoring_proposal_key = "real-proposal:#{authored_output.output_id}"

    proposal =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: authoring_proposal_key,
        agent_run_id: State.source_id!(state, :agent_run, authored_output.agent_run_key),
        actor_id: authored_output.user_id,
        story_id: story_id,
        fixture_id: nil,
        classification: "verified_authoring",
        confidence: ChangesetStore.decimal(1.0),
        rationale: "Verified real-source delta recorded with sentence-level evidence.",
        status: "pending"
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
      |> State.append(:proposals, accepted_proposal)
      |> State.put_source_id(:proposal, authoring_proposal_key, proposal.id)
      |> State.append(:proposal_decisions, decision)
      |> State.append(:proposal_ops, committed_op)
      |> State.append(:graph_commits, commit)
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
      |> State.audit(%{
        event: :graph_commit_created,
        proposal_id: proposal.id,
        proposal_op_id: committed_op.id,
        commit_id: commit.id,
        op_type: "record_event"
      })
      |> add_evidence_refs("proposal", accepted_proposal.id, authored_output.evidence_refs, %{
        proposal_id: accepted_proposal.id
      })
      |> add_evidence_refs("proposal_op", committed_op.id, authored_output.evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> add_evidence_refs("graph_commit", commit.id, authored_output.evidence_refs, %{
        proposal_id: accepted_proposal.id,
        proposal_op_id: committed_op.id
      })
      |> insert_authored_output_node(
        authored_output,
        output,
        accepted_proposal,
        committed_op,
        commit
      )

    authored_output.sentence_evidence
    |> Enum.with_index()
    |> Enum.reduce(state, fn {unit, position}, acc ->
      row =
        ChangesetStore.insert!(AuthoredOutputUnit, %{
          tenant_id: acc.tenant_id,
          authored_output_id: output.id,
          position: position,
          unit_type: "bullet",
          content: unit.text,
          story_id: State.source_id!(acc, :story, unit.story_key),
          evidence_refs: ChangesetStore.evidence_maps(unit.evidence_refs),
          claim_refs: Enum.map(unit.claim_refs, &%{"claim_ref" => &1})
        })

      acc
      |> State.append(:authored_output_units, row)
      |> add_evidence_refs("authored_output_unit", row.id, unit.evidence_refs, %{
        authored_output_id: output.id,
        authored_output_unit_id: row.id
      })
    end)
  end

  defp insert_authored_output_node(state, authored_output, output, proposal, op, commit) do
    node =
      ChangesetStore.insert!(SoupNode, %{
        tenant_id: state.tenant_id,
        node_key: authored_output.output_id,
        node_type: "authored_output",
        title: authored_output.output_id,
        state: "active",
        authored_output_id: output.id,
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        confidence: op.confidence,
        attrs: %{"user_id" => authored_output.user_id}
      })

    state
    |> State.append(:soup_nodes, node)
    |> State.put_source_id(:node, authored_output.output_id, node.id)
    |> add_evidence_refs("soup_node", node.id, authored_output.evidence_refs, %{
      proposal_id: proposal.id,
      proposal_op_id: op.id,
      soup_node_id: node.id
    })
  end

  defp mark_seen!(state, authored_output) do
    validate_output!(state, authored_output)
    output_id = State.source_id!(state, :authored_output, authored_output.output_id)

    Enum.reduce(authored_output.touched_story_keys, state, fn story_key, acc ->
      story = Enum.find(acc.stories, &(&1.story_key == story_key))

      row =
        case Enum.find(
               acc.seen_states,
               &(&1.user_id == authored_output.user_id and &1.story_id == story.id)
             ) do
          nil ->
            ChangesetStore.insert!(SeenState, %{
              tenant_id: acc.tenant_id,
              user_id: authored_output.user_id,
              story_id: story.id,
              seen_story_version: story.version,
              last_authored_output_id: output_id,
              seen_at: story.updated_at_story
            })

          existing ->
            ChangesetStore.update!(existing, %{
              seen_story_version: story.version,
              last_authored_output_id: output_id,
              seen_at: story.updated_at_story
            })
        end

      acc =
        if Enum.any?(acc.seen_states, &(&1.id == row.id)),
          do: State.replace(acc, :seen_states, row.id, row),
          else: State.append(acc, :seen_states, row)

      refs =
        seen_refs_for_output(authored_output, story_key) ++
          [{"story", story_key}, {"authored_output", authored_output.output_id}]

      Enum.reduce(refs, acc, fn {kind, ref}, state ->
        append_seen_ref(state, row.id, output_id, kind, ref)
      end)
    end)
  end

  defp append_seen_ref(state, seen_state_id, output_id, kind, ref) do
    if Enum.any?(
         state.seen_state_refs,
         &(&1.seen_state_id == seen_state_id and &1.ref_kind == kind and &1.ref_id == ref)
       ) do
      state
    else
      row =
        ChangesetStore.insert!(SeenStateRef, %{
          tenant_id: state.tenant_id,
          seen_state_id: seen_state_id,
          ref_kind: kind,
          ref_id: ref,
          authored_output_id: output_id
        })

      State.append(state, :seen_state_refs, row)
    end
  end

  defp story_bullets(state, story, user_id) do
    unseen_delta_events = unseen_delta_events(state, story, user_id)

    events =
      case unseen_delta_events do
        [] -> [latest_visible_story_event(state, story, user_id)]
        events -> events
      end

    Enum.map(events, &story_bullet(state, story, user_id, &1))
  end

  defp story_bullet(state, story, user_id, event) do
    facts = delta_fact_pairs(state, story, user_id, event)
    evidence_refs = bullet_evidence_refs(state, story, user_id, event)
    prefix = if watched_story?(state, story.story_key, user_id), do: "[watch] ", else: ""

    %{
      story_key: story.story_key,
      text:
        "#{prefix}#{story.title}: #{why_text(event)}. Evidence: #{Enum.join(evidence_refs, ", ")}#{fact_text(facts)}",
      evidence_refs: evidence_refs,
      claim_refs: claim_refs_for_event(state, story, event, facts),
      classification: event && event.classification
    }
  end

  defp fact_text([]), do: ""

  defp fact_text(facts) do
    facts
    |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
    |> Enum.join(", ")
    |> then(&" (#{&1})")
  end

  defp delta_fact_pairs(state, story, user_id, latest_event) do
    facts =
      if latest_event && latest_event.changed_facts != %{} do
        latest_event.changed_facts
      else
        story.structural_facts
      end

    facts
    |> Enum.filter(fn {key, _value} ->
      state.story_fact_versions
      |> Enum.filter(&(&1.story_id == story.id and &1.fact_key == key))
      |> Enum.any?(fn fact ->
        input = Enum.find(state.inputs, &(&1.id == fact.input_id))
        input_visible_to_actor?(input, user_id)
      end)
    end)
    |> Enum.sort()
    |> Enum.take(4)
  end

  defp claim_refs_for_facts(state, story, facts) do
    facts
    |> Enum.map(fn {key, _value} -> "claim:#{story.story_key}:#{key}" end)
    |> Enum.filter(&Map.has_key?(state.source_ids, {:node, &1}))
  end

  defp claim_refs_for_event(state, story, event, facts) when event != nil do
    if material_event?(event), do: claim_refs_for_facts(state, story, facts), else: []
  end

  defp claim_refs_for_event(state, story, nil, facts),
    do: claim_refs_for_facts(state, story, facts)

  defp story_changed_for_actor?(state, story, user_id) do
    seen_inputs = seen_input_refs(state, story, user_id) |> MapSet.new()
    authored_inputs = authored_input_refs(state, user_id) |> MapSet.new()
    seen_version = seen_story_version(state, story, user_id)

    state
    |> visible_story_events(story, user_id)
    |> Enum.any?(fn event ->
      input_ref = input_ref(state, event.input_id)

      not MapSet.member?(seen_inputs, input_ref) and
        not MapSet.member?(authored_inputs, input_ref) and event.story_version >= seen_version and
        (seen_version == 0 or delta_visible_event?(event))
    end)
  end

  defp material_event?(event),
    do: event.classification in ["split", "attach", "conflict"] and event.changed_facts != %{}

  defp nonmaterial_exclusion_event?(event),
    do: event.classification in ["duplicate", "no_op", "stale", "color"]

  defp delta_visible_event?(event),
    do: material_event?(event) or nonmaterial_exclusion_event?(event)

  defp latest_visible_story_event(state, story, user_id) do
    events = visible_story_events(state, story, user_id)
    Enum.find(Enum.reverse(events), &material_event?/1) || List.last(events)
  end

  defp unseen_delta_events(state, story, user_id) do
    seen_inputs = seen_input_refs(state, story, user_id) |> MapSet.new()
    authored_inputs = authored_input_refs(state, user_id) |> MapSet.new()
    seen_version = seen_story_version(state, story, user_id)

    if seen_version == 0 do
      []
    else
      state
      |> visible_story_events(story, user_id)
      |> Enum.filter(fn event ->
        input_ref = input_ref(state, event.input_id)

        not MapSet.member?(seen_inputs, input_ref) and
          not MapSet.member?(authored_inputs, input_ref) and event.story_version >= seen_version and
          delta_visible_event?(event)
      end)
    end
  end

  defp authored_input_refs(state, user_id) do
    output_ids =
      state.authored_outputs
      |> Enum.filter(&(&1.user_id == user_id))
      |> MapSet.new(& &1.id)

    state.authored_output_units
    |> Enum.filter(&MapSet.member?(output_ids, &1.authored_output_id))
    |> Enum.flat_map(&input_refs_from_unit/1)
    |> Enum.uniq()
  end

  defp input_refs_from_unit(unit) do
    Enum.flat_map(unit.evidence_refs || [], fn
      %{"input_ref" => ref} -> [ref]
      %{input_ref: ref} -> [ref]
      _ -> []
    end)
  end

  defp bullet_evidence_refs(state, story, user_id, latest_event) do
    cond do
      latest_event && delta_visible_event?(latest_event) ->
        [input_ref(state, latest_event.input_id)]

      true ->
        latest_story_input_refs(state, story, user_id) |> Enum.take(3)
    end
  end

  defp visible_story_events(state, story, user_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.filter(fn event ->
      state.inputs
      |> Enum.find(&(&1.id == event.input_id))
      |> input_visible_to_actor?(user_id)
    end)
  end

  defp story_visible_to_actor?(state, story, user_id) do
    state
    |> latest_story_input_refs(story, user_id)
    |> Enum.any?()
  end

  defp latest_story_input_refs(state, story, user_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.reverse()
    |> Enum.map(fn event -> Enum.find(state.inputs, &(&1.id == event.input_id)) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(&input_visible_to_actor?(&1, user_id))
    |> Enum.map(&input_ref/1)
    |> Enum.uniq()
  end

  defp seen_input_refs(state, story, user_id) do
    state.seen_states
    |> Enum.filter(&(&1.user_id == user_id and &1.story_id == story.id))
    |> Enum.flat_map(fn seen ->
      state.seen_state_refs
      |> Enum.filter(&(&1.seen_state_id == seen.id and &1.ref_kind == "input"))
      |> Enum.map(& &1.ref_id)
    end)
  end

  defp seen_story_version(state, story, user_id) do
    state.seen_states
    |> Enum.find(&(&1.user_id == user_id and &1.story_id == story.id))
    |> case do
      nil -> 0
      seen -> seen.seen_story_version
    end
  end

  defp unseen_story?(state, user_id, story_key) do
    story = Enum.find(state.stories, &(&1.story_key == story_key))
    story && seen_story_version(state, story, user_id) == 0
  end

  defp seen_refs_for_output(authored_output, story_key) do
    input_refs =
      authored_output.sentence_evidence
      |> Enum.filter(&(&1.story_key == story_key))
      |> Enum.flat_map(& &1.evidence_refs)
      |> Enum.uniq()
      |> Enum.map(&{"input", &1})

    claim_refs =
      authored_output.sentence_evidence
      |> Enum.filter(&(&1.story_key == story_key))
      |> Enum.flat_map(& &1.claim_refs)
      |> Enum.uniq()
      |> Enum.map(&{"claim", &1})

    input_refs ++ claim_refs
  end

  defp watch_matches_story?(watch, story) do
    haystack =
      ([story.story_key, story.title] ++
         story.topic_tokens ++
         Map.keys(story.structural_facts) ++
         Map.values(story.structural_facts))
      |> Enum.map(&String.downcase(to_string(&1)))

    watch.match_any
    |> Enum.map(&String.downcase/1)
    |> Enum.any?(fn term -> Enum.any?(haystack, &String.contains?(&1, term)) end)
  end

  defp story_relevance(state, story, user_id) do
    base = story.version * 10 + map_size(story.structural_facts) * 5
    if watched_story?(state, story.story_key, user_id), do: base + 1000, else: base
  end

  defp watched_story?(state, story_key, user_id) do
    story_node_id = Map.get(state.source_ids, {:node, story_key})

    Enum.any?(state.edges, fn edge ->
      edge.edge_type == "watch_applies_to" and edge.to_node_id == story_node_id and
        watch_node_belongs_to_user?(state, edge.from_node_id, user_id)
    end)
  end

  defp watch_node_belongs_to_user?(state, node_id, user_id) do
    with node when not is_nil(node) <- Enum.find(state.soup_nodes, &(&1.id == node_id)),
         watch when not is_nil(watch) <- Enum.find(state.watches, &(&1.id == node.watch_id)) do
      watch.user_id == user_id
    else
      _ -> false
    end
  end

  defp watch_attached?(state, watch_key, story_key) do
    watch_node_id = Map.get(state.source_ids, {:node, watch_key})
    story_node_id = Map.get(state.source_ids, {:node, story_key})

    watch_node_id &&
      Enum.any?(state.edges, fn edge ->
        edge.edge_type == "watch_applies_to" and edge.from_node_id == watch_node_id and
          edge.to_node_id == story_node_id
      end)
  end

  defp why_text(%{classification: "conflict"}),
    do: "changed for Flynn because committed evidence partially contradicts prior story facts"

  defp why_text(%{classification: "color"}),
    do: "no material new update; source adds color only"

  defp why_text(%{classification: "stale"}), do: "mostly background; no material change was found"

  defp why_text(%{classification: "no_op"}),
    do: "no material new update; repeated known facts without new material delta"

  defp why_text(%{classification: "duplicate"}), do: "duplicate evidence; no material new update"

  defp why_text(%{classification: "attach"}),
    do: "changed for Flynn since last seen because committed facts moved"

  defp why_text(%{classification: "split"}), do: "new to Flynn as a distinct story identity"
  defp why_text(_event), do: "new to Flynn since last seen"

  defp verified_output?([]), do: true

  defp verified_output?(bullet_records) do
    Enum.all?(bullet_records, fn unit ->
      unit.evidence_refs != [] and
        (unit.claim_refs != [] or nonmaterial_classification?(unit.classification))
    end)
  end

  defp validate_output!(state, output) do
    if output.verified != true, do: raise(ArgumentError, "cannot mark seen for unverified output")

    if output.evidence_refs == [],
      do: raise(ArgumentError, "authored output requires evidence refs")

    if output.sentence_evidence == [],
      do: raise(ArgumentError, "authored output requires grounded units")

    Enum.each(output.sentence_evidence, fn unit ->
      if unit.evidence_refs == [] or
           (unit.claim_refs == [] and not nonmaterial_classification?(unit.classification)),
         do: raise(ArgumentError, "authored output requires grounded units")

      validate_actor_evidence_refs!(state, output.user_id, unit.evidence_refs)
    end)
  end

  defp nonmaterial_classification?(classification),
    do: classification in ["duplicate", "no_op", "stale", "color"]

  defp current_story_card_version(state, story_id) do
    state.story_card_versions
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.sort_by(& &1.card_version, :desc)
    |> List.first()
  end

  defp prior_seen_card_version_id(_state, _story_id, nil), do: nil

  defp prior_seen_card_version_id(state, story_id, seen) do
    state.story_card_versions
    |> Enum.filter(&(&1.story_id == story_id and &1.story_version <= seen.seen_story_version))
    |> Enum.sort_by(& &1.card_version, :desc)
    |> List.first()
    |> case do
      nil -> nil
      card -> card.id
    end
  end

  defp material_delta_rows(rows) do
    rows
    |> Enum.reject(&nonmaterial_classification?(&1.classification))
    |> Enum.map(&delta_row/1)
  end

  defp nonmaterial_delta_rows(rows) do
    rows
    |> Enum.filter(&nonmaterial_classification?(&1.classification))
    |> Enum.map(&delta_row/1)
  end

  defp delta_row(row) do
    %{
      "story_key" => row.story_key,
      "text" => row.text,
      "classification" => row.classification,
      "evidence_refs" => row.evidence_refs,
      "claim_refs" => row.claim_refs
    }
  end

  defp add_evidence_refs(state, subject_type, subject_id, refs, attrs) do
    Enum.reduce(refs, state, fn ref, acc ->
      input_id = Map.get(acc.source_ids, {:input, ref})

      if is_nil(input_id),
        do: raise(ArgumentError, "evidence ref #{inspect(ref)} is not a committed input")

      row =
        ChangesetStore.insert!(EvidenceRef, %{
          tenant_id: acc.tenant_id,
          subject_type: subject_type,
          subject_id: subject_id,
          input_id: input_id,
          soup_node_id: attrs[:soup_node_id],
          proposal_id: attrs[:proposal_id],
          proposal_op_id: attrs[:proposal_op_id],
          edge_id: attrs[:edge_id],
          authored_output_id: attrs[:authored_output_id],
          authored_output_unit_id: attrs[:authored_output_unit_id],
          evidence_label: ref,
          evidence_hash: ChangesetStore.hash(ref)
        })

      State.append(acc, :evidence_refs, row)
    end)
  end

  defp validate_actor_evidence_refs!(state, actor_id, refs) do
    Enum.each(refs, fn ref ->
      input_id = Map.get(state.source_ids, {:input, ref})
      input = input_id && Enum.find(state.inputs, &(&1.id == input_id))

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

  defp input_visible_to_actor?(nil, _actor_id), do: false

  defp input_visible_to_actor?(input, actor_id) do
    acl = input.acl || %{"privacy" => "public"}
    acl["privacy"] == "public" or actor_id in (acl["participants"] || [])
  end

  defp evidence_refs(bullet_records),
    do: bullet_records |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq()

  defp input_ref(input), do: "#{input.source_type}:#{input.external_id}"

  defp input_ref(state, input_id) do
    state.inputs
    |> Enum.find(&(&1.id == input_id))
    |> input_ref()
  end

  defp normalize_keys(value) when is_map(value),
    do: Map.new(value, fn {key, value} -> {to_string(key), normalize_keys(value)} end)

  defp normalize_keys(value) when is_list(value), do: Enum.map(value, &normalize_keys/1)
  defp normalize_keys(value), do: value
end
