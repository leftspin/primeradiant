defmodule Primeradiant.StorageHarness.RealIngestion do
  @moduledoc false

  alias Primeradiant.Extraction.{EnvelopeAdapter, ProductionExtractor}
  alias Primeradiant.Ingestion.{Admission, RealNormalizer}
  alias Primeradiant.StoryIdentity.{Candidates, Decider}

  alias Primeradiant.StorageHarness.{
    AgentRun,
    ChangesetStore,
    Conflict,
    Edge,
    EvidenceRef,
    GraphCommit,
    Proposal,
    ProposalDecision,
    ProposalOp,
    SoupNode,
    State,
    Story,
    StoryEvent,
    StoryFactVersion
  }

  def ingest_items(items, actor_id \\ "flynn") when is_list(items) do
    tenant_id = tenant_id_for_items!(items)

    source_mode = source_mode(List.first(items))
    agent_run_key = agent_run_key(source_mode)

    state =
      State.new(user_id: actor_id, tenant_id: tenant_id)
      |> insert_agent_run(agent_run_key, %{
        "source_mode" => source_mode
      })

    {state, decisions} =
      items
      |> Enum.sort_by(
        &{to_string(&1[:observed_at] || &1["observed_at"]),
         to_string(&1[:retrieved_at] || &1["retrieved_at"]),
         to_string(&1[:external_id] || &1["external_id"])}
      )
      |> Enum.reduce({state, []}, fn item, {state, decisions} ->
        {state, decision} = ingest_item(state, item, actor_id)
        {state, decisions ++ [decision]}
      end)

    {:ok, %{state | decisions: decisions},
     %{
       source_behavior: String.to_atom(source_mode),
       decisions: decisions,
       inputs: length(state.inputs),
       proposals: length(state.proposals),
       graph_commits: length(state.graph_commits)
     }}
  end

  defp tenant_id_for_items!(items) do
    tenant_ids =
      items
      |> Enum.map(&(&1[:tenant_id] || &1["tenant_id"]))
      |> Enum.uniq()

    case tenant_ids do
      [tenant_id] when is_binary(tenant_id) -> tenant_id
      [nil] -> raise ArgumentError, "real source item requires tenant_id"
      _ -> raise ArgumentError, "real ingestion batch must use one tenant_id"
    end
  end

  defp source_mode(item),
    do: to_string(item[:source_mode] || item["source_mode"] || "manual_real_ingest_v1")

  defp agent_run_key(source_mode) when is_binary(source_mode),
    do: "agent-run:#{String.replace(source_mode, "_", "-")}"

  defp current_agent_run_key(state) do
    state.agent_runs
    |> List.first()
    |> Map.fetch!(:agent_run_key)
  end

  def ingest_item(%State{} = state, item, actor_id \\ "flynn") do
    {state, input, input_ref} = Admission.admit_source_item(state, item)
    {:ok, extraction_result} = ProductionExtractor.extract(input)
    envelope = EnvelopeAdapter.to_envelope(input, extraction_result)
    input = persist_normalized_input(input, envelope, extraction_result)
    state = State.replace(state, :inputs, input.id, input)
    candidates = Candidates.retrieve(state, envelope, actor_id)
    decision = Decider.decide(state, envelope, candidates) |> resolve_story_key_collision(state)

    state =
      if decision.status == :needs_more_evidence do
        submit_needs_more_evidence(state, decision, actor_id)
      else
        proposal = proposal_for_decision(state, decision, envelope, actor_id)
        submit_decide_and_commit(state, proposal, decision, envelope, actor_id)
      end

    {state, Map.put(decision, :input_ref, input_ref)}
  end

  defp persist_normalized_input(input, envelope, extraction_result) do
    ChangesetStore.update!(input, %{
      normalized:
        Map.merge(input.normalized, %{
          "real_envelope" => stringify(envelope),
          "production_extractor_v1" => stringify(extraction_result),
          "extractor_version" => envelope.provenance.extractor_version,
          "contract_version" => envelope.provenance.contract_version
        }),
      facts: RealNormalizer.facts(envelope),
      questions: Map.new(envelope.extracted.questions, &{&1.question_key, &1.text}),
      colors: Enum.map(envelope.extracted.colors, & &1.text),
      topic_tokens: envelope.extracted.topic_tokens
    })
  end

  defp insert_agent_run(state, key, scope) do
    row =
      ChangesetStore.insert!(AgentRun, %{
        tenant_id: state.tenant_id,
        agent_run_key: key,
        agent_type: scope["source_mode"],
        scope: scope,
        status: "succeeded"
      })

    state
    |> State.append(:agent_runs, row)
    |> State.put_source_id(:agent_run, key, row.id)
  end

  defp submit_needs_more_evidence(state, decision, actor_id) do
    proposal =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: proposal_key(decision),
        agent_run_id: State.source_id!(state, :agent_run, current_agent_run_key(state)),
        actor_id: actor_id,
        classification: Atom.to_string(decision.decision_type),
        confidence: ChangesetStore.decimal(decision.confidence),
        rationale: decision.rationale,
        status: "pending"
      })

    decision_row =
      ChangesetStore.insert!(ProposalDecision, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        from_status: "pending",
        to_status: "needs_more_evidence",
        actor_type: "arbiter",
        actor_id: actor_id,
        evidence_refs: ChangesetStore.evidence_maps(decision.evidence_refs),
        confidence: ChangesetStore.decimal(decision.confidence),
        rationale: decision.rationale
      })

    final_proposal = ChangesetStore.update!(proposal, %{status: "needs_more_evidence"})

    state
    |> State.append(:proposals, final_proposal)
    |> State.put_source_id(:proposal, proposal.proposal_key, proposal.id)
    |> State.append(:proposal_decisions, decision_row)
    |> add_evidence_refs("proposal", proposal.id, decision.evidence_refs, %{
      proposal_id: proposal.id
    })
  end

  defp proposal_for_decision(state, decision, envelope, actor_id) do
    story_id =
      case decision.candidate_story_key do
        nil -> nil
        story_key -> Map.get(state.source_ids, {:story, story_key})
      end

    %{
      key: proposal_key(decision),
      actor_id: actor_id,
      story_id: story_id,
      classification: Atom.to_string(decision.decision_type),
      confidence: decision.confidence,
      rationale: decision.rationale,
      evidence_refs: decision.evidence_refs,
      ops: ops_for_decision(decision, envelope)
    }
  end

  defp ops_for_decision(decision, envelope) do
    input_op = %{
      op: :create_input,
      input_ref: envelope.input_ref,
      title: envelope.title || envelope.external_id,
      acl: envelope.acl,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

    case decision.decision_type do
      :split ->
        [
          input_op,
          create_story_op(decision, envelope),
          attach_op(decision.proposed_story_key, envelope, :supports),
          merge_facts_op(decision, envelope),
          event_op(decision.proposed_story_key, decision, envelope)
        ]

      :part_of ->
        [
          input_op,
          create_story_op(decision, envelope),
          attach_op(decision.proposed_story_key, envelope, :supports),
          merge_facts_op(decision, envelope),
          part_of_op(decision),
          event_op(decision.proposed_story_key, decision, envelope)
        ]

      :attach ->
        [
          input_op,
          attach_op(decision.candidate_story_key, envelope, :updates),
          maybe_merge_facts_op(decision, envelope),
          maybe_questions_op(decision, envelope),
          event_op(decision.candidate_story_key, decision, envelope)
        ]
        |> Enum.reject(&is_nil/1)

      :conflict ->
        [
          input_op,
          attach_op(decision.candidate_story_key, envelope, :contradicts),
          conflict_op(decision, envelope),
          event_op(decision.candidate_story_key, decision, envelope)
        ]

      :no_op ->
        [
          input_op,
          attach_op(decision.candidate_story_key, envelope, :duplicates),
          event_op(decision.candidate_story_key, decision, envelope)
        ]

      :color ->
        [
          input_op,
          attach_op(decision.candidate_story_key, envelope, :adds_color),
          color_op(decision, envelope),
          event_op(decision.candidate_story_key, decision, envelope)
        ]

      :stale ->
        [
          input_op,
          background_op(decision, envelope),
          mark_state_op(decision, envelope),
          event_op(decision.candidate_story_key, decision, envelope)
        ]
    end
  end

  defp create_story_op(decision, envelope) do
    %{
      op: :create_story,
      story_key: decision.proposed_story_key,
      title: envelope.title || String.replace(decision.proposed_story_key, "-", " "),
      observed_at: envelope.observed_at,
      identity_anchors: decision.identity_anchors,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }
  end

  defp attach_op(story_key, envelope, edge_type) do
    %{
      op: :attach_input,
      story_key: story_key,
      input_ref: envelope.input_ref,
      edge_type: edge_type,
      evidence_refs: envelope_evidence_refs(envelope),
      confidence: 0.86
    }
  end

  defp merge_facts_op(decision, envelope),
    do: %{
      op: :merge_facts,
      story_key: decision.proposed_story_key,
      facts: RealNormalizer.facts(envelope),
      fact_scopes: RealNormalizer.fact_scopes(envelope),
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp maybe_merge_facts_op(decision, envelope) do
    if decision.changed_facts == %{},
      do: nil,
      else: %{
        op: :merge_facts,
        story_key: decision.candidate_story_key,
        facts: decision.changed_facts,
        fact_scopes: RealNormalizer.fact_scopes(envelope),
        evidence_refs: decision.evidence_refs,
        confidence: decision.confidence
      }
  end

  defp maybe_questions_op(decision, _envelope) do
    if decision.questions == %{},
      do: nil,
      else: %{
        op: :add_questions,
        story_key: decision.candidate_story_key,
        questions: decision.questions,
        evidence_refs: decision.evidence_refs,
        confidence: decision.confidence
      }
  end

  defp part_of_op(decision),
    do: %{
      op: :attach_story_part_of,
      child_story_key: decision.proposed_story_key,
      parent_story_key: decision.parent_story_key,
      edge_type: :part_of,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp conflict_op(decision, _envelope),
    do: %{
      op: :record_conflicts,
      story_key: decision.candidate_story_key,
      conflicts: decision.conflicting_facts,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp color_op(decision, _envelope),
    do: %{
      op: :append_colors,
      story_key: decision.candidate_story_key,
      colors: decision.colors,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp background_op(decision, envelope),
    do: %{
      op: :merge_background,
      story_key: decision.candidate_story_key,
      background: %{"last_check_in" => envelope.input_ref},
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp mark_state_op(decision, _envelope),
    do: %{
      op: :mark_state,
      story_key: decision.candidate_story_key,
      state: :background,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp event_op(story_key, decision, envelope),
    do: %{
      op: :record_event,
      story_key: story_key,
      classification: decision.decision_type,
      input_ref: envelope.input_ref,
      changed_facts: decision.changed_facts,
      evidence_refs: decision.evidence_refs,
      confidence: decision.confidence
    }

  defp submit_decide_and_commit(state, proposal, decision, envelope, actor_id) do
    validate_proposal!(state, proposal, decision)
    {state, proposal_row, op_rows} = submit_proposal(state, proposal)

    {state, proposal_row, op_rows} =
      decide_proposal(state, proposal_row, op_rows, proposal, actor_id)

    commit_ops(state, proposal_row, op_rows, envelope, actor_id)
  end

  defp submit_proposal(state, proposal) do
    proposal_row =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: proposal.key,
        agent_run_id: State.source_id!(state, :agent_run, current_agent_run_key(state)),
        actor_id: proposal.actor_id,
        story_id: proposal.story_id,
        fixture_id: nil,
        classification: proposal.classification,
        confidence: ChangesetStore.decimal(proposal.confidence),
        rationale: proposal.rationale,
        status: "pending"
      })

    state =
      state
      |> State.append(:proposals, proposal_row)
      |> State.put_source_id(:proposal, proposal.key, proposal_row.id)
      |> State.audit(%{
        event: :proposal_submitted,
        proposal_id: proposal_row.id,
        status: "pending"
      })
      |> add_evidence_refs("proposal", proposal_row.id, proposal.evidence_refs, %{
        proposal_id: proposal_row.id
      })

    {state, op_rows} =
      proposal.ops
      |> Enum.with_index()
      |> Enum.reduce({state, []}, fn {op, position}, {state, rows} ->
        row =
          ChangesetStore.insert!(ProposalOp, %{
            tenant_id: state.tenant_id,
            proposal_id: proposal_row.id,
            position: position,
            op_type: Atom.to_string(op.op),
            payload: stringify(op),
            evidence_refs: ChangesetStore.evidence_maps(op.evidence_refs),
            confidence: ChangesetStore.decimal(op.confidence),
            status: "pending"
          })

        state =
          state
          |> State.append(:proposal_ops, row)
          |> add_evidence_refs("proposal_op", row.id, op.evidence_refs, %{
            proposal_id: proposal_row.id,
            proposal_op_id: row.id
          })

        {state, rows ++ [{op, row}]}
      end)

    {state, proposal_row, op_rows}
  end

  defp decide_proposal(state, proposal_row, op_rows, proposal, actor_id) do
    decision_row =
      ChangesetStore.insert!(ProposalDecision, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal_row.id,
        from_status: "pending",
        to_status: "accepted",
        actor_type: "arbiter",
        actor_id: actor_id,
        evidence_refs: ChangesetStore.evidence_maps(proposal.evidence_refs),
        confidence: ChangesetStore.decimal(proposal.confidence),
        rationale: proposal.rationale
      })

    accepted_proposal = ChangesetStore.update!(proposal_row, %{status: "accepted"})

    accepted_ops =
      Enum.map(op_rows, fn {op, row} ->
        {op, ChangesetStore.update!(row, %{status: "accepted"})}
      end)

    state =
      state
      |> State.append(:proposal_decisions, decision_row)
      |> State.replace(:proposals, proposal_row.id, accepted_proposal)
      |> State.audit(%{
        event: :proposal_decided,
        proposal_id: accepted_proposal.id,
        from_status: "pending",
        to_status: "accepted"
      })

    state =
      Enum.reduce(accepted_ops, state, fn {_op, row}, acc ->
        State.replace(acc, :proposal_ops, row.id, row)
      end)

    {state, accepted_proposal, accepted_ops}
  end

  defp commit_ops(state, proposal_row, op_rows, envelope, actor_id) do
    Enum.reduce(op_rows, state, fn {op, op_row}, state ->
      committed_op =
        ChangesetStore.update!(op_row, %{
          status: "committed",
          committed_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
        })

      commit =
        ChangesetStore.insert!(GraphCommit, %{
          tenant_id: state.tenant_id,
          proposal_id: proposal_row.id,
          proposal_op_id: committed_op.id,
          commit_type: Atom.to_string(op.op),
          committed_by_type: "arbiter",
          committed_by_id: actor_id,
          evidence_refs: ChangesetStore.evidence_maps(op.evidence_refs),
          confidence: ChangesetStore.decimal(op.confidence)
        })

      state
      |> State.replace(:proposal_ops, committed_op.id, committed_op)
      |> State.append(:graph_commits, commit)
      |> State.audit(%{
        event: :graph_commit_created,
        proposal_id: proposal_row.id,
        proposal_op_id: committed_op.id,
        commit_id: commit.id,
        op_type: committed_op.op_type
      })
      |> add_evidence_refs("graph_commit", commit.id, op.evidence_refs, %{
        proposal_id: proposal_row.id,
        proposal_op_id: committed_op.id
      })
      |> apply_op(op, envelope, proposal_row, committed_op, commit)
    end)
  end

  defp apply_op(
         state,
         %{op: :create_input, input_ref: input_ref, title: title, acl: acl},
         _envelope,
         proposal,
         op,
         commit
       ) do
    input_id = State.source_id!(state, :input, input_ref)

    insert_node(state, input_ref, "input", title, %{
      input_id: input_id,
      proposal: proposal,
      op: op,
      commit: commit,
      attrs: %{"acl" => acl}
    })
  end

  defp apply_op(
         state,
         %{
           op: :create_story,
           story_key: story_key,
           title: title,
           observed_at: observed_at,
           identity_anchors: anchors
         },
         envelope,
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
        topic_tokens: envelope.extracted.topic_tokens,
        attrs: %{"identity_anchors" => anchors}
      })

    state
    |> State.append(:stories, story)
    |> State.put_source_id(:story, story_key, story.id)
    |> insert_node(story_key, "story", title, %{
      story_id: story.id,
      proposal: proposal,
      op: op,
      commit: commit,
      attrs: %{"identity_anchors" => anchors}
    })
  end

  defp apply_op(
         state,
         %{op: :attach_input, story_key: story_key, input_ref: input_ref, edge_type: edge_type},
         _envelope,
         proposal,
         op,
         commit
       ),
       do: insert_edge(state, input_ref, story_key, edge_type, proposal, op, commit)

  defp apply_op(
         state,
         %{op: :merge_facts, story_key: story_key, facts: facts} = merge_op,
         envelope,
         proposal,
         op,
         commit
       ) do
    state =
      update_story(state, story_key, fn story ->
        Story.changeset(story, %{
          structural_facts: Map.merge(story.structural_facts, stringify(facts)),
          last_material_at: envelope.observed_at
        })
        |> Ecto.Changeset.apply_changes()
      end)

    Enum.reduce(facts, state, fn {key, value}, acc ->
      claim_key = "claim:#{story_key}:#{key}"

      acc =
        insert_node(acc, claim_key, "claim", "#{key}=#{value}", %{
          proposal: proposal,
          op: op,
          commit: commit,
          attrs: %{"story_key" => story_key, "fact" => key, "value" => value}
        })

      fact =
        ChangesetStore.insert!(StoryFactVersion, %{
          tenant_id: acc.tenant_id,
          story_id: State.source_id!(acc, :story, story_key),
          claim_node_id: State.source_id!(acc, :node, claim_key),
          fact_key: to_string(key),
          fact_value: to_string(value),
          time_scope: Map.get(merge_op, :fact_scopes, %{}) |> Map.get(to_string(key), "current"),
          status: "current",
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          input_id: envelope.input_id,
          confidence: op.confidence,
          observed_at: envelope.observed_at
        })

      acc
      |> State.append(:story_fact_versions, fact)
      |> add_evidence_refs("story_fact_version", fact.id, op.evidence_refs, %{
        proposal_id: proposal.id,
        proposal_op_id: op.id
      })
    end)
  end

  defp apply_op(
         state,
         %{op: :record_conflicts, story_key: story_key, conflicts: conflicts},
         envelope,
         proposal,
         op,
         commit
       ) do
    Enum.reduce(conflicts, state, fn conflict, acc ->
      row =
        ChangesetStore.insert!(Conflict, %{
          tenant_id: acc.tenant_id,
          story_id: State.source_id!(acc, :story, story_key),
          fact_key: conflict.fact_key,
          prior_value: conflict.prior_value,
          incoming_value: conflict.incoming_value,
          status: conflict.proposed_status,
          input_id: envelope.input_id,
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          agent_run_id: State.source_id!(acc, :agent_run, current_agent_run_key(acc)),
          confidence: op.confidence
        })

      acc
      |> State.append(:conflicts, row)
      |> add_evidence_refs("conflict", row.id, op.evidence_refs, %{
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        conflict_id: row.id
      })
    end)
  end

  defp apply_op(
         state,
         %{op: :append_colors, story_key: story_key, colors: colors},
         envelope,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{colors: Enum.uniq(story.colors ++ colors)})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_event(story_key, "color", %{"colors" => colors}, envelope, proposal, op, commit)
  end

  defp apply_op(
         state,
         %{op: :add_questions, story_key: story_key, questions: questions},
         envelope,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{questions: Map.merge(story.questions, stringify(questions))})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_event(story_key, "add_questions", questions, envelope, proposal, op, commit)
  end

  defp apply_op(
         state,
         %{op: :merge_background, story_key: story_key, background: background},
         envelope,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{
        background_facts: Map.merge(story.background_facts, stringify(background))
      })
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_event(story_key, "stale", background, envelope, proposal, op, commit)
  end

  defp apply_op(
         state,
         %{op: :mark_state, story_key: story_key, state: story_state},
         envelope,
         proposal,
         op,
         commit
       ) do
    state
    |> update_story(story_key, fn story ->
      Story.changeset(story, %{state: Atom.to_string(story_state)})
      |> Ecto.Changeset.apply_changes()
    end)
    |> record_event(
      story_key,
      "mark_state",
      %{"state" => Atom.to_string(story_state)},
      envelope,
      proposal,
      op,
      commit
    )
  end

  defp apply_op(
         state,
         %{
           op: :attach_story_part_of,
           child_story_key: child,
           parent_story_key: parent,
           edge_type: edge_type
         },
         _envelope,
         proposal,
         op,
         commit
       ),
       do: insert_edge(state, child, parent, edge_type, proposal, op, commit)

  defp apply_op(
         state,
         %{
           op: :record_event,
           story_key: story_key,
           classification: classification,
           changed_facts: changed_facts
         },
         envelope,
         proposal,
         op,
         commit
       ),
       do:
         record_event(
           state,
           story_key,
           Atom.to_string(classification),
           changed_facts,
           envelope,
           proposal,
           op,
           commit
         )

  defp insert_node(state, node_key, node_type, title, opts) do
    if Map.has_key?(state.source_ids, {:node, node_key}) do
      state
    else
      node =
        ChangesetStore.insert!(SoupNode, %{
          tenant_id: state.tenant_id,
          node_key: node_key,
          node_type: node_type,
          title: title,
          state: "active",
          input_id: opts[:input_id],
          story_id: opts[:story_id],
          proposal_id: opts.proposal.id,
          proposal_op_id: opts.op.id,
          graph_commit_id: opts.commit.id,
          confidence: opts.op.confidence,
          attrs: opts[:attrs] || %{}
        })

      state
      |> State.append(:soup_nodes, node)
      |> State.put_source_id(:node, node_key, node.id)
      |> add_evidence_refs("soup_node", node.id, opts.op.evidence_refs, %{
        proposal_id: opts.proposal.id,
        proposal_op_id: opts.op.id,
        soup_node_id: node.id
      })
    end
  end

  defp insert_edge(state, from_key, to_key, edge_type, proposal, op, commit) do
    validate_edge!(edge_type, node_type!(state, from_key), node_type!(state, to_key))

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
    |> add_evidence_refs("edge", edge.id, op.evidence_refs, %{
      proposal_id: proposal.id,
      proposal_op_id: op.id,
      edge_id: edge.id
    })
  end

  defp record_event(
         state,
         story_key,
         classification,
         changed_facts,
         envelope,
         proposal,
         op,
         commit
       ) do
    event =
      ChangesetStore.insert!(StoryEvent, %{
        tenant_id: state.tenant_id,
        story_id: State.source_id!(state, :story, story_key),
        input_id: envelope.input_id,
        classification: classification,
        story_version: story_version(state, story_key),
        changed_facts: stringify(changed_facts),
        observed_at: envelope.observed_at,
        proposal_id: proposal.id,
        proposal_op_id: op.id,
        graph_commit_id: commit.id,
        confidence: op.confidence
      })

    state
    |> State.append(:story_events, event)
    |> bump_story(story_key, envelope)
    |> add_evidence_refs("story_event", event.id, op.evidence_refs, %{
      proposal_id: proposal.id,
      proposal_op_id: op.id
    })
  end

  defp add_evidence_refs(state, subject_type, subject_id, refs, attrs) do
    Enum.reduce(refs, state, fn ref, acc ->
      input_ref = evidence_input_ref(ref)
      label = stored_evidence_label(ref)
      input_id = Map.get(acc.source_ids, {:input, input_ref})

      if is_nil(input_id),
        do: raise(ArgumentError, "evidence ref #{inspect(ref)} is not a committed input")

      span = evidence_span(acc, input_id, ref)

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
          conflict_id: attrs[:conflict_id],
          span_start: span && span["span_start"],
          span_end: span && span["span_end"],
          evidence_label: label,
          evidence_hash: (span && span["text_hash"]) || ChangesetStore.hash(label)
        })

      State.append(acc, :evidence_refs, row)
    end)
  end

  defp validate_proposal!(state, proposal, decision) do
    if proposal.evidence_refs == [] or proposal.ops == [],
      do: raise(ArgumentError, "proposal requires evidence-backed ops")

    validate_accessible_evidence!(state, proposal.actor_id, proposal.evidence_refs)

    if decision.decision_type == :attach do
      candidate = Enum.find(state.stories, &(&1.story_key == decision.candidate_story_key))

      if candidate &&
           MapSet.disjoint?(
             MapSet.new(candidate.topic_tokens),
             MapSet.new(decision.identity_anchors)
           ) && decision.changed_facts == %{} do
        raise ArgumentError, "attach requires structural identity evidence"
      end
    end

    Enum.each(proposal.ops, fn op ->
      if op.evidence_refs == [], do: raise(ArgumentError, "op requires evidence refs")
      if op.confidence == nil, do: raise(ArgumentError, "op requires confidence")

      if Map.get(op, :edge_type) in [:related, :adjacent_to, :reframes, :mentions],
        do: raise(ArgumentError, "unsupported edge type")

      validate_accessible_evidence!(state, proposal.actor_id, op.evidence_refs)
    end)
  end

  defp validate_accessible_evidence!(state, actor_id, refs) do
    Enum.each(refs, fn ref ->
      input_ref = evidence_input_ref(ref)
      input_id = Map.get(state.source_ids, {:input, input_ref})
      input = input_id && Enum.find(state.inputs, &(&1.id == input_id))

      cond do
        input == nil ->
          raise ArgumentError, "evidence ref #{inspect(ref)} is not a committed input"

        not is_nil(evidence_id(ref)) and is_nil(evidence_span(state, input_id, ref)) ->
          raise ArgumentError,
                "evidence ref #{inspect(ref)} is not in the extractor evidence index"

        input_visible?(input, actor_id) ->
          :ok

        true ->
          raise ArgumentError, "proposal evidence is not accessible to actor"
      end
    end)
  end

  defp input_visible?(input, actor_id) do
    acl = input.acl || %{"privacy" => "public"}
    acl["privacy"] == "public" or actor_id in (acl["participants"] || [])
  end

  defp evidence_label(%{"input_ref" => ref}), do: ref
  defp evidence_label(ref), do: ref
  defp evidence_input_ref(%{"input_ref" => ref}), do: ref
  defp evidence_input_ref(ref), do: evidence_label(ref)

  defp stored_evidence_label(%{"input_ref" => input_ref, "evidence_id" => evidence_id}),
    do: "#{input_ref}##{evidence_id}"

  defp stored_evidence_label(%{"input_ref" => input_ref}), do: input_ref
  defp stored_evidence_label(ref), do: evidence_label(ref)
  defp evidence_id(%{"evidence_id" => evidence_id}), do: evidence_id
  defp evidence_id(_ref), do: nil

  defp evidence_span(state, input_id, ref) do
    with evidence_id when is_binary(evidence_id) <- evidence_id(ref),
         input when not is_nil(input) <- Enum.find(state.inputs, &(&1.id == input_id)),
         index when is_list(index) <-
           get_in(input.normalized, ["production_extractor_v1", "evidence_index"]) do
      Enum.find(index, &(&1["evidence_id"] == evidence_id))
    else
      _ -> nil
    end
  end

  defp resolve_story_key_collision(%{proposed_story_key: nil} = decision, _state), do: decision

  defp resolve_story_key_collision(decision, state) do
    existing = Enum.find(state.stories, &(&1.story_key == decision.proposed_story_key))

    cond do
      existing == nil ->
        decision

      decision.candidate_story_key == existing.story_key and
          not identity_mismatch?(decision.changed_facts, existing.structural_facts) ->
        %{
          decision
          | decision_type: :attach,
            candidate_story_id: existing.id,
            candidate_story_key: existing.story_key,
            proposed_story_key: nil,
            rationale: "Existing story key has compatible identity; attach without renaming."
        }

      true ->
        disambiguator =
          identity_disambiguator(decision.changed_facts, existing.structural_facts) ||
            decision.identity_anchors
            |> Enum.drop(2)
            |> Enum.find(&(&1 not in String.split(existing.story_key, "-")))
            |> case do
              nil -> "distinct"
              value -> value
            end

        %{
          decision
          | proposed_story_key: "#{decision.proposed_story_key}-#{disambiguator}",
            rationale:
              decision.rationale <>
                " Story key collision resolved with deterministic identity-anchor disambiguator."
        }
    end
  end

  defp identity_mismatch?(incoming, existing) do
    Enum.any?(identity_anchor_fact_keys(), fn key ->
      Map.has_key?(incoming, key) and Map.has_key?(existing, key) and
        incoming[key] != existing[key]
    end)
  end

  defp identity_disambiguator(incoming, existing) do
    Enum.find_value(identity_anchor_fact_keys(), fn key ->
      if Map.has_key?(incoming, key) and Map.has_key?(existing, key) and
           incoming[key] != existing[key] do
        incoming[key]
      end
    end)
  end

  defp identity_anchor_fact_keys, do: ~w(venue location actor affected_group group date)

  defp proposal_key(decision), do: "real-proposal:#{decision.input_ref}:#{decision.decision_type}"

  defp bump_story(state, story_key, envelope) do
    update_story(state, story_key, fn story ->
      Story.changeset(story, %{version: story.version + 1, updated_at_story: envelope.observed_at})
      |> Ecto.Changeset.apply_changes()
    end)
  end

  defp update_story(state, story_key, fun),
    do: %{
      state
      | stories:
          Enum.map(state.stories, fn
            %{story_key: ^story_key} = story -> fun.(story)
            story -> story
          end)
    }

  defp story_version(state, story_key),
    do: (Enum.find(state.stories, &(&1.story_key == story_key)) || %{version: 0}).version

  defp node_type!(state, node_key) do
    node_id = State.source_id!(state, :node, node_key)
    Enum.find(state.soup_nodes, &(&1.id == node_id)).node_type
  end

  defp validate_edge!(:part_of, "story", "story"), do: :ok

  defp validate_edge!(edge, "input", "story")
       when edge in [:supports, :updates, :duplicates, :contradicts, :adds_color], do: :ok

  defp validate_edge!(edge, _from, _to),
    do: raise(ArgumentError, "unsupported edge contract #{edge}")

  defp stringify(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp stringify(%_struct{} = value), do: value |> Map.from_struct() |> stringify()

  defp stringify(value) when is_map(value),
    do: Map.new(value, fn {key, value} -> {to_string(key), stringify(value)} end)

  defp stringify(value) when is_list(value), do: Enum.map(value, &stringify/1)
  defp stringify(value), do: value

  defp envelope_evidence_refs(envelope) do
    [
      envelope.extracted.claims,
      envelope.extracted.colors,
      envelope.extracted.questions,
      envelope.extracted.events
    ]
    |> Enum.flat_map(fn items ->
      (items || [])
      |> Enum.flat_map(&(&1.evidence_refs || []))
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> case do
      [] -> [%{"input_ref" => envelope.input_ref}]
      refs -> refs
    end
  end
end
