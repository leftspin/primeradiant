defmodule Primeradiant.Arbitration.Engine do
  @moduledoc false

  alias Primeradiant.Soup.Store

  def commit(store, proposal, normalized) do
    validate_proposal!(store, proposal)

    accepted = %{proposal | status: :accepted}

    store = %{
      store
      | proposals: store.proposals ++ [accepted],
        proposal_decisions:
          store.proposal_decisions ++
            [
              %{
                proposal_id: proposal.id,
                from: proposal.status,
                to: :accepted,
                actor: :arbiter,
                actor_id: proposal.actor_id,
                evidence_refs: proposal.evidence_refs,
                confidence: proposal.confidence
              }
            ]
    }

    Enum.reduce(accepted.ops, store, fn op, acc ->
      apply_op(acc, op, normalized, accepted)
    end)
  end

  defp apply_op(
         store,
         %{
           op: :create_input,
           input_id: input_id,
           title: title,
           source_type: source_type,
           observed_at: observed_at,
           acl: acl
         },
         _normalized,
         proposal
       ) do
    store
    |> Store.put_node(input_id, %{
      node_type: :input,
      title: title,
      state: :active,
      attrs: %{source_type: source_type, observed_at: observed_at, acl: acl},
      evidence_refs: proposal.evidence_refs,
      proposal_id: proposal.id,
      confidence: proposal.confidence
    })
    |> add_commit(proposal, :create_input)
  end

  defp apply_op(
         store,
         %{op: :create_story, story_key: story_key, title: title, observed_at: observed_at},
         normalized,
         proposal
       ) do
    story = %{
      story_key: story_key,
      title: title,
      created_at: observed_at,
      updated_at: observed_at,
      last_material_at: observed_at,
      inputs: [],
      input_fingerprints: MapSet.new(),
      structural_facts: %{},
      fact_provenance: %{},
      background_facts: %{},
      colors: [],
      questions: %{},
      state: :active,
      version: 0,
      history: [],
      conflicts: [],
      topic_tokens: normalized.topic_tokens,
      watch_ids: []
    }

    store
    |> Store.put_story(story_key, story)
    |> Store.put_node(story_key, %{
      node_type: :story,
      title: title,
      state: :active,
      attrs: %{created_at: observed_at},
      evidence_refs: proposal.evidence_refs,
      proposal_id: proposal.id,
      confidence: proposal.confidence
    })
    |> add_commit(proposal, :create_story)
  end

  defp apply_op(
         store,
         %{
           op: :attach_input,
           story_key: story_key,
           input_id: input_id,
           edge_type: edge_type
         } = op,
         normalized,
         proposal
       ) do
    story = Store.story(store, story_key)

    updated = %{
      story
      | inputs: story.inputs ++ [input_id],
        input_fingerprints: MapSet.put(story.input_fingerprints, normalized.fingerprint),
        updated_at: normalized.observed_at,
        version: story.version + 1,
        state: :active,
        topic_tokens: MapSet.union(story.topic_tokens, normalized.topic_tokens)
    }

    store
    |> Store.put_story(story_key, updated)
    |> Map.update!(:indexed_fingerprints, &MapSet.put(&1, normalized.fingerprint))
    |> add_edge(%{
      from: input_id,
      to: story_key,
      edge_type: edge_type,
      confidence: op.confidence,
      status: :committed,
      proposal_id: proposal.id,
      evidence_refs: op.evidence_refs
    })
    |> add_commit(proposal, :attach_input)
  end

  defp apply_op(
         store,
         %{op: :merge_facts, story_key: story_key, facts: facts} = op,
         normalized,
         proposal
       ) do
    story = Store.story(store, story_key)

    updated = %{
      story
      | structural_facts: Map.merge(story.structural_facts, stringify_keys(facts)),
        fact_provenance: Map.merge(story.fact_provenance, provenance_for(facts, proposal, op)),
        last_material_at: normalized.observed_at
    }

    store
    |> Store.put_story(story_key, updated)
    |> put_fact_nodes(story_key, facts, proposal, op)
    |> add_commit(proposal, :merge_facts)
  end

  defp apply_op(
         store,
         %{op: :merge_background, story_key: story_key, background: background},
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)

    updated = %{
      story
      | background_facts: Map.merge(story.background_facts, stringify_keys(background))
    }

    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :merge_background)
  end

  defp apply_op(
         store,
         %{op: :append_colors, story_key: story_key, colors: colors},
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)
    updated = %{story | colors: Enum.uniq(story.colors ++ colors)}
    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :append_colors)
  end

  defp apply_op(
         store,
         %{op: :add_questions, story_key: story_key, questions: questions},
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)
    updated = %{story | questions: Map.merge(story.questions, stringify_keys(questions))}
    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :add_questions)
  end

  defp apply_op(
         store,
         %{op: :record_conflicts, story_key: story_key, conflicts: conflicts} = op,
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)

    conflicts =
      Enum.map(conflicts, fn conflict ->
        conflict
        |> Map.put(:proposal_id, proposal.id)
        |> Map.put(:agent_run_id, proposal.agent_run_id)
        |> Map.put(:evidence_refs, op.evidence_refs)
        |> Map.put(:confidence, op.confidence)
      end)

    updated = %{story | conflicts: story.conflicts ++ conflicts}
    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :record_conflicts)
  end

  defp apply_op(
         store,
         %{op: :attach_watch, story_key: story_key, watch_id: watch_id, edge_type: edge_type} =
           op,
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)
    updated = %{story | watch_ids: Enum.uniq(story.watch_ids ++ [watch_id])}

    store
    |> Store.put_story(story_key, updated)
    |> add_edge(%{
      from: watch_id,
      to: story_key,
      edge_type: edge_type,
      confidence: op.confidence,
      status: :committed,
      proposal_id: proposal.id,
      evidence_refs: op.evidence_refs
    })
    |> add_commit(proposal, :attach_watch)
  end

  defp apply_op(
         store,
         %{
           op: :attach_story_part_of,
           child_story_key: child_story_key,
           parent_story_key: parent_story_key,
           edge_type: edge_type
         } = op,
         _normalized,
         proposal
       ) do
    store
    |> add_edge(%{
      from: child_story_key,
      to: parent_story_key,
      edge_type: edge_type,
      confidence: op.confidence,
      status: :committed,
      proposal_id: proposal.id,
      evidence_refs: op.evidence_refs
    })
    |> add_commit(proposal, :attach_story_part_of)
  end

  defp apply_op(
         store,
         %{op: :mark_state, story_key: story_key, state: state},
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)
    updated = %{story | state: state}
    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :mark_state)
  end

  defp apply_op(
         store,
         %{op: :mark_last_seen_input, story_key: story_key, observed_at: observed_at},
         _normalized,
         proposal
       ) do
    story = Store.story(store, story_key)
    updated = %{story | updated_at: observed_at}
    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :mark_last_seen_input)
  end

  defp apply_op(
         store,
         %{
           op: :record_event,
           story_key: story_key,
           classification: classification,
           input_id: input_id,
           changed_facts: changed_facts
         },
         normalized,
         proposal
       ) do
    story = Store.story(store, story_key)

    updated = %{
      story
      | history:
          story.history ++
            [
              %{
                classification: classification,
                input_id: input_id,
                observed_at: normalized.observed_at,
                story_version: story.version,
                changed_facts: changed_facts
              }
            ]
    }

    store |> Store.put_story(story_key, updated) |> add_commit(proposal, :record_event)
  end

  defp add_commit(store, proposal, op) do
    %{
      store
      | commits:
          store.commits ++
            [
              %{
                proposal_id: proposal.id,
                op: op,
                status: :committed,
                actor_id: proposal.actor_id,
                evidence_refs: proposal.evidence_refs,
                confidence: proposal.confidence
              }
            ]
    }
  end

  defp add_edge(store, edge) do
    %{store | edges: store.edges ++ [edge]}
  end

  defp stringify_keys(map) do
    Map.new(map, fn {k, v} -> {to_string(k), v} end)
  end

  defp validate_proposal!(store, proposal) do
    if proposal.status != :pending,
      do: raise(ArgumentError, "proposal must be pending before arbitration")

    if !is_binary(proposal.agent_run_id),
      do: raise(ArgumentError, "proposal requires agent_run_id")

    if !is_binary(proposal.actor_id), do: raise(ArgumentError, "proposal requires actor_id")
    if proposal.evidence_refs == [], do: raise(ArgumentError, "proposal requires evidence refs")
    if !is_float(proposal.confidence), do: raise(ArgumentError, "proposal requires confidence")

    if proposal.confidence < 0.0 or proposal.confidence > 1.0,
      do: raise(ArgumentError, "proposal confidence out of range")

    validate_accessible_evidence!(store, proposal)

    Enum.each(proposal.ops, fn op ->
      validate_op!(op)
    end)
  end

  defp validate_accessible_evidence!(store, proposal) do
    refs =
      [proposal.evidence_refs | Enum.map(proposal.ops, &Map.get(&1, :evidence_refs, []))]
      |> List.flatten()
      |> Enum.uniq()

    Enum.each(refs, fn ref ->
      input_op = Enum.find(proposal.ops, &(&1.op == :create_input and &1.input_id == ref))
      input_node = store.nodes[ref]
      acl = if input_op, do: input_op.acl, else: get_in(input_node || %{}, [:attrs, :acl])

      cond do
        acl == nil ->
          raise ArgumentError, "proposal evidence ref is not a committed or proposed input"

        acl["privacy"] == "public" ->
          :ok

        proposal.actor_id in (acl["participants"] || []) ->
          :ok

        true ->
          raise ArgumentError, "proposal evidence is not accessible to actor"
      end
    end)
  end

  defp validate_op!(op) do
    allowed_ops = [
      :create_input,
      :create_story,
      :attach_input,
      :merge_facts,
      :merge_background,
      :append_colors,
      :add_questions,
      :record_conflicts,
      :attach_watch,
      :attach_story_part_of,
      :mark_state,
      :mark_last_seen_input,
      :record_event
    ]

    allowed_edge_types = [
      :supports,
      :updates,
      :duplicates,
      :contradicts,
      :adds_color,
      :part_of,
      :watch_applies_to
    ]

    if op.op not in allowed_ops, do: raise(ArgumentError, "unsupported op")

    if Map.get(op, :evidence_refs, []) == [],
      do: raise(ArgumentError, "op requires evidence refs")

    if !is_float(Map.get(op, :confidence)), do: raise(ArgumentError, "op requires confidence")

    if op.confidence < 0.0 or op.confidence > 1.0,
      do: raise(ArgumentError, "op confidence out of range")

    if Map.has_key?(op, :edge_type) and op.edge_type not in allowed_edge_types do
      raise(ArgumentError, "unsupported edge type")
    end
  end

  defp put_fact_nodes(store, story_key, facts, proposal, op) do
    Enum.reduce(facts, store, fn {key, value}, acc ->
      claim_id = "claim:#{story_key}:#{key}"
      entity_id = "entity:#{value}"

      acc
      |> Store.put_node(claim_id, %{
        node_type: :claim,
        title: "#{key}=#{value}",
        state: :active,
        attrs: %{story_key: story_key, fact: to_string(key), value: value},
        evidence_refs: op.evidence_refs,
        proposal_id: proposal.id,
        confidence: op.confidence
      })
      |> Store.put_node(entity_id, %{
        node_type: :entity,
        title: value,
        state: :active,
        attrs: %{value: value},
        evidence_refs: op.evidence_refs,
        proposal_id: proposal.id,
        confidence: op.confidence
      })
    end)
  end

  defp provenance_for(facts, proposal, op) do
    Map.new(facts, fn {key, _value} ->
      {to_string(key),
       %{
         proposal_id: proposal.id,
         agent_run_id: proposal.agent_run_id,
         evidence_refs: op.evidence_refs,
         confidence: op.confidence
       }}
    end)
  end
end
