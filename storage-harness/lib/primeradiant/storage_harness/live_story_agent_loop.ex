defmodule Primeradiant.StorageHarness.LiveStoryAgentLoop do
  @moduledoc false

  alias Primeradiant.Agentic.LiveGibson
  alias Primeradiant.Ingestion.Admission

  alias Primeradiant.StorageHarness.{
    AgentRun,
    ChangesetStore,
    Edge,
    EvidenceRef,
    GraphCommit,
    Input,
    Proposal,
    ProposalDecision,
    ProposalOp,
    SoupNode,
    State,
    Story,
    StoryEvent,
    StoryFactVersion
  }

  @candidate_stopwords MapSet.new(
                         ~w(a an and are as at be by for from has have in into is it its of on or the this to with without)
                       )

  @system_prompt """
  You are a Prime Radiant story agent. Use only the bounded packet supplied in the user message.
  Source admission is evidence only; you own story/meaning output only when it is packet-grounded.
  Return exactly one JSON object matching the requested schema.
  """

  @identity_prompt """
  Decide story identity/shape for the admitted source evidence. Choose a stable story_key from the packet text.
  Compare the packet to visible_story_refs. Reuse an existing story_key only when source evidence is about the same story.
  If a visible story already covers the same named company, product, person, event, place, or incident family, reuse that story_key.
  Treat soup_candidate_hint as bounded context only; do not copy its suggested story key unless your packet-grounded judgment agrees.
  Do not use source-provided story labels. Return new_story or substantive_update with rationale.
  """

  @meaning_prompt """
  Author the smallest story/meaning mutation justified by the bounded packet and the story identity decision.
  If the packet supports a story write, return operation_family commit_story_meaning with changed_facts.
  If the packet repeats an existing story without material change, classify duplicate, no_op, stale, or adds_color.
  If evidence is insufficient, return operation_family mark_no_op with a refusal_reason.
  """

  def run(%State{} = state, admissions, actor_id, opts \\ []) when is_list(admissions) do
    adapter = Keyword.get(opts, :adapter, &__MODULE__.invoke_live_agent/3)

    {state, chains} =
      Enum.reduce(admissions, {state, []}, fn admission, {state, chains} ->
        {state, chain} = run_for_admission(state, admission, actor_id, adapter)
        {state, chains ++ [chain]}
      end)

    {state,
     %{
       source_behavior: :evidence_admission_then_live_story_agents,
       substrate_proof_only: not story_meaning_proof?(state, chains),
       story_meaning_proof: story_meaning_proof?(state, chains),
       correlation_chains: chains,
       agent_families: [:story_identity, :meaning_update],
       activations: Enum.count(state.audit_events, &(&1.event == :story_agent_activation)),
       packets: Enum.count(state.audit_events, &(&1.event == :story_agent_packet)),
       agent_runs: length(state.agent_runs),
       stories: length(state.stories),
       proposals: length(state.proposals),
       proposal_ops: length(state.proposal_ops),
       proposal_decisions: length(state.proposal_decisions),
       graph_commits: length(state.graph_commits),
       story_events: length(state.story_events),
       zero_agent_zero_story_shape_nonconforming?: zero_agent_zero_story_shape?(state)
     }}
  end

  defp story_meaning_proof?(state, chains) do
    chains != [] and
      length(state.agent_runs) >= 2 and
      length(state.proposals) > 0 and
      (length(state.graph_commits) > 0 or length(state.story_events) > 0)
  end

  defp zero_agent_zero_story_shape?(state) do
    length(state.agent_runs) == 0 and
      length(state.stories) == 0 and
      length(state.proposals) == 0 and
      length(state.graph_commits) == 0
  end

  def invoke_live_agent(config, packet, _ctx) do
    live = LiveGibson.invoke(config.role, config, packet)

    %{
      output: live.output,
      model: live.model,
      model_route: live.model_route,
      producer_kind: Atom.to_string(live.producer_kind),
      decision_source: Atom.to_string(live.decision_source),
      output_hash: live.agent_output_hash,
      invocation_transport_id: live.response_id,
      duration_ms: live.duration_ms
    }
  end

  defp run_for_admission(state, admission, actor_id, adapter) do
    input = input_for!(state, admission)
    source_ref = Admission.input_ref(input)
    correlation_id = "correlation:#{source_ref}:#{System.unique_integer([:positive])}"
    evidence_refs = admission.evidence_refs

    activation = %{
      event: :story_agent_activation,
      activation_kind: :ingest_event,
      activation_id: "activation:#{correlation_id}",
      source_ref: source_ref,
      correlation_id: correlation_id,
      scheduler_substrate: true,
      story_meaning_proof: false,
      eligible_agent_families: [:story_identity, :meaning_update]
    }

    soup_candidate_hint = soup_candidate_hint(state, input, actor_id)

    {state, identity_run, identity} =
      invoke_agent(
        state,
        config(:story_identity),
        packet(state, input, admission, :story_identity, correlation_id, actor_id, %{
          soup_candidate_hint: soup_candidate_hint
        }),
        actor_id,
        adapter,
        correlation_id
      )

    story_key = story_key(identity.output, input)

    {state, meaning_run, meaning} =
      invoke_agent(
        state,
        config(:meaning_update),
        packet(state, input, admission, :meaning_update, correlation_id, actor_id, %{
          story_identity: %{
            story_key: story_key,
            classification: classification(identity.output, "new_story"),
            confidence: confidence(identity.output, 0.66)
          },
          soup_candidate_hint: soup_candidate_hint
        }),
        actor_id,
        adapter,
        correlation_id
      )

    {state, write_chain} =
      write_story_meaning(
        state,
        input,
        source_ref,
        story_key,
        soup_candidate_hint,
        identity,
        meaning,
        evidence_refs,
        actor_id,
        correlation_id
      )

    chain =
      Map.merge(write_chain, %{
        correlation_id: correlation_id,
        source_ref: source_ref,
        activation_id: activation.activation_id,
        packet_ids: [identity.packet_id, meaning.packet_id],
        agent_run_ids: [identity_run.id, meaning_run.id],
        agent_families: [identity_run.agent_type, meaning_run.agent_type],
        evidence_refs: evidence_refs
      })

    state =
      state
      |> State.audit(activation)
      |> State.audit(%{
        event: :story_agent_chain_completed,
        correlation_id: correlation_id,
        source_ref: source_ref,
        proposal_id: chain.proposal_id,
        graph_commit_id: chain.graph_commit_id,
        story_event_id: chain.story_event_id
      })

    {state, chain}
  end

  defp invoke_agent(state, config, packet, actor_id, adapter, correlation_id) do
    packet_hash = hash(packet)
    output = adapter.(config, packet, %{actor_id: actor_id, correlation_id: correlation_id})
    output_hash = output[:output_hash] || hash(output.output)

    run =
      ChangesetStore.insert!(AgentRun, %{
        tenant_id: state.tenant_id,
        agent_run_key: "agent-run:#{config.config_version}:#{correlation_id}",
        agent_type: Atom.to_string(config.role),
        prompt_version: config.config_version,
        model: output[:model] || "unrecorded",
        scope: %{
          "correlation_id" => correlation_id,
          "packet_id" => packet.packet_id,
          "packet_hash" => packet_hash,
          "prompt_version_hash" => config.prompt_version_hash,
          "output_hash" => output_hash,
          "model_route" => output[:model_route],
          "producer_kind" => output[:producer_kind],
          "decision_source" => output[:decision_source],
          "invocation_transport_id" => output[:invocation_transport_id],
          "evidence_refs" => packet.evidence_refs
        },
        status: "succeeded",
        trace_id: "trace:#{correlation_id}:#{config.role}",
        started_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
        ended_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })

    state =
      state
      |> State.append(:agent_runs, run)
      |> State.put_source_id(:agent_run, run.agent_run_key, run.id)
      |> State.audit(%{
        event: :story_agent_packet,
        packet_id: packet.packet_id,
        packet_hash: packet_hash,
        correlation_id: correlation_id,
        agent_role: config.role,
        evidence_refs: packet.evidence_refs,
        bounded: packet.raw_database_access == false
      })
      |> State.audit(%{
        event: :story_agent_invoked,
        agent_run_id: run.id,
        agent_role: config.role,
        config_version: config.config_version,
        packet_hash: packet_hash,
        output_hash: output_hash,
        correlation_id: correlation_id
      })

    {state, run,
     %{
       output: normalize_output(output.output),
       packet_id: packet.packet_id,
       packet_hash: packet_hash,
       run: run
     }}
  end

  defp write_story_meaning(
         state,
         input,
         source_ref,
         story_key,
         soup_candidate_hint,
         identity,
         meaning,
         evidence_refs,
         actor_id,
         correlation_id
       ) do
    confidence = confidence(meaning.output, confidence(identity.output, 0.66))
    raw_changed_facts = changed_facts(meaning.output, input)
    title = story_title(meaning.output, input)
    existing_story? = Enum.any?(state.stories, &(&1.story_key == story_key))

    event_classification = event_classification(meaning.output, not existing_story?)

    changed_facts =
      if material_event_classification?(event_classification), do: raw_changed_facts, else: %{}

    {state, story, story_inserted?} =
      upsert_story(
        state,
        story_key,
        title,
        input,
        changed_facts,
        identity,
        meaning,
        correlation_id
      )

    story_version = story.version

    event_classification =
      if story_inserted?,
        do: "split",
        else: event_classification

    proposal =
      ChangesetStore.insert!(Proposal, %{
        tenant_id: state.tenant_id,
        proposal_key: "live-story-agent-proposal:#{correlation_id}",
        agent_run_id: meaning.run.id,
        actor_id: actor_id,
        story_id: story.id,
        fixture_id: nil,
        classification: event_classification,
        confidence: ChangesetStore.decimal(confidence),
        rationale: rationale(meaning.output),
        status: "accepted"
      })

    proposal_op =
      ChangesetStore.insert!(ProposalOp, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        position: 0,
        op_type: "record_event",
        payload: %{
          "operation_family" => operation_family(meaning.output),
          "story_key" => story_key,
          "classification" => event_classification,
          "changed_facts" => changed_facts,
          "source_ref" => source_ref,
          "soup_candidate_hint" => soup_candidate_hint,
          "correlation_id" => correlation_id,
          "identity_agent_run_id" => identity.run.id,
          "meaning_agent_run_id" => meaning.run.id,
          "packet_hash" => meaning.packet_hash,
          "output_hash" => meaning.run.scope["output_hash"],
          "refusal_reason" => meaning.output["refusal_reason"]
        },
        evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
        confidence: ChangesetStore.decimal(confidence),
        status: "committed",
        committed_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })

    decision =
      ChangesetStore.insert!(ProposalDecision, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        from_status: "pending",
        to_status: "accepted",
        actor_type: "agentic_write_gate",
        actor_id: actor_id,
        evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
        confidence: ChangesetStore.decimal(confidence),
        rationale: rationale(meaning.output)
      })

    commit =
      ChangesetStore.insert!(GraphCommit, %{
        tenant_id: state.tenant_id,
        proposal_id: proposal.id,
        proposal_op_id: proposal_op.id,
        commit_type: operation_family(meaning.output),
        committed_by_type: "agent",
        committed_by_id: meaning.run.id,
        evidence_refs: ChangesetStore.evidence_maps(evidence_refs),
        confidence: ChangesetStore.decimal(confidence)
      })

    input_node =
      ChangesetStore.insert!(SoupNode, %{
        tenant_id: state.tenant_id,
        node_key: source_ref,
        node_type: "input",
        title: input.title || source_ref,
        state: "active",
        input_id: input.id,
        proposal_id: proposal.id,
        proposal_op_id: proposal_op.id,
        graph_commit_id: commit.id,
        confidence: ChangesetStore.decimal(confidence),
        attrs: %{"acl" => input.acl, "correlation_id" => correlation_id}
      })

    {state, story_node} =
      upsert_story_node(state, story, proposal, proposal_op, commit, confidence, correlation_id)

    edge =
      ChangesetStore.insert!(Edge, %{
        tenant_id: state.tenant_id,
        from_node_id: input_node.id,
        to_node_id: story_node.id,
        edge_type: edge_type_for_event(event_classification),
        status: "committed",
        confidence: ChangesetStore.decimal(confidence),
        proposal_id: proposal.id,
        proposal_op_id: proposal_op.id,
        graph_commit_id: commit.id,
        attrs:
          article_story_edge_attrs(
            source_ref,
            event_classification,
            evidence_refs,
            identity,
            meaning,
            correlation_id
          )
      })

    event =
      ChangesetStore.insert!(StoryEvent, %{
        tenant_id: state.tenant_id,
        story_id: story.id,
        input_id: input.id,
        classification: event_classification,
        story_version: story_version,
        changed_facts: changed_facts,
        observed_at: input.observed_at,
        proposal_id: proposal.id,
        proposal_op_id: proposal_op.id,
        graph_commit_id: commit.id,
        confidence: ChangesetStore.decimal(confidence)
      })

    state =
      state
      |> State.append(:proposals, proposal)
      |> State.append(:proposal_ops, proposal_op)
      |> State.append(:proposal_decisions, decision)
      |> State.append(:graph_commits, commit)
      |> State.append(:soup_nodes, input_node)
      |> State.put_source_id(:node, source_ref, input_node.id)
      |> append_new_story_node(story_node, story_inserted?)
      |> State.append(:edges, edge)
      |> State.append(:story_events, event)
      |> write_fact_versions(
        story,
        input,
        proposal,
        proposal_op,
        commit,
        changed_facts,
        evidence_refs
      )
      |> evidence("proposal", proposal.id, input, evidence_refs, proposal.id, nil, nil)
      |> evidence(
        "proposal_op",
        proposal_op.id,
        input,
        evidence_refs,
        proposal.id,
        proposal_op.id,
        nil
      )
      |> evidence(
        "graph_commit",
        commit.id,
        input,
        evidence_refs,
        proposal.id,
        proposal_op.id,
        nil
      )
      |> evidence(
        "soup_node",
        input_node.id,
        input,
        evidence_refs,
        proposal.id,
        proposal_op.id,
        input_node.id
      )
      |> evidence(
        "soup_node",
        story_node.id,
        input,
        evidence_refs,
        proposal.id,
        proposal_op.id,
        story_node.id
      )
      |> evidence(
        "edge",
        edge.id,
        input,
        evidence_refs,
        proposal.id,
        proposal_op.id,
        nil,
        edge.id
      )
      |> evidence("story_event", event.id, input, evidence_refs, proposal.id, proposal_op.id, nil)

    {state,
     %{
       story_id: story.id,
       story_key: story_key,
       proposal_id: proposal.id,
       proposal_op_id: proposal_op.id,
       proposal_decision_id: decision.id,
       graph_commit_id: commit.id,
       story_event_id: event.id,
       classification: event_classification,
       operation_family: operation_family(meaning.output),
       meaning_agent_run_id: meaning.run.id
     }}
  end

  defp upsert_story(
         state,
         story_key,
         title,
         input,
         changed_facts,
         identity,
         meaning,
         correlation_id
       ) do
    attrs = %{
      "correlation_id" => correlation_id,
      "identity_agent_run_id" => identity.run.id,
      "meaning_agent_run_id" => meaning.run.id,
      "identity_packet_hash" => identity.packet_hash,
      "meaning_packet_hash" => meaning.packet_hash,
      "identity_output_hash" => identity.run.scope["output_hash"],
      "meaning_output_hash" => meaning.run.scope["output_hash"]
    }

    case Enum.find(state.stories, &(&1.story_key == story_key)) do
      nil ->
        story =
          ChangesetStore.insert!(Story, %{
            tenant_id: state.tenant_id,
            story_key: story_key,
            title: title,
            state: "active",
            version: 1,
            first_observed_at: input.observed_at,
            updated_at_story: input.observed_at,
            last_material_at: input.observed_at,
            structural_facts: changed_facts,
            background_facts: %{},
            colors: [],
            questions: %{},
            topic_tokens: [],
            attrs: attrs
          })

        {state
         |> State.append(:stories, story)
         |> State.put_source_id(:story, story_key, story.id), story, true}

      existing ->
        story =
          ChangesetStore.update!(existing, %{
            title: title,
            version: existing.version + 1,
            updated_at_story: input.observed_at,
            last_material_at:
              if(
                material_event_classification?(event_classification(meaning.output, false)) and
                  changed_facts != %{},
                do: input.observed_at,
                else: existing.last_material_at
              ),
            structural_facts:
              if(
                material_event_classification?(event_classification(meaning.output, false)) and
                  changed_facts != %{},
                do: Map.merge(existing.structural_facts || %{}, changed_facts),
                else: existing.structural_facts || %{}
              ),
            attrs: Map.merge(existing.attrs || %{}, attrs)
          })

        {State.replace(state, :stories, story.id, story), story, false}
    end
  end

  defp upsert_story_node(state, story, proposal, op, commit, confidence, correlation_id) do
    case Enum.find(state.soup_nodes, &(&1.node_key == story.story_key)) do
      nil ->
        node =
          ChangesetStore.insert!(SoupNode, %{
            tenant_id: state.tenant_id,
            node_key: story.story_key,
            node_type: "story",
            title: story.title,
            state: "active",
            story_id: story.id,
            proposal_id: proposal.id,
            proposal_op_id: op.id,
            graph_commit_id: commit.id,
            confidence: ChangesetStore.decimal(confidence),
            attrs: %{"correlation_id" => correlation_id}
          })

        {state |> State.put_source_id(:node, story.story_key, node.id), node}

      node ->
        {state, node}
    end
  end

  defp append_new_story_node(state, story_node, true) do
    state
    |> State.append(:soup_nodes, story_node)
    |> State.put_source_id(:node, story_node.node_key, story_node.id)
  end

  defp append_new_story_node(state, _story_node, false), do: state

  defp write_fact_versions(
         state,
         story,
         input,
         proposal,
         op,
         commit,
         changed_facts,
         evidence_refs
       ) do
    Enum.reduce(changed_facts, state, fn {key, value}, acc ->
      claim_key = "claim:#{story.story_key}:#{key}"

      claim_node =
        case Enum.find(acc.soup_nodes, &(&1.node_key == claim_key)) do
          nil ->
            ChangesetStore.insert!(SoupNode, %{
              tenant_id: acc.tenant_id,
              node_key: claim_key,
              node_type: "claim",
              title: "#{key}=#{value}",
              state: "active",
              story_id: story.id,
              proposal_id: proposal.id,
              proposal_op_id: op.id,
              graph_commit_id: commit.id,
              confidence: op.confidence,
              attrs: %{"fact_key" => key, "fact_value" => value}
            })

          existing ->
            existing
        end

      fact =
        ChangesetStore.insert!(StoryFactVersion, %{
          tenant_id: acc.tenant_id,
          story_id: story.id,
          claim_node_id: claim_node.id,
          fact_key: to_string(key),
          fact_value: to_string(value),
          time_scope: "current",
          status: "current",
          proposal_id: proposal.id,
          proposal_op_id: op.id,
          graph_commit_id: commit.id,
          input_id: input.id,
          confidence: op.confidence,
          observed_at: input.observed_at
        })

      acc =
        if Enum.any?(acc.soup_nodes, &(&1.id == claim_node.id)),
          do: acc,
          else:
            acc
            |> State.append(:soup_nodes, claim_node)
            |> State.put_source_id(:node, claim_key, claim_node.id)

      acc
      |> State.append(:story_fact_versions, fact)
      |> evidence(
        "soup_node",
        claim_node.id,
        input,
        evidence_refs,
        proposal.id,
        op.id,
        claim_node.id
      )
      |> evidence("story_fact_version", fact.id, input, evidence_refs, proposal.id, op.id, nil)
    end)
  end

  defp evidence(
         state,
         subject_type,
         subject_id,
         input,
         refs,
         proposal_id,
         proposal_op_id,
         soup_node_id,
         edge_id \\ nil
       ) do
    Enum.reduce(refs, state, fn ref, state ->
      evidence =
        ChangesetStore.insert!(EvidenceRef, %{
          tenant_id: state.tenant_id,
          subject_type: subject_type,
          subject_id: subject_id,
          input_id: input.id,
          soup_node_id: soup_node_id,
          proposal_id: proposal_id,
          proposal_op_id: proposal_op_id,
          edge_id: edge_id,
          span_start: 0,
          span_end: byte_size(input.body_text || ""),
          evidence_label: ref,
          evidence_hash: input.content_sha256
        })

      State.append(state, :evidence_refs, evidence)
    end)
  end

  defp input_for!(state, admission) do
    Enum.find(state.inputs, &(Admission.input_ref(&1) == admission.source_ref)) ||
      raise ArgumentError, "admitted input not found for #{admission.source_ref}"
  end

  defp packet(state, %Input{} = input, admission, role, correlation_id, actor_id, extra) do
    Map.merge(
      %{
        packet_id: "packet:#{role}:#{correlation_id}",
        packet_contract: :acl_scoped_soup_packet,
        role: role,
        output_visibility: input.acl["privacy"] || "public",
        source_ref: admission.source_ref,
        input_id: input.id,
        external_id: input.external_id,
        evidence_refs: admission.evidence_refs,
        content_span_refs: admission.content_span_refs,
        source_provenance: admission.source_provenance,
        snippet: String.slice(input.body_text || "", 0, 600),
        visible_story_refs: visible_story_refs(state, actor_id),
        traversal_depth: 1,
        raw_database_access: false
      },
      extra
    )
  end

  defp soup_candidate_hint(state, input, actor_id) do
    input_tokens = content_tokens([input.title, input.body_text])

    state.stories
    |> Enum.map(fn story ->
      story_inputs = visible_story_inputs(state, story, actor_id)
      story_tokens = content_tokens(Enum.flat_map(story_inputs, &[&1.title, &1.body_text]))
      overlap = MapSet.intersection(input_tokens, story_tokens) |> MapSet.to_list()

      %{
        story_key: story.story_key,
        overlap_tokens: overlap,
        overlap_count: length(overlap),
        input_token_count: MapSet.size(input_tokens),
        evidence_input_refs: Enum.map(story_inputs, &Admission.input_ref/1)
      }
    end)
    |> Enum.filter(&(&1.overlap_count >= 10))
    |> Enum.sort_by(&{-&1.overlap_count, &1.story_key})
    |> List.first()
    |> case do
      nil ->
        nil

      candidate ->
        Map.merge(candidate, %{
          suggested_story_key: candidate.story_key,
          suggested_classification: "no_op",
          rationale:
            "committed story/input token overlap indicates repeated nonmaterial source pressure"
        })
    end
  end

  defp visible_story_refs(state, actor_id) do
    state.stories
    |> Enum.map(fn story ->
      %{
        story_key: story.story_key,
        title: story.title,
        version: story.version,
        state: story.state,
        structural_facts: story.structural_facts || %{},
        updated_at_story: story.updated_at_story && DateTime.to_iso8601(story.updated_at_story),
        last_material_at: story.last_material_at && DateTime.to_iso8601(story.last_material_at),
        evidence_input_refs:
          Enum.map(visible_story_inputs(state, story, actor_id), &Admission.input_ref/1)
      }
    end)
  end

  defp visible_story_inputs(state, story, actor_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story.id))
    |> Enum.map(fn event -> Enum.find(state.inputs, &(&1.id == event.input_id)) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(&input_visible_to_actor?(&1, actor_id))
    |> Enum.uniq_by(& &1.id)
  end

  defp input_visible_to_actor?(input, actor_id) do
    acl = input.acl || %{"privacy" => "public"}
    acl["privacy"] == "public" or actor_id in (acl["participants"] || [])
  end

  defp content_tokens(values) do
    values
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> String.downcase()
    |> String.split(~r/[^a-z0-9]+/, trim: true)
    |> Enum.reject(&(String.length(&1) < 3 or MapSet.member?(@candidate_stopwords, &1)))
    |> MapSet.new()
  end

  defp config(:story_identity) do
    config(:story_identity, "story-identity.v1.t1269.live-loop", @identity_prompt, %{
      story_key: "stable story key",
      classification: "new_story | substantive_update",
      confidence: "0.0-1.0",
      rationale: "string"
    })
  end

  defp config(:meaning_update) do
    config(:meaning_update, "meaning-update.v1.t1269.live-loop", @meaning_prompt, %{
      operation_family: "commit_story_meaning | mark_no_op",
      classification:
        "new_story | substantive_update | repeated_noop_input | duplicate | no_op | adds_color | stale",
      story_key: "stable story key",
      changed_facts: "object",
      confidence: "0.0-1.0",
      rationale: "string",
      refusal_reason: "string or null"
    })
  end

  defp config(role, version, task_prompt, output_schema) do
    prompt_body = @system_prompt <> "\n" <> task_prompt <> "\n" <> Jason.encode!(output_schema)

    %{
      role: role,
      config_version: version,
      system_prompt: @system_prompt,
      task_prompt: task_prompt,
      output_schema: output_schema,
      prompt_body: prompt_body,
      prompt_version_hash: hash(prompt_body)
    }
  end

  defp normalize_output(output), do: Admission.normalize_keys(output || %{})

  defp story_key(output, input) do
    slug(output["story_key"] || input.title || input.external_id)
  end

  defp article_story_edge_attrs(
         source_ref,
         event_classification,
         evidence_refs,
         identity,
         meaning,
         correlation_id
       ) do
    %{
      "edge_contract" => "article_story_contribution",
      "link_basis" => rationale(meaning.output),
      "contribution_type" => event_classification,
      "source_ref" => source_ref,
      "evidence_refs" => ChangesetStore.evidence_maps(evidence_refs),
      "agent_run_id" => meaning.run.id,
      "agent_prompt_version" => meaning.run.prompt_version,
      "agent_output_hash" => meaning.run.scope["output_hash"],
      "packet_hash" => meaning.packet_hash,
      "identity_agent_run_id" => identity.run.id,
      "identity_prompt_version" => identity.run.prompt_version,
      "identity_output_hash" => identity.run.scope["output_hash"],
      "identity_packet_hash" => identity.packet_hash,
      "correlation_id" => correlation_id
    }
  end

  defp story_title(output, input), do: output["title"] || input.title || input.external_id

  defp classification(output, default) do
    value = to_string(output["classification"] || default)

    if value in [
         "new_story",
         "substantive_update",
         "repeated_noop_input",
         "duplicate",
         "no_op",
         "adds_color",
         "stale"
       ] do
      value
    else
      default
    end
  end

  defp event_classification(_output, true), do: "split"

  defp event_classification(output, false) do
    case classification(output, "substantive_update") do
      "duplicate" -> "duplicate"
      "no_op" -> "no_op"
      "repeated_noop_input" -> "no_op"
      "adds_color" -> "color"
      "stale" -> "stale"
      _ -> "attach"
    end
  end

  defp edge_type_for_event("duplicate"), do: "duplicates"
  defp edge_type_for_event("no_op"), do: "duplicates"
  defp edge_type_for_event("color"), do: "adds_color"
  defp edge_type_for_event("conflict"), do: "contradicts"
  defp edge_type_for_event(_classification), do: "supports"

  defp material_event_classification?(classification),
    do: classification in ["split", "attach", "conflict"]

  defp operation_family(output) do
    case to_string(output["operation_family"] || "commit_story_meaning") do
      "mark_no_op" -> "mark_no_op"
      _ -> "commit_story_meaning"
    end
  end

  defp confidence(output, default) do
    case output["confidence"] do
      value when is_float(value) ->
        min(max(value, 0.0), 1.0)

      value when is_integer(value) ->
        min(max(value / 1, 0.0), 1.0)

      value when is_binary(value) ->
        case Float.parse(value) do
          {number, _} -> min(max(number, 0.0), 1.0)
          :error -> default
        end

      _ ->
        default
    end
  end

  defp changed_facts(output, input) do
    case output["changed_facts"] do
      %{} = facts when map_size(facts) > 0 -> facts
      _ -> %{"admitted_source_title" => input.title || input.external_id}
    end
  end

  defp rationale(output),
    do: output["rationale"] || output["refusal_reason"] || "Packet-grounded story agent output."

  defp slug(value) do
    value
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
    |> case do
      "" -> "story"
      slug -> String.slice(slug, 0, 80)
    end
  end

  defp hash(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(value))
    |> Base.encode16(case: :lower)
  end
end
