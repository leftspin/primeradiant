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
    StoryCardChangeSet,
    StoryCardVersion,
    StoryEvent,
    StoryFactVersion,
    StoryKeyClaim,
    StorySourceCoverage
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

  @synthesis_prompt """
  Maintain the current living story card from committed story state and linked source evidence.
  Return deck, summary, key_claims, source_coverage, contribution explanations, salience hints, and changed fields.
  If any required source display or story-card field is unavailable, return explicit unavailable/refused/incomplete field state with reason and provenance.
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
       agent_families: [:story_identity, :meaning_update, :story_synthesis],
       activations: Enum.count(state.audit_events, &(&1.event == :story_agent_activation)),
       packets: Enum.count(state.audit_events, &(&1.event == :story_agent_packet)),
       agent_runs: length(state.agent_runs),
       stories: length(state.stories),
       proposals: length(state.proposals),
       proposal_ops: length(state.proposal_ops),
       proposal_decisions: length(state.proposal_decisions),
       graph_commits: length(state.graph_commits),
       story_events: length(state.story_events),
       story_card_versions: length(state.story_card_versions),
       story_source_coverage: length(state.story_source_coverage),
       story_key_claims: length(state.story_key_claims),
       zero_agent_zero_story_shape_nonconforming?: zero_agent_zero_story_shape?(state)
     }}
  end

  defp story_meaning_proof?(state, chains) do
    production_agent_runs =
      Enum.filter(state.agent_runs, fn run ->
        get_in(run.scope, ["producer_kind"]) == "live_model_inference"
      end)

    chains != [] and
      length(production_agent_runs) >= 3 and
      length(state.proposals) > 0 and
      (length(state.graph_commits) > 0 or length(state.story_events) > 0) and
      length(state.story_card_versions) > 0
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

  def refresh_story_cards(%State{} = state, actor_id, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, &__MODULE__.invoke_live_agent/3)
    cadence = Keyword.get(opts, :cadence, :hourly_story_card_synthesis)
    limit = Keyword.get(opts, :limit, 8)

    candidates =
      state.stories
      |> Enum.filter(&(&1.state in ["active", "background"]))
      |> Enum.sort_by(&{card_sort_key(current_story_card_version(state, &1.id)), &1.story_key})
      |> Enum.take(limit)

    {state, refreshes} =
      Enum.reduce(candidates, {state, []}, fn story, {state, refreshes} ->
        case latest_story_input(state, story, actor_id) do
          nil ->
            {state,
             refreshes ++
               [
                 %{
                   story_id: story.id,
                   story_key: story.story_key,
                   status: "refused",
                   reason: "no_visible_linked_source"
                 }
               ]}

          input ->
            {state, refresh} =
              refresh_story_card_for_story(state, story, input, actor_id, adapter, cadence)

            {state, refreshes ++ [refresh]}
        end
      end)

    {state,
     %{
       source_behavior: :recurring_cadence_over_admitted_soup,
       source_admission_performed: false,
       cadence: cadence,
       candidate_count: length(candidates),
       refreshed_count: Enum.count(refreshes, &Map.has_key?(&1, :story_card_version_id)),
       refused_count: Enum.count(refreshes, &(&1[:status] == "refused")),
       refreshes: refreshes,
       agent_families: [:story_synthesis],
       agent_runs: length(state.agent_runs),
       story_card_versions: length(state.story_card_versions),
       story_meaning_proof:
         Enum.any?(refreshes, fn refresh ->
           refresh[:producer_kind] == "live_model_inference" and
             refresh[:story_card_status] in ["complete", "refused", "unavailable", "incomplete"]
         end)
     }}
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

    story = Enum.find(state.stories, &(&1.id == write_chain.story_id))

    {state, synthesis_run, synthesis} =
      invoke_agent(
        state,
        config(:story_synthesis),
        packet(state, input, admission, :story_synthesis, correlation_id, actor_id, %{
          story_id: story.id,
          story_key: story.story_key,
          story_version: story.version,
          story_event_id: write_chain.story_event_id,
          refresh_reason: refresh_reason(write_chain.classification),
          committed_story_state: story_packet_state(state, story, actor_id),
          prior_story_card_version: current_story_card_version(state, story.id)
        }),
        actor_id,
        adapter,
        correlation_id
      )

    {state, card_chain} =
      write_story_card(
        state,
        story,
        input,
        admission,
        write_chain,
        synthesis,
        actor_id,
        correlation_id
      )

    chain =
      write_chain
      |> Map.merge(card_chain)
      |> Map.merge(%{
        correlation_id: correlation_id,
        source_ref: source_ref,
        activation_id: activation.activation_id,
        packet_ids: [identity.packet_id, meaning.packet_id, synthesis.packet_id],
        agent_run_ids: [identity_run.id, meaning_run.id, synthesis_run.id],
        agent_families: [
          identity_run.agent_type,
          meaning_run.agent_type,
          synthesis_run.agent_type
        ],
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
        story_event_id: chain.story_event_id,
        story_card_version_id: chain.story_card_version_id
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
          "producer_kind" => output[:producer_kind] || "unrecorded",
          "decision_source" => output[:decision_source] || "unrecorded",
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

  defp refresh_story_card_for_story(state, story, input, actor_id, adapter, cadence) do
    source_ref = Admission.input_ref(input)
    latest_event = latest_story_event(state, story.id, input.id)

    correlation_id =
      "recurring:#{cadence}:#{story.story_key}:#{System.unique_integer([:positive])}"

    evidence_refs =
      evidence_refs_for_input(state, story.id, input.id, ["story:#{story.story_key}"])

    activation = %{
      event: :story_agent_activation,
      activation_kind: cadence,
      activation_id: "activation:#{correlation_id}",
      source_ref: nil,
      story_id: story.id,
      story_key: story.story_key,
      correlation_id: correlation_id,
      scheduler_substrate: true,
      story_meaning_proof: false,
      eligible_agent_families: [:story_synthesis],
      bounded_candidate_packet: true,
      source_admission_performed: false
    }

    admission = %{
      source_ref: source_ref,
      evidence_refs: evidence_refs,
      content_span_refs: [],
      source_provenance: get_in(input.normalized || %{}, ["source_provenance"]) || %{}
    }

    {state, synthesis_run, synthesis} =
      invoke_agent(
        state,
        config(:story_synthesis),
        packet(state, input, admission, :story_synthesis, correlation_id, actor_id, %{
          story_id: story.id,
          story_key: story.story_key,
          story_version: story.version,
          story_event_id: latest_event && latest_event.id,
          refresh_reason: refresh_reason_for_cadence(cadence),
          committed_story_state: story_packet_state(state, story, actor_id),
          prior_story_card_version: current_story_card_version(state, story.id),
          cadence: cadence,
          candidate_reason: refresh_reason_for_cadence(cadence)
        }),
        actor_id,
        adapter,
        correlation_id
      )

    write_chain = %{
      story_event_id: latest_event && latest_event.id,
      classification: refresh_reason_for_cadence(cadence)
    }

    {state, card_chain} =
      write_story_card(
        state,
        story,
        input,
        admission,
        write_chain,
        synthesis,
        actor_id,
        correlation_id
      )

    state =
      state
      |> State.audit(activation)
      |> State.audit(%{
        event: :recurring_story_card_refresh_completed,
        cadence: cadence,
        correlation_id: correlation_id,
        story_id: story.id,
        story_key: story.story_key,
        story_card_version_id: card_chain.story_card_version_id,
        producing_agent_run_id: synthesis_run.id
      })

    {state,
     Map.merge(card_chain, %{
       correlation_id: correlation_id,
       story_id: story.id,
       story_key: story.story_key,
       source_ref: source_ref,
       agent_run_id: synthesis_run.id,
       model: synthesis_run.model,
       model_route: synthesis_run.scope["model_route"],
       producer_kind: synthesis_run.scope["producer_kind"],
       decision_source: synthesis_run.scope["decision_source"]
     })}
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

    event_classification =
      event_classification(meaning.output, not existing_story?, soup_candidate_hint)

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

  defp write_story_card(
         state,
         story,
         input,
         admission,
         write_chain,
         synthesis,
         _actor_id,
         _correlation_id
       ) do
    prior_card = current_story_card_version(state, story.id)
    refresh_reason = refresh_reason(write_chain.classification)
    card_version = card_version_for(state, story.id)
    field_provenance_manifest_id = "fieldprov:#{story.story_key}:card-v#{card_version}"
    provenance_refs = [field_provenance_manifest_id]
    card_status = card_status(synthesis.output)

    card =
      ChangesetStore.insert!(StoryCardVersion, %{
        tenant_id: state.tenant_id,
        story_id: story.id,
        story_version: story.version,
        card_version: card_version,
        status: card_status,
        supersedes_id: prior_card && prior_card.id,
        refresh_reason: refresh_reason,
        producing_agent_run_id: synthesis.run.id,
        packet_hash: synthesis.packet_hash,
        prompt_config_hash: synthesis.run.scope["prompt_version_hash"],
        output_hash: synthesis.run.scope["output_hash"],
        field_provenance_manifest_id: field_provenance_manifest_id,
        title: field_value(synthesis.output["title"], story.title, provenance_refs),
        deck: field_value(synthesis.output["deck"], nil, provenance_refs),
        summary: field_value(synthesis.output["summary"], nil, provenance_refs),
        freshness: %{
          "status" => story.state,
          "story_updated_at" =>
            story.updated_at_story && DateTime.to_iso8601(story.updated_at_story)
        },
        field_completeness: field_completeness(synthesis.output, card_status),
        topic_salience: salience_hints(synthesis.output, state, story),
        provenance: %{
          "agent_run_ids" => [synthesis.run.id],
          "packet_hash" => synthesis.packet_hash,
          "prompt_config_hash" => synthesis.run.scope["prompt_version_hash"],
          "output_hash" => synthesis.run.scope["output_hash"],
          "evidence_refs" => admission.evidence_refs,
          "source_refs" => [admission.source_ref],
          "story_event_refs" => [write_chain.story_event_id],
          "prior_card_version_id" => prior_card && prior_card.id
        }
      })

    coverage_rows =
      source_coverage_rows(
        state,
        story,
        card,
        input,
        admission,
        synthesis.output,
        provenance_refs
      )

    claim_rows =
      key_claim_rows(state, story, card, synthesis.output, admission.evidence_refs)

    change_set =
      ChangesetStore.insert!(StoryCardChangeSet, %{
        tenant_id: state.tenant_id,
        story_id: story.id,
        prior_card_version_id: prior_card && prior_card.id,
        new_card_version_id: card.id,
        changed_field_keys: changed_field_keys(synthesis.output, prior_card),
        added_claim_refs: Enum.map(claim_rows, & &1.claim_ref),
        removed_claim_refs: [],
        changed_claim_refs: Enum.map(claim_rows, & &1.claim_ref),
        changed_source_coverage_refs: Enum.map(coverage_rows, & &1.source_ref),
        refresh_reason: refresh_reason,
        change_summary: field_value(synthesis.output["change_summary"], nil, provenance_refs)
      })

    state =
      state
      |> State.append(:story_card_versions, card)
      |> append_rows(:story_source_coverage, coverage_rows)
      |> append_rows(:story_key_claims, claim_rows)
      |> State.append(:story_card_change_sets, change_set)
      |> State.audit(%{
        event: :story_card_synthesized,
        story_id: story.id,
        story_version: story.version,
        story_card_version_id: card.id,
        refresh_reason: refresh_reason,
        producing_agent_run_id: synthesis.run.id,
        field_provenance_manifest_id: field_provenance_manifest_id,
        status: card.status
      })

    {state,
     %{
       story_card_version_id: card.id,
       story_synthesis_agent_run_id: synthesis.run.id,
       story_card_status: card.status,
       refresh_reason: refresh_reason
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
                material_event_classification?(event_classification(meaning.output, false, nil)) and
                  changed_facts != %{},
                do: input.observed_at,
                else: existing.last_material_at
              ),
            structural_facts:
              if(
                material_event_classification?(event_classification(meaning.output, false, nil)) and
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

  defp append_rows(state, field, rows), do: Enum.reduce(rows, state, &State.append(&2, field, &1))

  defp current_story_card_version(state, story_id) do
    state.story_card_versions
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.sort_by(& &1.card_version, :desc)
    |> List.first()
  end

  defp card_sort_key(nil), do: {0, ""}
  defp card_sort_key(card), do: {1, card.updated_at || card.inserted_at || DateTime.utc_now()}

  defp latest_story_input(state, story, actor_id) do
    state
    |> visible_story_inputs(story, actor_id)
    |> Enum.sort_by(&(&1.observed_at || DateTime.from_unix!(0)), {:desc, DateTime})
    |> List.first()
  end

  defp latest_story_event(state, story_id, input_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story_id and &1.input_id == input_id))
    |> Enum.sort_by(&(&1.observed_at || DateTime.from_unix!(0)), {:desc, DateTime})
    |> List.first()
  end

  defp card_version_for(state, story_id) do
    case current_story_card_version(state, story_id) do
      nil -> 1
      card -> card.card_version + 1
    end
  end

  defp story_packet_state(state, story, actor_id) do
    %{
      title: story.title,
      state: story.state,
      version: story.version,
      structural_facts: story.structural_facts || %{},
      background_facts: story.background_facts || %{},
      topic_tokens: story.topic_tokens || [],
      linked_sources:
        state
        |> visible_story_inputs(story, actor_id)
        |> Enum.map(&source_packet_state/1)
    }
  end

  defp source_packet_state(input) do
    %{
      source_ref: Admission.input_ref(input),
      article_ref: input.external_id,
      canonical_uri: get_in(input.normalized || %{}, ["canonical_uri"]),
      source_name: get_in(input.normalized || %{}, ["source_name"]),
      source_actor: get_in(input.normalized || %{}, ["source_actor"]),
      observed_at: input.observed_at && DateTime.to_iso8601(input.observed_at)
    }
  end

  defp card_status(output) do
    status = to_string(output["status"] || "incomplete")

    cond do
      status in ["refused", "unavailable"] ->
        status

      missing_agent_key_claims?(output) or missing_durable_topic_nodes?(output) ->
        "incomplete"

      status == "complete" ->
        "complete"

      true ->
        "incomplete"
    end
  end

  defp missing_agent_key_claims?(output), do: output["key_claims"] in [nil, []]

  defp missing_durable_topic_nodes?(output) do
    case get_in(output, ["topic_salience", "durable_topic_nodes"]) do
      %{"state" => state} when state in ["complete", "refused"] -> false
      _ -> true
    end
  end

  defp field_value(%{} = value, _fallback, _provenance_refs), do: value

  defp field_value(value, _fallback, provenance_refs) when is_binary(value) and value != "",
    do: %{"text" => value, "state" => "complete", "provenance_refs" => provenance_refs}

  defp field_value(_value, _fallback, provenance_refs) do
    %{
      "text" => nil,
      "state" => "unavailable",
      "reason" => "story_synthesis_agent_did_not_supply_field",
      "provenance_refs" => provenance_refs
    }
  end

  defp field_completeness(output, status) do
    supplied = output["field_completeness"] || %{}

    %{
      "title" => Map.get(supplied, "title", completeness_for(output["title"])),
      "deck" => Map.get(supplied, "deck", completeness_for(output["deck"])),
      "summary" => Map.get(supplied, "summary", completeness_for(output["summary"])),
      "key_claims" =>
        Map.get(
          supplied,
          "key_claims",
          if(missing_agent_key_claims?(output), do: "unavailable", else: "complete")
        ),
      "topic_salience" =>
        Map.get(
          supplied,
          "topic_salience",
          if(missing_durable_topic_nodes?(output), do: "unavailable", else: "complete")
        ),
      "canonical_public_url" => Map.get(supplied, "canonical_public_url", "source_level"),
      "source_label" => Map.get(supplied, "source_label", "source_level"),
      "publication" => Map.get(supplied, "publication", "source_level"),
      "overall" => status
    }
  end

  defp completeness_for(%{"state" => state}), do: state
  defp completeness_for(value) when is_binary(value) and value != "", do: "complete"
  defp completeness_for(_), do: "unavailable"

  defp salience_hints(output, state, story) do
    source_count =
      state.story_events
      |> Enum.filter(&(&1.story_id == story.id))
      |> Enum.map(& &1.input_id)
      |> Enum.uniq()
      |> length()

    agent_salience = output["topic_salience"] || %{}

    Map.merge(agent_salience, %{
      "related_source_count" => source_count,
      "distinct_source_count" => source_count,
      "material_update_count" =>
        Enum.count(
          state.story_events,
          &(&1.story_id == story.id and material_event_classification?(&1.classification))
        ),
      "freshness" => story.state,
      "user_priority_affinity" =>
        if(watched_story?(state, story), do: "flynn_priority", else: "unavailable")
    })
    |> Map.put_new("durable_topic_nodes", %{
      "state" => "unavailable",
      "reason" => "story_synthesis_topic_node_model_not_committed"
    })
  end

  defp source_coverage_rows(state, story, card, input, admission, output, provenance_refs) do
    output_coverage = output["source_coverage"] || []
    linked_inputs = story_inputs_for_card(state, story.id)

    linked_inputs
    |> Enum.uniq_by(&Admission.input_ref/1)
    |> Enum.map(fn linked_input ->
      source_ref = Admission.input_ref(linked_input)
      agent_row = Enum.find(output_coverage, &(Map.get(&1, "source_ref") == source_ref)) || %{}
      normalized = linked_input.normalized || %{}
      canonical = normalized["canonical_uri"]
      source_name = normalized["source_name"]
      host = if is_binary(canonical) and canonical != "", do: URI.parse(canonical).host, else: nil

      evidence_refs =
        evidence_refs_for_input(state, story.id, linked_input.id, admission.evidence_refs)

      ChangesetStore.insert!(StorySourceCoverage, %{
        tenant_id: state.tenant_id,
        story_id: story.id,
        story_card_version_id: card.id,
        source_ref: source_ref,
        article_ref: linked_input.external_id,
        canonical_public_url:
          availability_field(canonical, "canonical_public_url_unavailable", provenance_refs),
        source_domain: availability_field(host, "source_domain_unavailable", provenance_refs),
        source_label:
          availability_field(source_name, "source_label_unavailable", provenance_refs),
        publication: availability_field(source_name, "publication_unavailable", provenance_refs),
        source_posture:
          Map.get(agent_row, "source_posture", %{
            "state" => "unavailable",
            "reason" => "not_supplied"
          }),
        contribution_reason:
          field_value(
            Map.get(agent_row, "contribution_reason"),
            nil,
            provenance_refs
          ),
        materiality: Map.get(agent_row, "materiality", "unavailable"),
        source_weight:
          Map.get(agent_row, "source_weight", %{
            "state" => "unavailable",
            "reason" => "not_supplied"
          }),
        first_observed_at: linked_input.observed_at,
        last_observed_at: linked_input.observed_at,
        evidence_refs: evidence_refs,
        provenance_refs: provenance_refs
      })
    end)
    |> case do
      [] ->
        [
          ChangesetStore.insert!(StorySourceCoverage, %{
            tenant_id: state.tenant_id,
            story_id: story.id,
            story_card_version_id: card.id,
            source_ref: admission.source_ref,
            article_ref: input.external_id,
            canonical_public_url:
              availability_field(nil, "canonical_public_url_unavailable", provenance_refs),
            source_domain: availability_field(nil, "source_domain_unavailable", provenance_refs),
            source_label: availability_field(nil, "source_label_unavailable", provenance_refs),
            publication: availability_field(nil, "publication_unavailable", provenance_refs),
            source_posture: %{"state" => "unavailable", "reason" => "not_supplied"},
            contribution_reason: field_value(nil, nil, provenance_refs),
            materiality: "unavailable",
            source_weight: %{"state" => "unavailable", "reason" => "not_supplied"},
            first_observed_at: input.observed_at,
            last_observed_at: input.observed_at,
            evidence_refs: admission.evidence_refs,
            provenance_refs: provenance_refs
          })
        ]

      rows ->
        rows
    end
  end

  defp key_claim_rows(state, story, card, output, _fallback_evidence_refs) do
    output_claims = output["key_claims"] || []

    output_claims
    |> Enum.filter(&valid_agent_claim?/1)
    |> Enum.map(fn claim ->
      ChangesetStore.insert!(StoryKeyClaim, %{
        tenant_id: state.tenant_id,
        story_id: story.id,
        story_card_version_id: card.id,
        claim_ref: claim["claim_ref"],
        text: claim["text"] || claim["value"] || claim["claim_ref"],
        status: claim_status(claim["status"]),
        materiality: claim["materiality"] || "material",
        evidence_refs: claim["evidence_refs"],
        conflict_refs: claim["conflict_refs"] || [],
        uncertainty:
          claim["uncertainty"] || %{"state" => "unavailable", "reason" => "not_supplied"},
        appears_in_current_card: Map.get(claim, "appears_in_current_card", true)
      })
    end)
  end

  defp valid_agent_claim?(%{"claim_ref" => ref, "evidence_refs" => [_ | _]} = claim)
       when is_binary(ref) and ref != "" do
    text = claim["text"] || claim["value"] || claim["claim_ref"]
    is_binary(text) and text != ""
  end

  defp valid_agent_claim?(_claim), do: false

  defp availability_field(value, _unavailable_reason, provenance_refs)
       when is_binary(value) and value != "" do
    %{"value" => value, "state" => "complete", "provenance_refs" => provenance_refs}
  end

  defp availability_field(_value, unavailable_reason, provenance_refs) do
    %{
      "value" => nil,
      "state" => "unavailable",
      "reason" => unavailable_reason,
      "provenance_refs" => provenance_refs
    }
  end

  defp story_inputs_for_card(state, story_id) do
    state.story_events
    |> Enum.filter(&(&1.story_id == story_id))
    |> Enum.map(fn event -> Enum.find(state.inputs, &(&1.id == event.input_id)) end)
    |> Enum.reject(&is_nil/1)
  end

  defp evidence_refs_for_input(state, story_id, input_id, fallback) do
    story_subjects =
      (state.story_events ++ state.story_fact_versions)
      |> Enum.filter(&(&1.story_id == story_id))
      |> Enum.map(& &1.id)
      |> MapSet.new()

    state.evidence_refs
    |> Enum.filter(&(&1.input_id == input_id and MapSet.member?(story_subjects, &1.subject_id)))
    |> Enum.map(& &1.evidence_label)
    |> Enum.uniq()
    |> non_empty_list(fallback)
  end

  defp changed_field_keys(output, nil),
    do:
      Map.get(output, "changed_field_keys", [
        "title",
        "deck",
        "summary",
        "source_coverage",
        "key_claims"
      ])

  defp changed_field_keys(output, _prior),
    do:
      Map.get(output, "changed_field_keys", ["deck", "summary", "source_coverage", "key_claims"])

  defp refresh_reason("split"), do: "story_created"
  defp refresh_reason("attach"), do: "source_linked"
  defp refresh_reason("no_op"), do: "source_linked"
  defp refresh_reason("duplicate"), do: "source_linked"
  defp refresh_reason("color"), do: "source_linked"
  defp refresh_reason("stale"), do: "stale_recheck"
  defp refresh_reason("active_story_recurring_15m"), do: "active_story_recurring_15m"
  defp refresh_reason("story_card_hourly_synthesis"), do: "story_card_hourly_synthesis"
  defp refresh_reason("daily_deep_soup_sweep"), do: "daily_deep_soup_sweep"
  defp refresh_reason(_), do: "source_content_changed"

  defp refresh_reason_for_cadence(:active_story_transform_detect_link_15m),
    do: "active_story_recurring_15m"

  defp refresh_reason_for_cadence(:hourly_story_card_synthesis),
    do: "story_card_hourly_synthesis"

  defp refresh_reason_for_cadence(:daily_deep_soup_sweep), do: "daily_deep_soup_sweep"
  defp refresh_reason_for_cadence(_), do: "manual_review"

  defp claim_status(status)
       when status in ["current", "disputed", "stale", "background", "unresolved"], do: status

  defp claim_status(_), do: "current"

  defp non_empty_list([], fallback), do: non_empty_list(nil, fallback)

  defp non_empty_list(nil, fallback), do: fallback

  defp non_empty_list(list, _fallback) when is_list(list), do: list

  defp watched_story?(state, story) do
    story_node_id = Map.get(state.source_ids, {:node, story.story_key})

    Enum.any?(state.edges, fn edge ->
      edge.edge_type == "watch_applies_to" and edge.to_node_id == story_node_id
    end)
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

  defp config(:story_synthesis) do
    config(:story_synthesis, "story-synthesis.v1.t1312.story-cards", @synthesis_prompt, %{
      status: "complete | incomplete | refused | unavailable",
      title: %{text: "string", state: "complete | unavailable", provenance_refs: ["string"]},
      deck: %{
        text: "string or null",
        state: "complete | unavailable",
        reason: "string or null",
        provenance_refs: ["string"]
      },
      summary: %{
        text: "string or null",
        state: "complete | unavailable",
        reason: "string or null",
        provenance_refs: ["string"]
      },
      key_claims: [
        %{
          claim_ref: "claim ref",
          text: "claim text",
          status: "current | disputed | stale | background | unresolved",
          materiality: "material | background | unresolved",
          evidence_refs: ["evidence ref"],
          conflict_refs: ["conflict ref"],
          uncertainty: %{state: "known | unavailable", reason: "string or null"},
          appears_in_current_card: true
        }
      ],
      source_coverage: [
        %{
          source_ref: "source ref",
          contribution_reason: %{
            text: "why this article belongs to the story",
            state: "complete",
            provenance_refs: ["string"]
          },
          materiality: "material | nonmaterial | unavailable",
          source_posture: %{state: "complete | unavailable", value: "string or null"},
          source_weight: %{state: "complete | unavailable", value: "number or null"}
        }
      ],
      topic_salience: %{
        salience_explanation: "story-to-topic salience explanation or unavailable state",
        global_salience: "hint",
        flynn_priority: "hint"
      },
      changed_field_keys: ["field_key"],
      change_summary: %{
        text: "story-agent-authored change summary",
        state: "complete",
        provenance_refs: ["string"]
      },
      field_completeness: %{}
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

  defp event_classification(_output, true, _hint), do: "split"

  defp event_classification(output, false, hint) do
    case classification(output, "substantive_update") do
      "duplicate" -> "duplicate"
      "no_op" -> "no_op"
      "repeated_noop_input" -> "no_op"
      "adds_color" -> "color"
      "stale" -> "stale"
      _ -> hinted_event_classification(hint)
    end
  end

  defp hinted_event_classification(%{suggested_classification: "no_op"}), do: "no_op"

  defp hinted_event_classification(%{"suggested_classification" => "no_op"}), do: "no_op"

  defp hinted_event_classification(_hint), do: "attach"

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
