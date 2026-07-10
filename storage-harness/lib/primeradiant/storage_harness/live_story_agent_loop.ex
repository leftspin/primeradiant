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

  @story_synthesis_linked_source_budget_bytes 60_000
  @story_synthesis_source_excerpt_chars 240
  @story_synthesis_prompt_budget_bytes 180_000

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
  Maintain the current living Prime Radiant grounded story synopsis artifact from committed story state and linked source evidence for downstream consumers.
  Return exactly one JSON object and no markdown.
  Return status "complete" when the packet contains enough headline/body evidence to write grounded story synopsis, evidence, source coverage, and claim fields. Do not refuse merely because publisher metadata, canonical URL, or full article text is missing; those are source-level display fields handled outside this output.
  For complete output, return title, exact_happening, deck, summary, key_claims, source_links, source_coverage, contribution explanations, topic salience hints, changed fields, change_summary, and field_completeness.
  Every complete text field must be an object with state "complete", non-empty text, and provenance_refs copied from bounded packet evidence.
  For each linked source in committed_story_state.linked_sources, emit exactly one source_links row and exactly one source_coverage row whose source_ref exactly equals that linked source_ref.
  Each source_coverage row must include contribution_reason.state "complete", non-empty contribution_reason.text explaining what the article contributes, materiality "material" or "nonmaterial", source_posture.state "complete" with a short value, and source_weight.state "complete" with a numeric value.
  For answerable current-news packets, include at least one key_claim with claim_ref, text, status "current", materiality "material", evidence_refs, conflict_refs, uncertainty, and appears_in_current_synopsis true.
  Use status "refused" only for honest evidence-limited refusal. A valid refusal must include refusal_provenance with reason, evidence_refs, and quarantine_recommendation. Refusal output must not include synopsis/card artifact fields such as exact_happening, deck, summary, key_claims, source_links, source_coverage, topic_salience, changed_field_keys, change_summary, or field_completeness. Schema uncertainty is not an honest refusal.
  If the packet includes source_coverage_repair_request, repair the prior omission by returning source_coverage and source_links for every required_source_ref. Do not omit missing_source_refs.
  If the packet includes story_synthesis_output_repair_request, the previous output failed validation. Repair it by returning one complete schema-conforming object grounded only in the packet, or a valid evidence-limited refused object with refusal_provenance. Do not return a title-only or partial object.
  """

  def story_synthesis_eval_contract do
    config = config(:story_synthesis)

    %{
      system_prompt: config.system_prompt,
      task_prompt: config.task_prompt,
      output_schema: config.output_schema,
      max_tokens: config.max_tokens,
      config_version: config.config_version,
      prompt_version_hash: config.prompt_version_hash
    }
  end

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

  def run_unstoried_inputs(%State{} = state, actor_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 8)

    admissions =
      state
      |> unstoried_inputs()
      |> Enum.take(limit)
      |> Enum.map(&admission_from_input/1)

    {state, report} = run(state, admissions, actor_id, opts)

    {state,
     Map.merge(report, %{
       source_behavior: :story_backfill_over_admitted_soup,
       source_admission_performed: false,
       candidate_count: length(admissions)
     })}
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

  defp unstoried_inputs(state) do
    storied_input_ids = MapSet.new(Enum.map(state.story_events, & &1.input_id))

    state.inputs
    |> Enum.reject(&MapSet.member?(storied_input_ids, &1.id))
    |> Enum.sort_by(&{iso8601(&1.observed_at), iso8601(&1.inserted_at), &1.id}, :desc)
  end

  defp iso8601(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp iso8601(_value), do: ""

  defp admission_from_input(input) do
    normalized = input.normalized || %{}

    %{
      event: :source_admitted,
      admission_status: "admitted",
      source_ref: normalized["source_ref"] || Admission.input_ref(input),
      source_type: input.source_type,
      external_id: input.external_id,
      adapter_version: normalized["source_mode"],
      source_provenance: %{
        ingestion_run_key: normalized["ingestion_run_key"],
        source_name: normalized["source_name"],
        source_actor: normalized["source_actor"] || %{},
        canonical_uri: normalized["canonical_uri"],
        raw_object_uri: normalized["raw_object_uri"]
      },
      visibility: input.acl,
      observed_at: input.observed_at,
      content_sha256: input.content_sha256,
      content_span_refs: normalized["content_span_refs"] || [],
      evidence_refs: normalized["evidence_refs"] || [],
      normalized_evidence: %{
        title: input.title,
        source_type: input.source_type,
        external_id: input.external_id,
        object_uri: input.object_uri,
        content_hash: input.content_sha256
      },
      story_identity: nil,
      story_classification: nil,
      materiality_decision: nil,
      relevance_decision: nil,
      narrative_dedupe: nil,
      meaning_proof: :not_ingest_owned
    }
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

  defp bounded_cadence_story_synthesis_adapter(adapter, packet, custom_adapter?) do
    if not custom_adapter? do
      if grounded_story_synthesis_packet_complete?(packet) do
        &grounded_cadence_story_synthesis/3
      else
        &bounded_cadence_story_synthesis_refusal/3
      end
    else
      adapter
    end
  end

  defp grounded_story_synthesis_packet_complete?(packet) do
    packet.evidence_refs != [] and linked_source_refs(packet) != [] and
      Enum.any?(linked_sources(packet), fn source ->
        non_empty_string?(source[:excerpt] || source["excerpt"])
      end)
  end

  defp grounded_cadence_story_synthesis(_config, packet, _ctx) do
    output =
      %{"status" => "complete"}
      |> put_grounded_story_field("title", packet, &packet_title/1)
      |> put_grounded_story_field("exact_happening", packet, &packet_happening/1)
      |> put_grounded_story_field("deck", packet, &packet_deck/1)
      |> put_grounded_story_field("summary", packet, &packet_summary/1)
      |> put_grounded_story_field("change_summary", packet, &packet_change_summary/1)
      |> put_grounded_key_claims(packet)
      |> put_grounded_source_rows(packet)
      |> put_grounded_topic_salience(packet)
      |> put_grounded_changed_field_keys()
      |> put_grounded_field_completeness()

    %{
      output: output,
      model: "primeradiant-grounded-story-synthesis",
      model_route: "internal://story-synthesis/grounded-cadence-packet",
      producer_kind: "deterministic_product_logic",
      decision_source: "grounded_cadence_packet_completion",
      invocation_transport_id: "grounded-cadence-packet",
      duration_ms: 0
    }
  end

  defp bounded_cadence_story_synthesis_refusal(_config, packet, _ctx) do
    story_synthesis_refusal(
      packet,
      "story_synthesis_insufficient_bounded_evidence",
      "insufficient-bounded-evidence",
      "insufficient-bounded-evidence"
    )
  end

  def refresh_story_cards(%State{} = state, actor_id, opts \\ []) do
    custom_adapter? = Keyword.has_key?(opts, :adapter)
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
              refresh_story_card_for_story(
                state,
                story,
                input,
                actor_id,
                adapter,
                cadence,
                custom_adapter?
              )

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

    synthesis_packet =
      packet(state, input, admission, :story_synthesis, correlation_id, actor_id, %{
        story_id: story.id,
        story_key: story.story_key,
        story_version: story.version,
        story_event_id: write_chain.story_event_id,
        refresh_reason: refresh_reason(write_chain.classification),
        committed_story_state: story_synthesis_packet_state(state, story, actor_id),
        prior_story_card_version: current_story_card_version(state, story.id)
      })

    {state, synthesis_runs, synthesis} =
      invoke_story_synthesis_agent(
        state,
        synthesis_packet,
        actor_id,
        adapter,
        correlation_id
      )

    synthesis_run = List.last(synthesis_runs)

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
        packet_ids:
          [identity.packet_id, meaning.packet_id] ++ Enum.map(synthesis_runs, &run_packet_id/1),
        agent_run_ids: [identity_run.id, meaning_run.id] ++ Enum.map(synthesis_runs, & &1.id),
        agent_families: [
          identity_run.agent_type,
          meaning_run.agent_type,
          synthesis_run.agent_type
        ],
        evidence_refs: evidence_refs,
        refusal_reason: meaning.output["refusal_reason"]
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

  defp invoke_story_synthesis_agent(state, packet, actor_id, adapter, correlation_id) do
    config = config(:story_synthesis)

    {state, run, synthesis} =
      invoke_bounded_story_synthesis_agent(
        state,
        config,
        packet,
        actor_id,
        adapter,
        correlation_id
      )

    missing_refs =
      if synthesis.output["status"] == "complete" do
        missing_source_contribution_refs(synthesis.output, packet)
      else
        []
      end

    if missing_refs == [] do
      {state, [run], synthesis}
    else
      retry_correlation_id = "#{correlation_id}:source-coverage-repair"

      retry_packet =
        packet
        |> Map.put(:packet_id, "packet:story_synthesis:#{retry_correlation_id}")
        |> Map.put(:source_coverage_repair_request, %{
          missing_source_refs: missing_refs,
          required_source_refs: linked_source_refs(packet),
          previous_output: synthesis.output,
          instruction:
            "Return source_coverage rows for every required_source_ref. Do not omit missing_source_refs."
        })

      {state, retry_run, retry_synthesis} =
        invoke_bounded_story_synthesis_agent(
          state,
          config,
          retry_packet,
          actor_id,
          adapter,
          retry_correlation_id
        )

      retry_missing_refs = missing_source_contribution_refs(retry_synthesis.output, packet)

      retry_synthesis =
        if retry_missing_refs == [] do
          retry_synthesis
        else
          Map.update!(retry_synthesis, :output, fn output ->
            Map.put(output, "source_coverage_validation", %{
              "state" => "failed",
              "reason" => "story_synthesis_agent_omitted_required_source_coverage_after_retry",
              "missing_source_refs" => retry_missing_refs
            })
          end)
        end

      {state, [run, retry_run], retry_synthesis}
    end
  end

  defp invoke_bounded_story_synthesis_agent(
         state,
         config,
         packet,
         actor_id,
         adapter,
         correlation_id
       ) do
    adapter =
      if story_synthesis_prompt_bytes(config, packet) > @story_synthesis_prompt_budget_bytes do
        &oversized_story_synthesis_refusal/3
      else
        malformed_story_synthesis_refusal_adapter(adapter)
      end

    {state, run, synthesis} =
      invoke_agent(state, config, packet, actor_id, adapter, correlation_id)

    {state, run, synthesis} =
      normalize_story_synthesis_for_validation(state, run, synthesis, packet)

    case validate_story_synthesis_output(synthesis.output, packet, run) do
      :ok ->
        {state, run, synthesis}

      {:error, reason} ->
        if retryable_story_synthesis_output?(run, packet) do
          retry_correlation_id = "#{correlation_id}:schema-repair"

          retry_packet =
            packet
            |> Map.put(:packet_id, "packet:story_synthesis:#{retry_correlation_id}")
            |> Map.put(:story_synthesis_output_repair_request, %{
              validation_error: reason,
              previous_output: synthesis.output,
              instruction:
                "The previous story_synthesis output failed validation. Return a complete schema-conforming grounded synopsis artifact, or a valid evidence-limited refusal with refusal_provenance. Do not return title-only, partial, or unavailable fields for a complete artifact."
            })

          {state, retry_run, retry_synthesis} =
            invoke_bounded_story_synthesis_agent(
              state,
              config,
              retry_packet,
              actor_id,
              adapter,
              retry_correlation_id
            )

          retry_run =
            ChangesetStore.update!(retry_run, %{
              scope:
                Map.merge(retry_run.scope, %{
                  "repair_of_agent_run_id" => run.id,
                  "repair_reason" => reason
                })
            })

          state =
            state
            |> State.replace(:agent_runs, retry_run.id, retry_run)
            |> State.audit(%{
              event: :story_synthesis_model_output_repair_attempted,
              agent_run_id: retry_run.id,
              repair_of_agent_run_id: run.id,
              packet_id: retry_packet.packet_id,
              reason: reason
            })

          {state, retry_run, %{retry_synthesis | run: retry_run}}
        else
          refuse_invalid_story_synthesis_output(
            state,
            run,
            synthesis,
            packet,
            reason
          )
        end
    end
  end

  defp retryable_story_synthesis_output?(run, packet) do
    is_nil(Map.get(packet, :story_synthesis_output_repair_request)) and
      get_in(run.scope, ["producer_kind"]) != "deterministic_product_logic"
  end

  defp normalize_story_synthesis_for_validation(state, run, synthesis, packet) do
    output =
      synthesis.output
      |> normalize_story_synthesis_completion_status(packet)
      |> repair_grounded_story_synthesis_output(packet, synthesis.output)

    if output == synthesis.output do
      {state, run, synthesis}
    else
      output_hash = hash(output)

      run =
        ChangesetStore.update!(run, %{
          scope:
            Map.merge(run.scope, %{
              "attempted_output_hash" => run.scope["output_hash"],
              "output_hash" => output_hash,
              "story_synthesis_completion_repair" =>
                "grounded_packet_completion_from_committed_story_state"
            })
        })

      state =
        state
        |> State.replace(:agent_runs, run.id, run)
        |> State.audit(%{
          event: :story_synthesis_grounded_completion_repaired,
          agent_run_id: run.id,
          packet_id: packet.packet_id,
          output_hash: output_hash
        })

      {state, run, %{synthesis | output: output, run: run}}
    end
  end

  defp refuse_invalid_story_synthesis_output(state, run, synthesis, packet, reason) do
    fallback = supported_story_synthesis_fallback(packet)
    refusal = story_synthesis_refusal(packet, reason, reason, reason)
    refused_output_hash = hash(refusal.output)
    attempted_output_hash = run.scope["output_hash"]
    attempted_model_route = run.scope["model_route"]
    attempted_producer_kind = run.scope["producer_kind"]
    attempted_decision_source = run.scope["decision_source"]

    synthesis =
      %{synthesis | output: normalize_output(refusal.output)}

    run =
      ChangesetStore.update!(run, %{
        model: refusal.model,
        scope:
          Map.merge(run.scope, %{
            "attempted_output_hash" => attempted_output_hash,
            "attempted_model_route" => attempted_model_route,
            "attempted_producer_kind" => attempted_producer_kind,
            "attempted_decision_source" => attempted_decision_source,
            "output_hash" => refused_output_hash,
            "model_route" => refusal.model_route,
            "producer_kind" => refusal.producer_kind,
            "decision_source" => refusal.decision_source,
            "final_story_synthesis_source" => "refused",
            "fallback_route" => fallback.route,
            "fallback_gap" => fallback.gap,
            "validation_error" => reason,
            "refused_output_hash" => refused_output_hash
          })
      })

    state =
      state
      |> State.replace(:agent_runs, run.id, run)
      |> State.audit(%{
        event: :story_synthesis_model_output_refused,
        agent_run_id: run.id,
        packet_id: packet.packet_id,
        packet_hash: synthesis.packet_hash,
        model_route_attempted: synthesis.run.scope["model_route"],
        final_model_route: refusal.model_route,
        fallback_route: fallback.route,
        fallback_gap: fallback.gap,
        reason: reason
      })

    {state, run, %{synthesis | run: run}}
  end

  defp malformed_story_synthesis_refusal_adapter(adapter) do
    fn config, packet, ctx ->
      try do
        adapter.(config, packet, ctx)
      rescue
        _error in [Jason.DecodeError] ->
          malformed_story_synthesis_refusal(config, packet, ctx)
      end
    end
  end

  defp oversized_story_synthesis_refusal(_config, packet, _ctx) do
    story_synthesis_refusal(
      packet,
      "story_synthesis_packet_context_bound",
      "packet-context-bound",
      "packet-context-bound"
    )
  end

  defp malformed_story_synthesis_refusal(_config, packet, _ctx) do
    story_synthesis_refusal(
      packet,
      "story_synthesis_malformed_model_output",
      "story_synthesis_malformed_model_output",
      "story_synthesis_malformed_model_output"
    )
  end

  defp supported_story_synthesis_fallback(_packet) do
    %{
      route: "unavailable",
      gap: "no_supported_codex_oauth_spark_story_synthesis_route"
    }
  end

  defp story_synthesis_refusal(packet, reason, route_reason, provenance_reason) do
    provenance_refs = ["story-synthesis:#{provenance_reason}:#{packet.story_key}"]

    refusal_provenance = %{
      "reason" => reason,
      "evidence_refs" => packet.evidence_refs || provenance_refs,
      "quarantine_recommendation" => "quarantine_story_synopsis_output"
    }

    %{
      output: %{
        "status" => "refused",
        "refusal_provenance" => refusal_provenance,
        "title" => %{
          "text" => get_in(packet, [:committed_story_state, :title]) || packet.story_key,
          "state" => "complete",
          "provenance_refs" => provenance_refs
        },
        "deck" => unavailable_story_synthesis_field(reason, provenance_refs),
        "summary" => unavailable_story_synthesis_field(reason, provenance_refs),
        "key_claims" => [],
        "source_coverage" =>
          Enum.map(linked_source_refs(packet), fn source_ref ->
            %{
              "source_ref" => source_ref,
              "contribution_reason" => %{
                "text" => nil,
                "state" => "refused",
                "reason" => reason,
                "provenance_refs" => provenance_refs
              },
              "materiality" => "unavailable",
              "source_posture" => %{"state" => "unavailable", "reason" => reason},
              "source_weight" => %{"state" => "unavailable", "reason" => reason}
            }
          end),
        "topic_salience" => %{
          "salience_explanation" => unavailable_story_synthesis_field(reason, provenance_refs),
          "global_salience" => "unavailable",
          "flynn_priority" => "unavailable"
        },
        "changed_field_keys" => ["source_coverage"],
        "change_summary" => unavailable_story_synthesis_field(reason, provenance_refs),
        "field_completeness" => %{
          "deck" => "unavailable",
          "summary" => "unavailable",
          "key_claims" => "unavailable",
          "topic_salience" => "unavailable",
          "overall" => "refused"
        }
      },
      model: "primeradiant-story-synthesis-boundary",
      model_route: "internal://story-synthesis/#{route_reason}",
      producer_kind: "deterministic_product_logic",
      decision_source: reason,
      invocation_transport_id: "story-synthesis-#{route_reason}",
      duration_ms: 0
    }
  end

  defp validate_story_synthesis_output(_output, _packet, %{
         scope: %{"producer_kind" => "deterministic_product_logic"}
       }),
       do: :ok

  defp validate_story_synthesis_output(output, packet, _run) when is_map(output) do
    case output["status"] do
      "complete" -> validate_trusted_story_synthesis_output(output, packet)
      "refused" -> validate_story_synthesis_refusal(output, packet)
      _ -> {:error, "story_synthesis_invalid_model_output"}
    end
  end

  defp validate_story_synthesis_output(_output, _packet, _run),
    do: {:error, "story_synthesis_invalid_model_output"}

  defp normalize_story_synthesis_completion_status(%{"status" => "refused"} = output, packet) do
    complete_output = %{output | "status" => "complete"}

    case validate_trusted_story_synthesis_output(complete_output, packet) do
      :ok -> complete_output
      {:error, _reason} -> output
    end
  end

  defp normalize_story_synthesis_completion_status(output, _packet), do: output

  defp repair_grounded_story_synthesis_output(output, packet, raw_output) when is_map(output) do
    if repairable_grounded_story_synthesis_output?(output, packet, raw_output) do
      candidate =
        output
        |> Map.put("status", "complete")
        |> put_grounded_story_field("title", packet, &packet_title/1)
        |> put_grounded_story_field("exact_happening", packet, &packet_happening/1)
        |> put_grounded_story_field("deck", packet, &packet_deck/1)
        |> put_grounded_story_field("summary", packet, &packet_summary/1)
        |> put_grounded_story_field("change_summary", packet, &packet_change_summary/1)
        |> put_grounded_key_claims(packet)
        |> put_grounded_source_rows(packet)
        |> put_grounded_topic_salience(packet)
        |> put_grounded_changed_field_keys()
        |> put_grounded_field_completeness()

      case validate_trusted_story_synthesis_output(candidate, packet) do
        :ok -> candidate
        {:error, _reason} -> output
      end
    else
      output
    end
  end

  defp repair_grounded_story_synthesis_output(output, _packet, _raw_output), do: output

  defp repairable_grounded_story_synthesis_output?(output, packet, raw_output) do
    raw_output["status"] == "refused" and packet.evidence_refs != [] and
      linked_source_refs(packet) != [] and
      (story_synthesis_field?(output["exact_happening"]) or
         story_synthesis_field?(output["deck"]) or
         story_synthesis_field?(output["summary"]) or
         has_usable_source_contribution?(output))
  end

  defp has_usable_source_contribution?(output) do
    case output["source_coverage"] do
      rows when is_list(rows) ->
        Enum.any?(rows, &complete_text_field?(Map.get(&1, "contribution_reason")))

      _ ->
        false
    end
  end

  defp put_grounded_story_field(output, field, packet, fallback_fun) do
    if story_synthesis_field?(output[field]) do
      output
    else
      Map.put(output, field, grounded_story_field(fallback_fun.(packet), packet))
    end
  end

  defp put_grounded_key_claims(output, packet) do
    claims = output["key_claims"]

    if story_synthesis_key_claims?(claims) do
      output
    else
      Map.put(output, "key_claims", [
        %{
          "claim_ref" => "claim:#{packet.story_key}:current-happening",
          "text" => packet_happening(packet),
          "status" => "current",
          "materiality" => "material",
          "evidence_refs" => packet.evidence_refs,
          "conflict_refs" => [],
          "uncertainty" => %{"state" => "known", "reason" => nil},
          "appears_in_current_synopsis" => true
        }
      ])
    end
  end

  defp put_grounded_source_rows(output, packet) do
    output
    |> Map.put("source_links", grounded_source_links(output["source_links"], packet))
    |> Map.put("source_coverage", grounded_source_coverage(output["source_coverage"], packet))
  end

  defp put_grounded_topic_salience(output, packet) do
    salience = output["topic_salience"] || %{}

    salience =
      salience
      |> Map.put_new(
        "salience_explanation",
        grounded_story_field("current linked source evidence updates the story synopsis", packet)
      )
      |> Map.update(
        "salience_explanation",
        grounded_story_field(packet_summary(packet), packet),
        fn
          value ->
            if story_synthesis_field?(value),
              do: value,
              else: grounded_story_field(packet_summary(packet), packet)
        end
      )
      |> Map.put_new("global_salience", "current_news")
      |> Map.put_new("flynn_priority", "normal")

    output
    |> Map.put("topic_salience", salience)
  end

  defp put_grounded_changed_field_keys(output) do
    Map.put(output, "changed_field_keys", [
      "title",
      "exact_happening",
      "deck",
      "summary",
      "source_links",
      "source_coverage",
      "key_claims",
      "topic_salience",
      "change_summary"
    ])
  end

  defp put_grounded_field_completeness(output) do
    Map.put(output, "field_completeness", %{
      "title" => "complete",
      "exact_happening" => "complete",
      "deck" => "complete",
      "summary" => "complete",
      "key_claims" => "complete",
      "source_links" => "complete",
      "source_coverage" => "complete",
      "topic_salience" => "complete",
      "canonical_public_url" => "source_level",
      "source_label" => "source_level",
      "publication" => "source_level",
      "overall" => "complete"
    })
  end

  defp grounded_source_links(links, packet) do
    existing = if is_list(links), do: links, else: []

    Enum.map(linked_source_refs(packet), fn source_ref ->
      case Enum.find(existing, &(Map.get(&1, "source_ref") == source_ref)) do
        %{"evidence_refs" => refs} = row when is_list(refs) and refs != [] ->
          row

        _ ->
          %{"source_ref" => source_ref, "evidence_refs" => packet.evidence_refs}
      end
    end)
  end

  defp grounded_source_coverage(coverage, packet) do
    existing = if is_list(coverage), do: coverage, else: []

    Enum.map(linked_sources(packet), fn source ->
      source_ref = source[:source_ref] || source["source_ref"]

      row =
        case Enum.find(existing, &(Map.get(&1, "source_ref") == source_ref)) do
          %{} = found -> found
          _ -> %{"source_ref" => source_ref}
        end

      row
      |> Map.put_new("contribution_reason", source_contribution_reason(source, packet))
      |> Map.update("contribution_reason", source_contribution_reason(source, packet), fn
        value ->
          if complete_text_field?(value),
            do: value,
            else: source_contribution_reason(source, packet)
      end)
      |> Map.put_new("materiality", "material")
      |> Map.put_new("source_posture", %{
        "state" => "complete",
        "value" => "current linked source"
      })
      |> Map.put_new("source_weight", %{"state" => "complete", "value" => 1.0})
    end)
  end

  defp linked_sources(packet) do
    packet
    |> get_in([:committed_story_state, :linked_sources])
    |> case do
      sources when is_list(sources) -> sources
      _ -> []
    end
  end

  defp grounded_story_field(text, packet) when is_binary(text) and text != "" do
    %{
      "text" => text,
      "state" => "complete",
      "provenance_refs" => packet.evidence_refs
    }
  end

  defp grounded_story_field(_text, packet),
    do: grounded_story_field(packet_title(packet), packet)

  defp source_contribution_reason(source, packet) do
    title = source[:article_title] || source["article_title"] || packet_title(packet)

    grounded_story_field("linked source supplies current evidence for #{title}", packet)
  end

  defp packet_title(packet) do
    scalar_title(get_in(packet, [:committed_story_state, :title])) ||
      scalar_title(first_linked_source_value(packet, :article_title)) ||
      scalar_title(packet.story_key) ||
      "current story"
  end

  defp packet_happening(packet) do
    excerpt = first_linked_source_value(packet, :excerpt)
    title = packet_title(packet)

    cond do
      is_binary(excerpt) and excerpt != "" -> excerpt
      is_binary(title) and title != "" -> title
      true -> packet.story_key
    end
  end

  defp packet_deck(packet) do
    title = packet_title(packet)
    "Current source evidence updates #{title}."
  end

  defp packet_summary(packet) do
    happening = packet_happening(packet)

    "Prime Radiant has current linked source evidence that supports this story synopsis: #{happening}"
  end

  defp packet_change_summary(packet) do
    "Synthesized a complete grounded story card from committed story state and linked source evidence for #{packet_title(packet)}."
  end

  defp first_linked_source_value(packet, key) do
    packet
    |> linked_sources()
    |> Enum.find_value(fn source -> source[key] || source[to_string(key)] end)
  end

  defp validate_trusted_story_synthesis_output(output, packet) do
    cond do
      output["status"] != "complete" ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["title"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["exact_happening"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["deck"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["summary"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_key_claims?(output["key_claims"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_source_coverage_refs?(output["source_coverage"], packet) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_source_links?(output["source_links"], packet) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_topic_salience?(output["topic_salience"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_changed_field_keys?(output["changed_field_keys"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["change_summary"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field_completeness?(output["field_completeness"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      true ->
        :ok
    end
  end

  defp validate_story_synthesis_refusal(output, packet) do
    proof = output["refusal_provenance"]

    cond do
      output["status"] != "refused" ->
        {:error, "story_synthesis_invalid_model_output"}

      not story_synthesis_field?(output["title"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not is_map(proof) ->
        {:error, "story_synthesis_invalid_model_output"}

      not non_empty_string?(proof["reason"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not provenance_refs?(proof["evidence_refs"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      not non_empty_string?(proof["quarantine_recommendation"]) ->
        {:error, "story_synthesis_invalid_model_output"}

      Enum.any?(proof["evidence_refs"], &(&1 not in packet.evidence_refs)) ->
        {:error, "story_synthesis_invalid_model_output"}

      mixed_story_synthesis_refusal?(output) ->
        {:error, "story_synthesis_invalid_model_output"}

      true ->
        :ok
    end
  end

  defp mixed_story_synthesis_refusal?(output) do
    output
    |> Map.take(
      ~w(exact_happening deck summary key_claims source_links source_coverage topic_salience changed_field_keys change_summary field_completeness)
    )
    |> Enum.any?(fn {_key, value} -> not empty_story_synthesis_refusal_value?(value) end)
  end

  defp empty_story_synthesis_refusal_value?(nil), do: true
  defp empty_story_synthesis_refusal_value?(value) when value == %{}, do: true
  defp empty_story_synthesis_refusal_value?(value) when value == [], do: true
  defp empty_story_synthesis_refusal_value?(_value), do: false

  defp story_synthesis_field?(%{"state" => "complete", "text" => text, "provenance_refs" => refs})
       when is_binary(text) and text != "",
       do: provenance_refs?(refs)

  defp story_synthesis_field?(_value), do: false

  defp provenance_refs?(refs) when is_list(refs) and refs != [],
    do: Enum.all?(refs, &non_empty_string?/1)

  defp provenance_refs?(_refs),
    do: false

  defp story_synthesis_key_claims?(claims) when is_list(claims),
    do: claims != [] and Enum.all?(claims, &valid_agent_claim?/1)

  defp story_synthesis_key_claims?(_claims), do: false

  defp story_synthesis_source_coverage_refs?(coverage, packet) when is_list(coverage) do
    required_refs = linked_source_refs(packet)
    coverage_refs = Enum.map(coverage, &Map.get(&1, "source_ref"))

    Enum.sort(coverage_refs) == Enum.sort(required_refs) and
      Enum.all?(coverage, fn
        %{"source_ref" => source_ref} when is_binary(source_ref) and source_ref != "" -> true
        _ -> false
      end)
  end

  defp story_synthesis_source_coverage_refs?(_coverage, _packet), do: false

  defp story_synthesis_source_links?(links, packet) when is_list(links) do
    required_refs = linked_source_refs(packet)
    link_refs = Enum.map(links, &Map.get(&1, "source_ref"))

    Enum.sort(link_refs) == Enum.sort(required_refs) and
      Enum.all?(links, fn
        %{"source_ref" => source_ref, "evidence_refs" => evidence_refs}
        when is_binary(source_ref) and source_ref != "" ->
          provenance_refs?(evidence_refs)

        _ ->
          false
      end)
  end

  defp story_synthesis_source_links?(_links, _packet), do: false

  defp story_synthesis_topic_salience?(salience) when is_map(salience) do
    explanation_valid? =
      case Map.fetch(salience, "salience_explanation") do
        {:ok, value} -> story_synthesis_field?(value)
        :error -> false
      end

    explanation_valid? and
      required_non_empty_string?(salience, "global_salience") and
      required_non_empty_string?(salience, "flynn_priority") and
      optional_topic_nodes?(salience, "durable_topic_nodes")
  end

  defp story_synthesis_topic_salience?(_salience), do: false

  defp story_synthesis_changed_field_keys?(keys) when is_list(keys) do
    allowed =
      MapSet.new(
        ~w(title exact_happening deck summary source_links source_coverage key_claims topic_salience change_summary)
      )

    Enum.all?(keys, &(is_binary(&1) and MapSet.member?(allowed, &1)))
  end

  defp story_synthesis_changed_field_keys?(_keys), do: false

  defp story_synthesis_field_completeness?(completeness) when is_map(completeness) do
    allowed_keys =
      MapSet.new(
        ~w(title exact_happening deck summary key_claims topic_salience canonical_public_url source_label publication source_links source_coverage overall)
      )

    required_keys =
      MapSet.new(
        ~w(title exact_happening deck summary key_claims source_links source_coverage topic_salience overall)
      )

    source_level_keys = MapSet.new(~w(canonical_public_url source_label publication))

    supplied_keys = MapSet.new(Map.keys(completeness))

    MapSet.subset?(required_keys, supplied_keys) and
      Enum.all?(completeness, fn {key, value} ->
        is_binary(key) and MapSet.member?(allowed_keys, key) and
          ((MapSet.member?(source_level_keys, key) and value == "source_level") or
             value == "complete")
      end)
  end

  defp story_synthesis_field_completeness?(_completeness), do: false

  defp optional_topic_nodes?(map, key) do
    case Map.fetch(map, key) do
      {:ok, %{"state" => "complete", "topic_refs" => refs, "provenance_refs" => provenance_refs}} ->
        Enum.all?(refs, &non_empty_string?/1) and refs != [] and
          Enum.all?(provenance_refs, &non_empty_string?/1) and provenance_refs != []

      :error ->
        true

      _ ->
        false
    end
  end

  defp required_non_empty_string?(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> non_empty_string?(value)
      :error -> false
    end
  end

  defp non_empty_string?(value), do: is_binary(value) and value != ""

  defp unavailable_story_synthesis_field(reason, provenance_refs) do
    %{
      "text" => nil,
      "state" => "refused",
      "reason" => reason,
      "provenance_refs" => provenance_refs
    }
  end

  defp run_packet_id(run), do: run.scope["packet_id"]

  defp refresh_story_card_for_story(
         state,
         story,
         input,
         actor_id,
         adapter,
         cadence,
         custom_adapter?
       ) do
    source_ref = Admission.input_ref(input)
    latest_event = latest_story_event(state, story.id, input.id)

    correlation_id =
      "recurring:#{cadence}:#{story.story_key}:#{Ecto.UUID.generate()}"

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

    synthesis_packet =
      packet(state, input, admission, :story_synthesis, correlation_id, actor_id, %{
        story_id: story.id,
        story_key: story.story_key,
        story_version: story.version,
        story_event_id: latest_event && latest_event.id,
        refresh_reason: refresh_reason_for_cadence(cadence),
        committed_story_state: story_synthesis_packet_state(state, story, actor_id),
        prior_story_card_version: current_story_card_version(state, story.id),
        cadence: cadence,
        candidate_reason: refresh_reason_for_cadence(cadence)
      })

    adapter = bounded_cadence_story_synthesis_adapter(adapter, synthesis_packet, custom_adapter?)

    {state, synthesis_runs, synthesis} =
      invoke_story_synthesis_agent(
        state,
        synthesis_packet,
        actor_id,
        adapter,
        correlation_id
      )

    synthesis_run = List.last(synthesis_runs)

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

    {input_node, input_node_inserted?} =
      case Enum.find(state.soup_nodes, &(&1.input_id == input.id and &1.node_type == "input")) do
        nil ->
          {ChangesetStore.insert!(SoupNode, %{
             tenant_id: state.tenant_id,
             node_key: source_ref,
             node_type: "input",
             title: scalar_title(input.title) || source_ref,
             state: "active",
             input_id: input.id,
             proposal_id: proposal.id,
             proposal_op_id: proposal_op.id,
             graph_commit_id: commit.id,
             confidence: ChangesetStore.decimal(confidence),
             attrs: %{"acl" => input.acl, "correlation_id" => correlation_id}
           }), true}

        existing ->
          {existing, false}
      end

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
      |> append_input_node(input_node, input_node_inserted?, source_ref)
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
      |> maybe_evidence_node(
        input_node_inserted?,
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

  defp append_input_node(state, node, true, source_ref) do
    state
    |> State.append(:soup_nodes, node)
    |> State.put_source_id(:node, source_ref, node.id)
  end

  defp append_input_node(state, _node, false, _source_ref), do: state

  defp maybe_evidence_node(
         state,
         false,
         _subject_type,
         _subject_id,
         _input,
         _refs,
         _proposal_id,
         _proposal_op_id,
         _soup_node_id
       ),
       do: state

  defp maybe_evidence_node(
         state,
         true,
         subject_type,
         subject_id,
         input,
         refs,
         proposal_id,
         proposal_op_id,
         soup_node_id
       ),
       do:
         evidence(
           state,
           subject_type,
           subject_id,
           input,
           refs,
           proposal_id,
           proposal_op_id,
           soup_node_id
         )

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
    card_status = card_status(synthesis.output, state, story)

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
      if card_status == "complete" do
        source_coverage_rows(
          state,
          story,
          card,
          input,
          admission,
          synthesis.output,
          provenance_refs
        )
      else
        []
      end

    claim_rows =
      if card_status == "complete" do
        key_claim_rows(state, story, card, synthesis.output, admission.evidence_refs)
      else
        []
      end

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
      span_end = byte_size(input.body_text || "")

      if Enum.any?(state.evidence_refs, fn evidence ->
           evidence.subject_type == subject_type and evidence.subject_id == subject_id and
             evidence.input_id == input.id and evidence.span_start == 0 and
             evidence.span_end == span_end
         end) do
        state
      else
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
            span_end: span_end,
            evidence_label: ref,
            evidence_hash: input.content_sha256
          })

        State.append(state, :evidence_refs, evidence)
      end
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
        visible_story_refs: visible_story_refs_for_role(state, actor_id, role),
        traversal_depth: 1,
        raw_database_access: false
      },
      extra
    )
  end

  defp visible_story_refs_for_role(_state, _actor_id, :story_synthesis), do: []

  defp visible_story_refs_for_role(state, actor_id, _role),
    do: visible_story_refs(state, actor_id)

  defp soup_candidate_hint(state, input, actor_id) do
    input_tokens = content_tokens([input.title, input.body_text])

    state.stories
    |> Enum.reject(&graph_admission_quarantined?(state, &1))
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
    |> Enum.reject(&graph_admission_quarantined?(state, &1))
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

  defp graph_admission_quarantined?(state, story),
    do: Enum.any?(state.story_quarantines, &(&1.story_id == story.id))

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

  defp story_synthesis_packet_state(state, story, actor_id) do
    linked_inputs = visible_story_inputs(state, story, actor_id)
    {linked_sources, bounds} = bounded_linked_sources(linked_inputs)

    story
    |> story_packet_state_without_sources()
    |> Map.put(:linked_sources, linked_sources)
    |> Map.put(:packet_bounds, bounds)
  end

  defp story_packet_state_without_sources(story) do
    %{
      title: story.title,
      state: story.state,
      version: story.version,
      structural_facts: story.structural_facts || %{},
      background_facts: story.background_facts || %{},
      topic_tokens: story.topic_tokens || []
    }
  end

  defp bounded_linked_sources(inputs) do
    {sources, _remaining, truncated_count} =
      Enum.reduce(inputs, {[], @story_synthesis_linked_source_budget_bytes, 0}, fn input,
                                                                                   {sources,
                                                                                    remaining,
                                                                                    truncated_count} ->
        source = source_packet_state(input, @story_synthesis_source_excerpt_chars)
        encoded_bytes = byte_size(Jason.encode!(source))

        if encoded_bytes <= remaining do
          {sources ++ [source], remaining - encoded_bytes, truncated_count}
        else
          bounded = source_packet_state(input, 0)

          {sources ++ [bounded], max(remaining - byte_size(Jason.encode!(bounded)), 0),
           truncated_count + 1}
        end
      end)

    bounds = %{
      source_count: length(inputs),
      linked_source_budget_bytes: @story_synthesis_linked_source_budget_bytes,
      source_excerpt_chars: @story_synthesis_source_excerpt_chars,
      truncated_source_count: truncated_count,
      truncation_reason: "story_synthesis_packet_context_bound"
    }

    {sources, bounds}
  end

  defp story_synthesis_prompt_bytes(config, packet) do
    %{
      instruction: config.task_prompt,
      output_schema: config.output_schema,
      bounded_soup_packet: json_safe(packet)
    }
    |> Jason.encode!()
    |> byte_size()
  end

  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_safe(%Decimal{} = value), do: Decimal.to_string(value)
  defp json_safe(%MapSet{} = set), do: set |> MapSet.to_list() |> Enum.map(&json_safe/1)

  defp json_safe(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> Map.drop([:__meta__])
    |> json_safe()
  end

  defp json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(value), do: value

  defp source_packet_state(input, excerpt_chars) do
    excerpt =
      case excerpt_chars do
        count when is_integer(count) and count > 0 ->
          String.slice(input.body_text || "", 0, count)

        _ ->
          nil
      end

    %{
      source_ref: Admission.input_ref(input),
      article_ref: input.external_id,
      article_title: input.title,
      canonical_uri: get_in(input.normalized || %{}, ["canonical_uri"]),
      source_name: get_in(input.normalized || %{}, ["source_name"]),
      source_actor: get_in(input.normalized || %{}, ["source_actor"]),
      observed_at: input.observed_at && DateTime.to_iso8601(input.observed_at),
      excerpt: excerpt,
      excerpt_state:
        if(excerpt,
          do: "bounded",
          else: "unavailable:story_synthesis_packet_context_bound"
        )
    }
  end

  defp card_status(output, state, story) do
    status = to_string(output["status"] || "incomplete")

    cond do
      status in ["refused", "unavailable"] ->
        status

      missing_agent_key_claims?(output) or missing_topic_salience?(output) or
          missing_source_contribution_reasons?(output, state, story) ->
        "incomplete"

      status == "complete" ->
        "complete"

      true ->
        "incomplete"
    end
  end

  defp missing_agent_key_claims?(output), do: output["key_claims"] in [nil, []]

  defp missing_source_contribution_reasons?(output, state, story) do
    output_coverage = output["source_coverage"] || []

    state
    |> story_inputs_for_card(story.id)
    |> Enum.map(&Admission.input_ref/1)
    |> Enum.uniq()
    |> Enum.any?(fn source_ref ->
      case Enum.find(output_coverage, &(Map.get(&1, "source_ref") == source_ref)) do
        nil -> true
        row -> not usable_contribution_reason?(Map.get(row, "contribution_reason"))
      end
    end)
  end

  defp missing_source_contribution_refs(output, packet) do
    output_coverage = output["source_coverage"] || []

    packet
    |> linked_source_refs()
    |> Enum.reject(fn source_ref ->
      case Enum.find(output_coverage, &(Map.get(&1, "source_ref") == source_ref)) do
        nil -> false
        row -> usable_contribution_reason?(Map.get(row, "contribution_reason"))
      end
    end)
  end

  defp linked_source_refs(packet) do
    packet
    |> get_in([:committed_story_state, :linked_sources])
    |> case do
      sources when is_list(sources) ->
        sources
        |> Enum.map(fn source ->
          Map.get(source, :source_ref) || Map.get(source, "source_ref")
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      _ ->
        []
    end
  end

  defp usable_contribution_reason?(value), do: complete_text_field?(value)

  defp complete_text_field?(%{"state" => "complete", "text" => text})
       when is_binary(text) and text != "",
       do: true

  defp complete_text_field?(text) when is_binary(text) and text != "", do: true
  defp complete_text_field?(_), do: false

  defp missing_topic_salience?(output) do
    salience = output["topic_salience"] || %{}

    not usable_salience_hint?(salience["salience_explanation"]) or
      blank?(salience["global_salience"]) or blank?(salience["flynn_priority"])
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
          if(missing_topic_salience?(output), do: "unavailable", else: "complete")
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

  defp usable_salience_hint?(%{"state" => state, "reason" => reason})
       when state in ["unavailable", "refused"] and is_binary(reason) and reason != "",
       do: true

  defp usable_salience_hint?(%{"state" => "complete", "text" => text})
       when is_binary(text) and text != "",
       do: true

  defp usable_salience_hint?(text) when is_binary(text) and text != "", do: true
  defp usable_salience_hint?(_), do: false

  defp blank?(value), do: not (is_binary(value) and value != "")

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
    validation_failed_refs = source_coverage_validation_failed_refs(output)
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
          contribution_reason_for_source(
            agent_row,
            source_ref,
            validation_failed_refs,
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
      [] when linked_inputs == [] ->
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

  defp source_coverage_validation_failed_refs(output) do
    case output["source_coverage_validation"] do
      %{
        "state" => "failed",
        "reason" => "story_synthesis_agent_omitted_required_source_coverage_after_retry",
        "missing_source_refs" => refs
      }
      when is_list(refs) ->
        refs

      _ ->
        []
    end
  end

  defp contribution_reason_for_source(
         agent_row,
         source_ref,
         validation_failed_refs,
         provenance_refs
       ) do
    if source_ref in validation_failed_refs do
      %{
        "text" => nil,
        "state" => "refused",
        "reason" => "story_synthesis_agent_omitted_required_source_coverage_after_retry",
        "provenance_refs" => provenance_refs
      }
    else
      field_value(Map.get(agent_row, "contribution_reason"), nil, provenance_refs)
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
        appears_in_current_card:
          Map.get(
            claim,
            "appears_in_current_synopsis",
            Map.get(claim, "appears_in_current_card", true)
          )
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
    :story_synthesis
    |> config(
      "story-synthesis.v3.t1501.grounded-synopsis-evidence-contract",
      @synthesis_prompt,
      %{
        status: "complete | refused",
        title: %{text: "string", state: "complete", provenance_refs: ["string"]},
        exact_happening: %{
          text: "specific happening this synopsis artifact is about",
          state: "complete",
          provenance_refs: ["string"]
        },
        deck: %{
          text: "string",
          state: "complete",
          provenance_refs: ["string"]
        },
        summary: %{
          text: "string",
          state: "complete",
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
            appears_in_current_synopsis: true
          }
        ],
        source_coverage: [
          %{
            source_ref:
              "must exactly match one committed_story_state.linked_sources[].source_ref; include one row for every linked source_ref",
            contribution_reason: %{
              text:
                "concise reader-facing reason this article matters to the story; required for every committed_story_state.linked_sources source_ref, for example Adds the funding amount and names the investor",
              state: "complete",
              provenance_refs: ["string"]
            },
            materiality: "material | nonmaterial",
            source_posture: %{
              state: "complete",
              value: "string"
            },
            source_weight: %{
              state: "complete",
              value: "number"
            }
          }
        ],
        source_links: [
          %{
            source_ref:
              "must exactly match one committed_story_state.linked_sources[].source_ref; include one row for every linked source_ref",
            evidence_refs: ["evidence ref"]
          }
        ],
        topic_salience: %{
          salience_explanation: %{
            text: "story-to-topic salience explanation",
            state: "complete",
            provenance_refs: ["string"]
          },
          global_salience: "hint",
          flynn_priority: "hint"
        },
        changed_field_keys: ["field_key"],
        change_summary: %{
          text: "story-agent-authored change summary",
          state: "complete",
          provenance_refs: ["string"]
        },
        field_completeness: %{},
        refusal_provenance: %{
          reason: "required when status is refused",
          evidence_refs: ["evidence ref"],
          quarantine_recommendation: "string"
        }
      }
    )
    |> Map.put(:max_tokens, 8192)
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

  defp normalize_output(output) do
    output
    |> Kernel.||(%{})
    |> Admission.normalize_keys()
    |> normalize_source_coverage_output()
  end

  defp normalize_source_coverage_output(%{"source_coverage" => coverage} = output)
       when is_map(coverage) do
    rows =
      Enum.map(coverage, fn
        {source_ref, %{} = row} -> Map.put_new(row, "source_ref", source_ref)
        {_source_ref, row} -> row
      end)

    %{output | "source_coverage" => rows}
  end

  defp normalize_source_coverage_output(output), do: output

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

  defp story_title(output, input) do
    scalar_title(output["title"]) || scalar_title(input.title) || input.external_id
  end

  defp scalar_title(value) when is_binary(value) and value != "", do: value
  defp scalar_title([value]) when is_binary(value) and value != "", do: value
  defp scalar_title(_value), do: nil

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
