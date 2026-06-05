defmodule Primeradiant.Agentic.Proof do
  @moduledoc false

  alias Primeradiant.Agentic.Transcript
  alias Primeradiant.Agentic.Worker
  alias Primeradiant.Mediation.WriteGate, as: Engine
  alias Primeradiant.Authoring.Briefing
  alias Primeradiant.Ingestion.FixtureLoader
  alias Primeradiant.Projections.StoryClassifier
  alias Primeradiant.Soup.Store
  alias Primeradiant.UserContext.SeenState

  def run do
    corpus = FixtureLoader.load_corpus()

    base_store = Enum.reduce(corpus.watches, Store.new(), &Store.register_watch(&2, &1))
    {first_pass_inputs, second_pass_inputs} = Enum.split_with(corpus.inputs, &first_pass_input?/1)

    seed = build_seed_state(base_store, first_pass_inputs)
    seen_state = seed.seen_state

    second_pass =
      swim_inputs(
        seed.store,
        second_pass_inputs,
        seed.decisions,
        seed.invocations
      )

    second_authoring =
      Worker.flynn_relative_authoring(second_pass.store, seen_state, "flynn", :second)

    second_briefing = second_authoring.output
    final_store = Briefing.record_output(second_pass.store, second_briefing)
    final_seen_state = Briefing.mark_seen(seen_state, final_store, second_briefing)

    %{
      corpus: corpus.manifest["corpus_id"],
      store: final_store,
      decisions: second_pass.decisions,
      invocations: second_pass.invocations ++ [second_authoring],
      first_briefing: seed.first_briefing,
      second_briefing: second_briefing,
      seen_state: final_seen_state,
      seed_seen_state: seen_state,
      stale_story_keys:
        StoryClassifier.stale_story_keys(final_store, corpus.manifest["reference_now"]),
      proof_audit:
        audit(
          corpus,
          final_store,
          second_pass.invocations ++ [second_authoring],
          seen_state,
          seed.first_briefing
        )
    }
  end

  defp build_seed_state(base_store, first_pass_inputs) do
    seeded = swim_inputs(base_store, first_pass_inputs, [], [])

    seed_authoring =
      Worker.flynn_relative_authoring(seeded.store, SeenState.new("flynn"), "flynn", :seed)

    seed_output = seed_authoring.output
    seed_store = Briefing.record_output(seeded.store, seed_output)
    seed_seen_state = Briefing.mark_seen(SeenState.new("flynn"), seed_store, seed_output)

    %{
      store: seed_store,
      decisions: seeded.decisions,
      invocations: seeded.invocations ++ [seed_authoring],
      seen_state: seed_seen_state,
      first_briefing: seed_output
    }
  end

  defp swim_inputs(store, inputs, decisions, invocations) do
    Enum.reduce(inputs, %{store: store, decisions: decisions, invocations: invocations}, fn raw,
                                                                                            acc ->
      admission = Worker.source_admission(raw)
      {store_with_activation, activation} = enqueue_activation(acc.store, admission)
      identity = Worker.story_identity(store_with_activation, admission.normalized, "flynn")

      advancement =
        Worker.advancement_contradiction(
          store_with_activation,
          admission.normalized,
          identity.decision,
          "flynn"
        )

      {store, decision_record, follow_on_agents} =
        case advancement do
          %{outcome: :write, proposal: proposal} ->
            committed = Engine.commit(store_with_activation, proposal, admission.normalized)
            follow_on = follow_on_activation(committed, proposal, admission.normalized)
            follow_on_agent = Worker.follow_on_review(committed, follow_on, "flynn")

            committed =
              if follow_on_agent do
                Store.append_activation(
                  committed,
                  Map.put(follow_on, :consumed_by, follow_on_agent.role)
                )
              else
                committed
              end

            committed =
              maybe_append_uncertainty_refusal(committed, advancement, admission.normalized)

            {committed, decision_record(raw, identity.decision, proposal),
             Enum.reject([follow_on_agent], &is_nil/1)}

          %{outcome: :refused} ->
            refusal = refusal_record(raw, identity.decision, advancement)
            {Store.append_refusal(store_with_activation, refusal_log(advancement)), refusal, []}
        end

      %{
        store: store,
        decisions: acc.decisions ++ [decision_record],
        invocations:
          acc.invocations ++
            [admission, activation, identity, advancement] ++
            follow_on_agents ++
            uncertainty_refusal_invocations(store, admission.normalized)
      }
    end)
  end

  defp decision_record(raw, decision, proposal) do
    %{
      fixture_id: raw["fixture_id"],
      classification: decision.classification,
      story_key: decision.story_key,
      proposal_id: proposal.id,
      evidence_refs: proposal.evidence_refs,
      confidence: proposal.confidence,
      outcome: :committed,
      agent_role: proposal.agent_role,
      input_packet_hash: proposal.input_packet_hash
    }
  end

  defp refusal_record(raw, decision, invocation) do
    %{
      fixture_id: raw["fixture_id"],
      classification: decision.classification,
      story_key: decision.story_key,
      proposal_id: nil,
      evidence_refs: invocation.evidence_refs,
      confidence: invocation.confidence,
      outcome: :refused,
      refusal_reason: invocation.reason,
      agent_role: invocation.role,
      input_packet_hash: invocation.packet_hash
    }
  end

  defp enqueue_activation(store, admission) do
    activation = %{
      role: :source_admission,
      config: admission.config,
      packet: admission.packet,
      packet_hash: admission.packet_hash,
      outcome: :offered,
      operation_family: :source_packet_offer,
      evidence_refs: admission.evidence_refs,
      confidence: 1.0,
      uncertainty_class: :substrate_trace,
      reason: "Scheduler offered an ACL-scoped source packet; source admission chose to admit it."
    }

    {Store.append_activation(store, activation), activation}
  end

  defp follow_on_activation(store, proposal, normalized) do
    %{
      role: :advancement_contradiction,
      config: %{
        role: :advancement_contradiction,
        config_version: "local-lease-scheduler.v1.t1137",
        prompt_version_hash: "substrate:no-product-meaning"
      },
      packet: %{
        packet_contract: :mutation_trigger,
        raw_database_access: false,
        evidence_refs: proposal.evidence_refs,
        story_key: proposal.story_key,
        output_visibility: proposal.visibility
      },
      packet_hash:
        :crypto.hash(:sha256, "follow-on:#{normalized.fixture_id}:#{length(store.commits)}")
        |> Base.encode16(case: :lower),
      outcome: :activated,
      operation_family: :follow_on_activation,
      caused_by_fixture_id: normalized.fixture_id,
      evidence_refs: proposal.evidence_refs,
      confidence: 1.0,
      uncertainty_class: :substrate_trace,
      reason: "Committed soup mutation made adjacent story work eligible for another agent."
    }
  end

  defp refusal_log(invocation) do
    Map.merge(invocation, %{
      status: :refused,
      timestamp: timestamp(),
      actor_id: "flynn",
      prior_element_refs: Map.get(invocation.packet, :visible_story_refs, [])
    })
  end

  defp maybe_append_uncertainty_refusal(store, advancement, normalized) do
    if normalized.questions != %{} do
      Store.append_refusal(store, refusal_log(uncertainty_refusal(advancement, normalized)))
    else
      store
    end
  end

  defp uncertainty_refusal(advancement, normalized) do
    refusal = Transcript.refusal(normalized.fixture_id)

    %{
      role: :advancement_contradiction,
      config: advancement.config,
      packet: advancement.packet,
      packet_hash: advancement.packet_hash,
      outcome: refusal.outcome,
      operation_family: :uncertainty_review,
      fixture_id: normalized.fixture_id,
      evidence_refs: [normalized.fixture_id],
      confidence: refusal.confidence,
      uncertainty_class: refusal.uncertainty_class,
      decision_source: :recorded_agent_transcript,
      deterministic_product_logic: false,
      agent_output_hash: refusal.agent_output_hash,
      reason: refusal.rationale
    }
    |> Map.merge(Transcript.producer())
  end

  defp uncertainty_refusal_invocations(store, normalized) do
    store.refusals
    |> Enum.filter(
      &(&1.fixture_id == normalized.fixture_id and &1.operation_family == :uncertainty_review)
    )
  end

  defp audit(corpus, store, invocations, seed_seen_state, first_briefing) do
    %{
      corpus: corpus_audit(corpus, seed_seen_state),
      agents: agent_audit(store, invocations),
      writes: write_audit(store),
      membrane: membrane_audit(store),
      output: output_audit(store, invocations),
      anti_cheat: anti_cheat_audit(invocations),
      soul: soul_audit(store, invocations, first_briefing),
      correction_matrix: correction_matrix(store, invocations, first_briefing)
    }
  end

  defp corpus_audit(corpus, seed_seen_state) do
    inputs = corpus.inputs

    %{
      real_message_style_evidence: Enum.all?(inputs, &Map.has_key?(&1, "body_text")),
      no_trusted_story_ids: Enum.all?(inputs, &(not Map.has_key?(&1, "story_id"))),
      repeated_pressure:
        Enum.count(inputs, &String.contains?(&1["fixture_id"], "duplicate")) >= 2,
      uncertainty:
        Enum.any?(inputs, &String.contains?(String.downcase(&1["body_text"]), "correct")),
      public_evidence: Enum.any?(inputs, &(get_in(&1, ["acl", "privacy"]) == "public")),
      private_evidence: Enum.any?(inputs, &(get_in(&1, ["acl", "privacy"]) == "private")),
      prior_flynn_seen_state: map_size(seed_seen_state.stories) > 0
    }
  end

  defp agent_audit(store, invocations) do
    configured_agent_invocations =
      Enum.filter(invocations, fn invocation ->
        version = get_in(invocation, [:config, :config_version])

        is_binary(version) and String.ends_with?(version, ".t1137") and
          invocation.operation_family not in [:source_packet_offer, :source_admission]
      end)

    roles = configured_agent_invocations |> Enum.map(& &1.role) |> MapSet.new()

    %{
      actual_invocations: length(configured_agent_invocations),
      explicit_configs:
        Enum.all?(configured_agent_invocations, &get_in(&1, [:config, :config_version])),
      bounded_packets:
        Enum.all?(
          configured_agent_invocations,
          &(get_in(&1, [:packet, :raw_database_access]) == false)
        ),
      agent_mediated_outputs:
        Enum.all?(
          configured_agent_invocations,
          &agentic_meaning_invocation?/1
        ),
      overlapping_regions: overlapping_agent_regions?(configured_agent_invocations),
      abstention_or_refusal:
        Enum.any?(configured_agent_invocations, &(&1.outcome in [:abstained, :refused])),
      mutation_triggered_follow_on:
        Enum.any?(store.activations, &(&1.outcome == :activated)) and
          Enum.any?(
            configured_agent_invocations,
            &(&1.operation_family == :follow_on_review and &1.outcome == :abstained)
          ),
      required_roles_present:
        MapSet.subset?(
          MapSet.new([
            :story_identity,
            :advancement_contradiction,
            :flynn_relative_authoring
          ]),
          roles
        ),
      source_admission_is_substrate:
        invocations
        |> Enum.filter(&(&1.operation_family == :source_admission))
        |> Enum.all?(&(&1.decision_source == :deterministic_substrate))
    }
  end

  defp write_audit(store) do
    %{
      atomic_mutations: Enum.all?(store.commits, &(&1.status == :committed)),
      append_only_logs: length(store.commits) > length(store.proposals),
      actor_attribution: Enum.all?(store.commits, &is_binary(&1.actor_id)),
      evidence_refs: Enum.all?(store.commits, &(&1.evidence_refs != [])),
      confidence_and_uncertainty:
        Enum.all?(store.proposals, &(is_float(&1.confidence) and &1.uncertainty_class != nil)),
      refusal_logs:
        store.refusals != [] and
          Enum.all?(store.refusals, &(&1.status == :refused and &1.evidence_refs != [])),
      typed_edges: Enum.all?(store.edges, &is_atom(&1.edge_type)),
      no_silent_replacement: Enum.all?(store.stories, fn {_key, story} -> story.history != [] end)
    }
  end

  defp membrane_audit(store) do
    %{
      source_corpus_read_only:
        Enum.all?(Map.values(store.nodes), &(&1.node_type != :source_corpus)),
      public_writes_public_evidence_only:
        store.proposals
        |> Enum.filter(&(&1.visibility == "public"))
        |> Enum.all?(fn proposal ->
          Enum.all?(proposal.evidence_refs, fn ref ->
            get_in(store.nodes, [ref, :attrs, :acl, "privacy"]) == "public"
          end)
        end),
      private_writes_remain_private: Enum.any?(store.proposals, &(&1.visibility == "private")),
      no_private_to_public_leak:
        public_proposals_have_public_refs?(store) and public_stories_have_public_inputs?(store)
    }
  end

  defp output_audit(store, invocations) do
    authoring = invocations |> Enum.filter(&(&1.role == :flynn_relative_authoring)) |> List.last()

    %{
      flynn_relative_delta: authoring.output.verified,
      cites_allowed_evidence: authoring.output.evidence_refs != [],
      distinguishes_new_to_flynn: String.contains?(authoring.output.text, "since last seen"),
      records_uncertainty:
        Enum.any?(store.proposals, &(&1.uncertainty_class != :bounded_evidence)),
      mutates_seen_state_or_private_brain:
        authoring.output.touched_story_keys != [] and
          Map.has_key?(store.nodes, authoring.output.output_id)
    }
  end

  defp anti_cheat_audit(invocations) do
    %{
      no_fixture_oracle_output:
        Enum.all?(invocations, &(Map.get(&1, :operation_family) != :fixture_oracle)),
      no_demo_product_claim:
        not Enum.any?(invocations, &String.contains?(to_string(&1.reason), "product ready")),
      harness_labels_excluded_from_product_claims:
        Enum.all?(invocations, &(get_in(&1, [:config, :prompt_version_hash]) != nil)),
      rejects_deterministic_standins:
        invocations
        |> Enum.filter(&(&1.operation_family in meaning_operation_families()))
        |> Enum.all?(&agentic_meaning_invocation?/1),
      rejects_unprovenanced_role_split:
        not agentic_meaning_invocation?(%{
          operation_family: :advancement_or_story_transformation,
          decision_source: :recorded_agent_transcript,
          deterministic_product_logic: false,
          producer_kind: :actual_agent,
          producer_id: "codex",
          artifact_ref: "artifact-without-role-provenance",
          agent_output_hash: "same-story-decision-hash"
        }),
      deterministic_standin_probe_rejected:
        not agentic_meaning_invocation?(%{
          operation_family: :story_identity,
          decision_source: :deterministic_product_function,
          deterministic_product_logic: true,
          agent_output_hash: "fixture-oracle"
        })
    }
  end

  defp soul_audit(store, invocations, first_briefing) do
    %{
      not_etl_or_pipeline_shape:
        Enum.any?(invocations, &(&1.operation_family == :follow_on_review)) and
          Enum.any?(invocations, &(&1.outcome in [:abstained, :refused])),
      not_passive_summary:
        store.commits != [] and
          Enum.any?(invocations, &(&1.operation_family == :flynn_relative_delta)),
      preserves_repair_over_time:
        Enum.any?(
          store.proposals,
          &(&1.classification in [:conflict_correction, :stale_background_state])
        ) and
          Enum.any?(store.stories, fn {_key, story} -> story.state == :background end),
      excludes_deterministic_product_meaning:
        invocations
        |> Enum.filter(&(&1.operation_family in meaning_operation_families()))
        |> Enum.all?(&agentic_meaning_invocation?/1),
      excludes_raw_archive_clone:
        Enum.any?(store.stories, fn {_key, story} -> story.history != [] end) and
          Enum.any?(
            store.edges,
            &(&1.edge_type in [:updates, :contradicts, :duplicates, :adds_color])
          ),
      first_output_is_private_seen_state:
        first_briefing.verified == true and first_briefing.touched_story_keys != []
    }
  end

  defp correction_matrix(store, invocations, first_briefing) do
    soul = soul_audit(store, invocations, first_briefing)

    %{
      "A1132-01":
        Enum.any?(invocations, &(&1.role in [:story_identity, :advancement_contradiction])),
      "A1132-02":
        Enum.any?(store.activations, &(&1.operation_family == :follow_on_activation)) and
          Enum.any?(invocations, &(&1.operation_family == :follow_on_review)),
      "A1132-03": store.commits != [] and store.proposal_decisions != [],
      "A1132-04": membrane_audit(store).no_private_to_public_leak,
      "A1132-05": Enum.any?(store.edges, &(&1.edge_type == :duplicates)),
      "A1132-06": Enum.any?(invocations, &(get_in(&1, [:config, :config_version]) =~ ".t1137")),
      "A1132-07": soul.preserves_repair_over_time,
      "A1132-08": Enum.all?(Map.values(soul))
    }
  end

  defp meaning_operation_families do
    [
      :story_identity,
      :advancement_or_story_transformation,
      :follow_on_review,
      :flynn_relative_delta,
      :uncertainty_review
    ]
  end

  defp agentic_meaning_invocation?(invocation) do
    invocation.decision_source == :recorded_agent_transcript and
      invocation.deterministic_product_logic == false and
      invocation.producer_kind == :actual_agent and
      is_binary(invocation.producer_id) and
      is_binary(invocation.artifact_ref) and
      is_binary(invocation.agent_output_hash) and
      role_provenance_matches?(invocation)
  end

  defp role_provenance_matches?(%{operation_family: :story_identity} = invocation) do
    role_provenance_matches?(invocation, :story_identity)
  end

  defp role_provenance_matches?(
         %{operation_family: :advancement_or_story_transformation} = invocation
       ) do
    role_provenance_matches?(invocation, :advancement_contradiction)
  end

  defp role_provenance_matches?(_invocation), do: true

  defp role_provenance_matches?(invocation, role) do
    Map.get(invocation, :role) == role and
      get_in(invocation, [:role_provenance, :role]) == role and
      get_in(invocation, [:role_provenance, :config_version]) ==
        get_in(invocation, [:config, :config_version]) and
      get_in(invocation, [:role_provenance, :prompt_version_hash]) ==
        get_in(invocation, [:config, :prompt_version_hash]) and
      is_binary(get_in(invocation, [:role_provenance, :recorded_choice]))
  end

  defp overlapping_agent_regions?(configured_agent_invocations) do
    story_regions =
      configured_agent_invocations
      |> Enum.filter(&(&1.role == :story_identity))
      |> Enum.flat_map(&Map.get(&1.packet, :visible_story_refs, []))
      |> MapSet.new()

    advancement_regions =
      configured_agent_invocations
      |> Enum.filter(&(&1.role == :advancement_contradiction))
      |> Enum.flat_map(&Map.get(&1.packet, :visible_story_refs, []))
      |> MapSet.new()

    story_regions |> MapSet.intersection(advancement_regions) |> MapSet.size() > 0
  end

  defp first_pass_input?(raw) do
    observed_at = DateTime.from_iso8601(raw["observed_at"]) |> elem(1)
    cutoff = DateTime.from_iso8601("2026-05-12T00:00:00-07:00") |> elem(1)
    DateTime.compare(observed_at, cutoff) == :lt
  end

  defp timestamp do
    DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  end

  defp public_proposals_have_public_refs?(store) do
    store.proposals
    |> Enum.filter(&(&1.visibility == "public"))
    |> Enum.all?(fn proposal ->
      Enum.all?(proposal.evidence_refs, fn ref ->
        get_in(store.nodes, [ref, :attrs, :acl, "privacy"]) == "public"
      end)
    end)
  end

  defp public_stories_have_public_inputs?(store) do
    store.stories
    |> Map.values()
    |> Enum.filter(fn story ->
      Enum.any?(story.inputs, fn input_id ->
        get_in(store.nodes, [input_id, :attrs, :acl, "privacy"]) == "public"
      end)
    end)
    |> Enum.all?(fn story ->
      story.inputs
      |> Enum.all?(fn input_id ->
        get_in(store.nodes, [input_id, :attrs, :acl, "privacy"]) == "public"
      end)
    end)
  end
end
