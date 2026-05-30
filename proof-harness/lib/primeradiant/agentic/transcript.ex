defmodule Primeradiant.Agentic.Transcript do
  @moduledoc false

  @artifact_relpath "agent_runs/t1137_agentic_meaning_run.json"

  @classification_map %{
    "new public story seed" => :new_story,
    "private user task seed" => :new_story,
    "private low-rank watch match" => :new_story,
    "adjacent distinct public story" => :new_story,
    "material public update" => :substantive_update,
    "private task progress update" => :substantive_update,
    "private blocker annotation" => :substantive_update,
    "second-pass private material update" => :substantive_update,
    "repeated pressure reinforcement" => :repeated_noop_input,
    "near-duplicate correction reinforcement" => :repeated_noop_input,
    "correction with conflicts" => :conflict_correction,
    "interpretive framing without material fact update" => :color_spin_without_structural_change,
    "second-pass stale related no-op" => :stale_background_state
  }

  def producer do
    artifact = artifact!()

    %{
      producer_kind: :actual_agent,
      producer_id: artifact["producer_id"],
      artifact_ref: artifact_path(),
      agent_run_id: artifact["agent_run_id"],
      model_family: artifact["model_family"],
      deterministic_product_logic: false,
      agent_run_controls: artifact["deterministic_product_logic"]
    }
  end

  def story_decision(fixture_id, role)
      when role in [:story_identity, :advancement_contradiction] do
    artifact = artifact!()
    role_provenance = role_provenance!(artifact, role)

    artifact["story_decisions"]
    |> Enum.find(&(&1["input_fixture_id"] == fixture_id))
    |> require_decision!(fixture_id)
    |> normalize_story_decision(fixture_id, role_provenance)
    |> Map.merge(producer())
    |> Map.put(
      :agent_output_hash,
      output_hash(["story_decisions", fixture_id, Atom.to_string(role)])
    )
  end

  def authoring_output(stage, store, user_id) when stage in [:seed, :second] do
    artifact = artifact!()

    artifact
    |> get_in(["authoring_outputs", Atom.to_string(stage)])
    |> require_decision!("authoring:#{stage}")
    |> build_authoring_output(store, user_id, stage)
  end

  def follow_on_review(caused_by_fixture_id) do
    case Enum.find(
           artifact!()["follow_on_reviews"],
           &(&1["caused_by_fixture_id"] == caused_by_fixture_id)
         ) do
      nil ->
        nil

      record ->
        record
        |> normalize_agent_outcome()
        |> Map.merge(producer())
        |> Map.put(:agent_output_hash, output_hash(["follow_on_reviews", caused_by_fixture_id]))
    end
  end

  def refusal(fixture_id) do
    artifact!()["refusals"]
    |> Enum.find(&(&1["fixture_id"] == fixture_id))
    |> require_decision!("refusal:#{fixture_id}")
    |> normalize_agent_outcome()
    |> Map.merge(producer())
    |> Map.put(:agent_output_hash, output_hash(["refusals", fixture_id]))
  end

  def artifact_path do
    :primeradiant_proof_harness
    |> :code.priv_dir()
    |> to_string()
    |> Path.join(@artifact_relpath)
  end

  def artifact_hash do
    :sha256
    |> :crypto.hash(File.read!(artifact_path()))
    |> Base.encode16(case: :lower)
  end

  defp artifact! do
    artifact_path()
    |> File.read!()
    |> Jason.decode!()
    |> validate_artifact!()
  end

  defp validate_artifact!(artifact) do
    controls = artifact["deterministic_product_logic"] || %{}

    true = artifact["producer_kind"] == "recorded_agent_run_artifact"
    true = artifact["ticket_id"] == "T1137"

    true =
      "proof-harness/priv/fixtures/primeradiant_golden/expected/*" in controls["excluded_paths"]

    true = "central deterministic arbitration" in controls["non_claims"]
    true = String.contains?(controls["claim"], "does not choose story meaning")
    true = is_map(artifact["story_decision_role_provenance"])

    true =
      String.contains?(
        artifact["story_decision_role_provenance"]["record_shape"],
        "combined-role"
      )

    Enum.each([:story_identity, :advancement_contradiction], fn role ->
      role_provenance!(artifact, role)
    end)

    artifact
  end

  defp require_decision!(nil, id), do: raise("missing T1137 recorded agent decision: #{id}")
  defp require_decision!(decision, _id), do: decision

  defp normalize_story_decision(record, fixture_id, role_provenance) do
    %{
      role: normalize_atom(role_provenance["role"]),
      config_version: role_provenance["config_version"],
      prompt_version_hash: role_provenance["prompt_version_hash"],
      recorded_choice: role_provenance["recorded_choice"],
      classification: Map.fetch!(@classification_map, record["classification"]),
      agent_classification: record["classification"],
      story_key: record["story_key"],
      confidence: record["confidence"],
      changed_facts: normalize_changed_facts(record["changed_facts"]),
      watch_ids: record["watch_ids"] || [],
      rationale: record["rationale"],
      parent_story_key: record["parent_story_key"],
      conflicts: normalize_conflicts(record["conflicts"] || []),
      fixture_id: fixture_id
    }
  end

  defp normalize_agent_outcome(record) do
    %{
      role: normalize_atom(record["role"]),
      config_version: record["config_version"],
      prompt_version_hash: record["prompt_version_hash"],
      outcome: normalize_atom(record["outcome"]),
      confidence: record["confidence"],
      uncertainty_class: normalize_atom(record["uncertainty_class"]),
      rationale: record["rationale"],
      caused_by_fixture_id: record["caused_by_fixture_id"],
      fixture_id: record["fixture_id"]
    }
  end

  defp build_authoring_output(record, store, user_id, stage) do
    output_id = "authored-output:" <> Integer.to_string(length(store.commits))
    bullets = record["bullets"]
    text = "what changed for Flynn since last seen, and why\n" <> Enum.join(bullets, "\n")
    raw_evidence_refs = record["evidence_refs"]
    evidence_refs = Enum.filter(raw_evidence_refs, &Map.has_key?(store.nodes, &1))
    context_refs = raw_evidence_refs -- evidence_refs
    touched_story_keys = record["touched_story_keys"]

    %{
      output_id: output_id,
      user_id: user_id,
      bullets: bullets,
      text: text,
      evidence_packet: %{
        user_id: user_id,
        evidence_refs: evidence_refs,
        context_refs: context_refs
      },
      sentence_evidence:
        Enum.map(bullets, fn bullet ->
          %{
            text: bullet,
            evidence_refs: evidence_refs,
            claim_refs: claim_refs_for(touched_story_keys, store)
          }
        end),
      evidence_refs: evidence_refs,
      touched_story_keys: touched_story_keys,
      verified: evidence_refs != [] and Enum.all?(evidence_refs, &Map.has_key?(store.nodes, &1)),
      producer: producer(),
      stage: stage,
      agent_output_hash: output_hash(["authoring_outputs", Atom.to_string(stage)])
    }
  end

  defp normalize_changed_facts(facts) when is_map(facts), do: facts

  defp normalize_changed_facts(facts) when is_list(facts) do
    facts
    |> Enum.with_index(1)
    |> Map.new(fn {fact, index} -> {"agent_fact_#{index}", fact} end)
  end

  defp normalize_changed_facts(_facts), do: %{}

  defp normalize_conflicts(conflicts) do
    Enum.map(conflicts, fn
      conflict when is_map(conflict) ->
        Map.new(conflict, fn {key, value} -> {String.to_atom(to_string(key)), value} end)

      source_ref when is_binary(source_ref) ->
        %{source_ref: source_ref, relation: "conflicts_with_correction"}
    end)
  end

  defp claim_refs_for(story_keys, store) do
    story_keys
    |> Enum.flat_map(fn story_key ->
      story = store.stories[story_key]

      if story do
        story.structural_facts
        |> Map.keys()
        |> Enum.map(&"claim:#{story_key}:#{&1}")
      else
        []
      end
    end)
    |> Enum.filter(&Map.has_key?(store.nodes, &1))
    |> Enum.uniq()
  end

  defp output_hash(path) do
    payload = payload_at(path)

    :sha256
    |> :crypto.hash(:erlang.term_to_binary(payload))
    |> Base.encode16(case: :lower)
  end

  defp payload_at(["story_decisions", fixture_id]) do
    artifact!()["story_decisions"]
    |> Enum.find(&(&1["input_fixture_id"] == fixture_id))
  end

  defp payload_at(["story_decisions", fixture_id, role]) do
    artifact = artifact!()

    %{
      story_decision:
        artifact["story_decisions"]
        |> Enum.find(&(&1["input_fixture_id"] == fixture_id)),
      role_provenance: role_provenance!(artifact, String.to_atom(role))
    }
  end

  defp payload_at(["follow_on_reviews", fixture_id]) do
    artifact!()["follow_on_reviews"]
    |> Enum.find(&(&1["caused_by_fixture_id"] == fixture_id))
  end

  defp payload_at(["refusals", fixture_id]) do
    artifact!()["refusals"]
    |> Enum.find(&(&1["fixture_id"] == fixture_id))
  end

  defp payload_at(path), do: get_in(artifact!(), path)

  defp role_provenance!(artifact, role) do
    role_string = Atom.to_string(role)

    artifact["story_decision_role_provenance"]["roles"]
    |> Enum.find(&(&1["role"] == role_string))
    |> require_decision!("story-role-provenance:#{role_string}")
  end

  defp normalize_atom(value), do: value |> String.replace("-", "_") |> String.to_atom()
end
