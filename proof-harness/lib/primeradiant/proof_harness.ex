defmodule Primeradiant.ProofHarness do
  @moduledoc false

  alias Primeradiant.Mediation.WriteGate, as: Engine
  alias Primeradiant.Authoring.Briefing
  alias Primeradiant.Ingestion.FixtureLoader
  alias Primeradiant.Ingestion.Normalizer
  alias Primeradiant.Projections.StoryClassifier
  alias Primeradiant.Proposals.Builder
  alias Primeradiant.Soup.Store
  alias Primeradiant.UserContext.SeenState

  def run do
    corpus = FixtureLoader.load_corpus()

    base_store = Enum.reduce(corpus.watches, Store.new(), &Store.register_watch(&2, &1))
    {first_pass_inputs, second_pass_inputs} = Enum.split_with(corpus.inputs, &first_pass_input?/1)

    %{store: store, decisions: decisions} =
      ingest_inputs(base_store, first_pass_inputs, [])

    seen_state = SeenState.new("flynn")
    first_briefing = Briefing.render(store, seen_state, "flynn")
    store = Briefing.record_output(store, first_briefing)
    seen_state = Briefing.mark_seen(seen_state, store, first_briefing)

    %{store: store, decisions: decisions} = ingest_inputs(store, second_pass_inputs, decisions)

    %{
      corpus: corpus.manifest["corpus_id"],
      decisions: decisions,
      store: store,
      stale_story_keys: StoryClassifier.stale_story_keys(store, corpus.manifest["reference_now"]),
      first_briefing: first_briefing,
      second_briefing: Briefing.render(store, seen_state, "flynn")
    }
  end

  defp ingest_inputs(store, inputs, decisions) do
    Enum.reduce(inputs, %{store: store, decisions: decisions}, fn raw, acc ->
      normalized = Normalizer.normalize(raw)
      decision = StoryClassifier.decide(acc.store, normalized)
      proposal = Builder.build(normalized, decision)
      store = Engine.commit(acc.store, proposal, normalized)

      decision_record = %{
        fixture_id: raw["fixture_id"],
        classification: decision.classification,
        story_key: decision.story_key,
        proposal_id: proposal.id,
        evidence_refs: proposal.evidence_refs,
        confidence: proposal.confidence
      }

      %{store: store, decisions: acc.decisions ++ [decision_record]}
    end)
  end

  defp first_pass_input?(raw) do
    observed_at = DateTime.from_iso8601(raw["observed_at"]) |> elem(1)
    cutoff = DateTime.from_iso8601("2026-05-12T00:00:00-07:00") |> elem(1)
    DateTime.compare(observed_at, cutoff) == :lt
  end
end
