defmodule Primeradiant.Soup.Store do
  @moduledoc false

  def new do
    %{
      nodes: %{},
      stories: %{},
      edges: [],
      proposals: [],
      proposal_decisions: [],
      commits: [],
      refusals: [],
      non_commitment_pressure: [],
      activations: [],
      indexed_fingerprints: MapSet.new(),
      watch_index: %{}
    }
  end

  def story(store, story_key), do: Map.fetch!(store.stories, story_key)

  def put_story(store, story_key, story) do
    put_in(store, [:stories, story_key], story)
  end

  def register_watch(store, watch) do
    store
    |> update_in([:watch_index], &Map.put(&1, watch["watch_id"], watch))
    |> put_node(watch["watch_id"], %{
      node_type: :user_watch,
      title: watch["label"],
      state: :active,
      attrs: watch,
      evidence_refs: [watch["watch_id"]]
    })
  end

  def put_node(store, node_id, node) do
    put_in(store, [:nodes, node_id], Map.put(node, :id, node_id))
  end

  def append_refusal(store, refusal) do
    %{store | refusals: store.refusals ++ [refusal]}
  end

  def append_non_commitment_pressure(store, pressure) do
    %{store | non_commitment_pressure: store.non_commitment_pressure ++ [pressure]}
  end

  def append_activation(store, activation) do
    %{store | activations: store.activations ++ [activation]}
  end
end
