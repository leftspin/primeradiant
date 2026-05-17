defmodule Primeradiant.UserContext.SeenState do
  @moduledoc false

  def new(user_id), do: %{user_id: user_id, stories: %{}}

  def mark_seen(seen_state, story, authored_output) do
    story_sentence_evidence =
      authored_output.sentence_evidence
      |> Enum.filter(fn sentence ->
        Enum.any?(sentence.evidence_refs, &(&1 in story.inputs))
      end)

    put_in(seen_state, [:stories, story.story_key], %{
      seen_story_version: story.version,
      output_id: authored_output.output_id,
      seen_input_refs:
        story_sentence_evidence |> Enum.flat_map(& &1.evidence_refs) |> Enum.uniq(),
      seen_claim_refs: story_sentence_evidence |> Enum.flat_map(& &1.claim_refs) |> Enum.uniq(),
      seen_at: story.updated_at
    })
  end

  def last_seen_version(seen_state, story_key) do
    get_in(seen_state, [:stories, story_key, :seen_story_version]) || 0
  end

  def seen_input_refs(seen_state, story_key) do
    get_in(seen_state, [:stories, story_key, :seen_input_refs]) || []
  end
end
