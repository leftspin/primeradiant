defmodule Primeradiant.StorageHarness.Schema do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key {:id, :binary_id, autogenerate: false}
      @foreign_key_type :binary_id

      defp put_id(changeset),
        do: put_change(changeset, :id, get_field(changeset, :id) || Ecto.UUID.generate())

      defp validate_confidence(changeset, field \\ :confidence) do
        changeset
        |> validate_number(field, greater_than_or_equal_to: Decimal.new("0"))
        |> validate_number(field, less_than_or_equal_to: Decimal.new("1"))
      end

      defp validate_non_empty_list(changeset, field) do
        value = get_field(changeset, field)

        if is_list(value) and value != [] do
          changeset
        else
          add_error(changeset, field, "must be a non-empty list")
        end
      end

      defp validate_required_map_keys(changeset, field, keys) do
        value = get_field(changeset, field) || %{}
        missing = Enum.reject(keys, &present_map_value?(value, &1))

        if missing == [] do
          changeset
        else
          add_error(changeset, field, "missing required keys: #{Enum.join(missing, ", ")}")
        end
      end

      defp present_map_value?(value, key) when is_map(value) do
        case Map.get(value, key) || Map.get(value, to_string(key)) do
          nil -> false
          "" -> false
          [] -> false
          %{} = map when map_size(map) == 0 -> false
          _ -> true
        end
      end

      defp present_map_value?(_value, _key), do: false
    end
  end
end

defmodule Primeradiant.StorageHarness.Input do
  use Primeradiant.StorageHarness.Schema

  schema "inputs" do
    field(:tenant_id, :binary_id)
    field(:fixture_id, :string)
    field(:source_type, :string)
    field(:external_id, :string)
    field(:observed_at, :utc_datetime_usec)
    field(:title, :string)
    field(:body_text, :string)
    field(:object_uri, :string)
    field(:content_sha256, :string)
    field(:acl, :map, default: %{})
    field(:normalized, :map, default: %{})
    field(:facts, :map, default: %{})
    field(:background, :map, default: %{})
    field(:questions, :map, default: %{})
    field(:colors, {:array, :string}, default: [])
    field(:topic_tokens, {:array, :string}, default: [])
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :fixture_id,
      :source_type,
      :external_id,
      :observed_at,
      :title,
      :body_text,
      :object_uri,
      :content_sha256,
      :acl,
      :normalized,
      :facts,
      :background,
      :questions,
      :colors,
      :topic_tokens
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :source_type,
      :external_id,
      :observed_at,
      :content_sha256,
      :acl,
      :normalized,
      :facts,
      :background,
      :questions,
      :colors,
      :topic_tokens
    ])
  end
end

defmodule Primeradiant.StorageHarness.AgentRun do
  use Primeradiant.StorageHarness.Schema

  schema "agent_runs" do
    field(:tenant_id, :binary_id)
    field(:agent_run_key, :string)
    field(:agent_type, :string)
    field(:prompt_version, :string)
    field(:model, :string)
    field(:scope, :map, default: %{})
    field(:status, :string, default: "succeeded")
    field(:trace_id, :string)
    field(:started_at, :utc_datetime_usec)
    field(:ended_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :agent_run_key,
      :agent_type,
      :prompt_version,
      :model,
      :scope,
      :status,
      :trace_id,
      :started_at,
      :ended_at
    ])
    |> put_id()
    |> validate_required([:tenant_id, :agent_run_key, :agent_type, :scope, :status])
    |> validate_inclusion(:status, ["succeeded", "failed", "running"])
  end
end

defmodule Primeradiant.StorageHarness.Watch do
  use Primeradiant.StorageHarness.Schema

  schema "watches" do
    field(:tenant_id, :binary_id)
    field(:user_id, :string)
    field(:watch_key, :string)
    field(:intent, :string)
    field(:priority, :integer, default: 0)
    field(:match_any, {:array, :string}, default: [])
    field(:filters, :map, default: %{})
    field(:status, :string, default: "active")
    field(:attrs, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :user_id,
      :watch_key,
      :intent,
      :priority,
      :match_any,
      :filters,
      :status,
      :attrs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :user_id,
      :watch_key,
      :intent,
      :priority,
      :match_any,
      :filters,
      :status,
      :attrs
    ])
    |> validate_inclusion(:status, ["active", "paused"])
  end
end

defmodule Primeradiant.StorageHarness.Story do
  use Primeradiant.StorageHarness.Schema

  @placeholder_story_keys ~w(new-story new_story newstory story news-story)

  schema "stories" do
    field(:tenant_id, :binary_id)
    field(:story_key, :string)
    field(:title, :string)
    field(:state, :string, default: "active")
    field(:version, :integer, default: 0)
    field(:first_observed_at, :utc_datetime_usec)
    field(:updated_at_story, :utc_datetime_usec)
    field(:last_material_at, :utc_datetime_usec)
    field(:structural_facts, :map, default: %{})
    field(:background_facts, :map, default: %{})
    field(:colors, {:array, :string}, default: [])
    field(:questions, :map, default: %{})
    field(:topic_tokens, {:array, :string}, default: [])
    field(:attrs, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_key,
      :title,
      :state,
      :version,
      :first_observed_at,
      :updated_at_story,
      :last_material_at,
      :structural_facts,
      :background_facts,
      :colors,
      :questions,
      :topic_tokens,
      :attrs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_key,
      :title,
      :state,
      :version,
      :first_observed_at,
      :updated_at_story,
      :structural_facts,
      :background_facts,
      :colors,
      :questions,
      :topic_tokens,
      :attrs
    ])
    |> validate_inclusion(:state, ["active", "background", "stale", "resolved"])
    |> validate_number(:version, greater_than_or_equal_to: 0)
    |> validate_change(:story_key, fn :story_key, story_key ->
      if String.downcase(story_key || "") in @placeholder_story_keys do
        [story_key: "cannot be a placeholder durable story identity"]
      else
        []
      end
    end)
  end
end

defmodule Primeradiant.StorageHarness.Proposal do
  use Primeradiant.StorageHarness.Schema

  schema "proposals" do
    field(:tenant_id, :binary_id)
    field(:proposal_key, :string)
    field(:agent_run_id, :binary_id)
    field(:actor_id, :string)
    field(:story_id, :binary_id)
    field(:fixture_id, :string)
    field(:classification, :string)
    field(:confidence, :decimal)
    field(:rationale, :string)
    field(:status, :string, default: "pending")
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :proposal_key,
      :agent_run_id,
      :actor_id,
      :story_id,
      :fixture_id,
      :classification,
      :confidence,
      :rationale,
      :status
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :proposal_key,
      :agent_run_id,
      :actor_id,
      :classification,
      :confidence,
      :rationale,
      :status
    ])
    |> validate_inclusion(:status, [
      "pending",
      "accepted",
      "accepted_weak",
      "rejected",
      "needs_more_evidence"
    ])
    |> validate_confidence()
  end
end

defmodule Primeradiant.StorageHarness.ProposalOp do
  use Primeradiant.StorageHarness.Schema

  @allowed ~w(create_input create_story attach_input merge_facts merge_background append_colors add_questions record_conflicts attach_watch attach_story_part_of mark_state mark_last_seen_input record_event)

  schema "proposal_ops" do
    field(:tenant_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:position, :integer)
    field(:op_type, :string)
    field(:payload, :map, default: %{})
    field(:evidence_refs, {:array, :map}, default: [])
    field(:confidence, :decimal)
    field(:status, :string, default: "pending")
    field(:committed_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :proposal_id,
      :position,
      :op_type,
      :payload,
      :evidence_refs,
      :confidence,
      :status,
      :committed_at
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :proposal_id,
      :position,
      :op_type,
      :payload,
      :evidence_refs,
      :confidence,
      :status
    ])
    |> validate_inclusion(:op_type, @allowed)
    |> validate_inclusion(:status, [
      "pending",
      "accepted",
      "accepted_weak",
      "rejected",
      "needs_more_evidence",
      "committed"
    ])
    |> validate_number(:position, greater_than_or_equal_to: 0)
    |> validate_confidence()
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.ProposalDecision do
  use Primeradiant.StorageHarness.Schema

  schema "proposal_decisions" do
    field(:tenant_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:from_status, :string)
    field(:to_status, :string)
    field(:actor_type, :string)
    field(:actor_id, :string)
    field(:evidence_refs, {:array, :map}, default: [])
    field(:confidence, :decimal)
    field(:rationale, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :proposal_id,
      :from_status,
      :to_status,
      :actor_type,
      :actor_id,
      :evidence_refs,
      :confidence,
      :rationale
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :proposal_id,
      :from_status,
      :to_status,
      :actor_type,
      :actor_id,
      :evidence_refs,
      :confidence
    ])
    |> validate_inclusion(:to_status, [
      "accepted",
      "accepted_weak",
      "rejected",
      "needs_more_evidence"
    ])
    |> validate_confidence()
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.GraphCommit do
  use Primeradiant.StorageHarness.Schema

  schema "graph_commits" do
    field(:tenant_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:commit_type, :string)
    field(:committed_by_type, :string)
    field(:committed_by_id, :string)
    field(:evidence_refs, {:array, :map}, default: [])
    field(:confidence, :decimal)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :proposal_id,
      :proposal_op_id,
      :commit_type,
      :committed_by_type,
      :committed_by_id,
      :evidence_refs,
      :confidence
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :proposal_id,
      :proposal_op_id,
      :commit_type,
      :committed_by_type,
      :committed_by_id,
      :evidence_refs,
      :confidence
    ])
    |> validate_confidence()
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.SoupNode do
  use Primeradiant.StorageHarness.Schema

  @types ~w(input story claim entity user_watch authored_output)

  schema "soup_nodes" do
    field(:tenant_id, :binary_id)
    field(:node_key, :string)
    field(:node_type, :string)
    field(:title, :string)
    field(:state, :string, default: "active")
    field(:input_id, :binary_id)
    field(:story_id, :binary_id)
    field(:watch_id, :binary_id)
    field(:authored_output_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:graph_commit_id, :binary_id)
    field(:confidence, :decimal)
    field(:attrs, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :node_key,
      :node_type,
      :title,
      :state,
      :input_id,
      :story_id,
      :watch_id,
      :authored_output_id,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :confidence,
      :attrs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :node_key,
      :node_type,
      :title,
      :state,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :confidence,
      :attrs
    ])
    |> validate_inclusion(:node_type, @types)
    |> validate_inclusion(:state, ["active", "background", "stale", "resolved"])
    |> validate_confidence()
  end
end

defmodule Primeradiant.StorageHarness.Edge do
  use Primeradiant.StorageHarness.Schema

  @types ~w(supports updates duplicates contradicts adds_color part_of watch_applies_to)
  @article_story_edge_metadata ~w(edge_contract link_basis contribution_type source_ref evidence_refs agent_run_id agent_prompt_version agent_output_hash packet_hash correlation_id)

  schema "edges" do
    field(:tenant_id, :binary_id)
    field(:from_node_id, :binary_id)
    field(:to_node_id, :binary_id)
    field(:edge_type, :string)
    field(:status, :string, default: "committed")
    field(:confidence, :decimal)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:graph_commit_id, :binary_id)
    field(:attrs, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :from_node_id,
      :to_node_id,
      :edge_type,
      :status,
      :confidence,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :attrs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :from_node_id,
      :to_node_id,
      :edge_type,
      :status,
      :confidence,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :attrs
    ])
    |> validate_inclusion(:edge_type, @types)
    |> validate_exclusion(:edge_type, ["related"])
    |> validate_inclusion(:status, ["committed"])
    |> validate_confidence()
    |> validate_article_story_edge_metadata()
  end

  defp validate_article_story_edge_metadata(changeset) do
    attrs = get_field(changeset, :attrs) || %{}

    if Map.get(attrs, "edge_contract") == "article_story_contribution" do
      changeset
      |> validate_required_map_keys(:attrs, @article_story_edge_metadata)
    else
      changeset
    end
  end
end

defmodule Primeradiant.StorageHarness.StoryFactVersion do
  use Primeradiant.StorageHarness.Schema

  schema "story_fact_versions" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:claim_node_id, :binary_id)
    field(:fact_key, :string)
    field(:fact_value, :string)
    field(:time_scope, :string, default: "current")
    field(:status, :string, default: "current")
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:graph_commit_id, :binary_id)
    field(:input_id, :binary_id)
    field(:confidence, :decimal)
    field(:replaced_fact_version_id, :binary_id)
    field(:observed_at, :utc_datetime_usec)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :claim_node_id,
      :fact_key,
      :fact_value,
      :time_scope,
      :status,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :input_id,
      :confidence,
      :replaced_fact_version_id,
      :observed_at
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :fact_key,
      :fact_value,
      :time_scope,
      :status,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :input_id,
      :confidence,
      :observed_at
    ])
    |> validate_inclusion(:status, ["current", "replaced"])
    |> validate_confidence()
  end
end

defmodule Primeradiant.StorageHarness.StoryEvent do
  use Primeradiant.StorageHarness.Schema

  schema "story_events" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:input_id, :binary_id)
    field(:classification, :string)
    field(:story_version, :integer)
    field(:changed_facts, :map, default: %{})
    field(:observed_at, :utc_datetime_usec)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:graph_commit_id, :binary_id)
    field(:confidence, :decimal)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :input_id,
      :classification,
      :story_version,
      :changed_facts,
      :observed_at,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :confidence
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :input_id,
      :classification,
      :story_version,
      :changed_facts,
      :observed_at,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :confidence
    ])
    |> validate_confidence()
  end
end

defmodule Primeradiant.StorageHarness.StoryCardVersion do
  use Primeradiant.StorageHarness.Schema

  @refresh_reasons ~w(story_created source_linked source_content_changed source_weight_changed reader_delta_requested repair_backfill stale_recheck manual_review active_story_recurring_15m story_card_hourly_synthesis daily_deep_soup_sweep)
  @statuses ~w(complete incomplete refused unavailable)

  schema "story_card_versions" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:story_version, :integer)
    field(:card_version, :integer)
    field(:status, :string)
    field(:supersedes_id, :binary_id)
    field(:refresh_reason, :string)
    field(:producing_agent_run_id, :binary_id)
    field(:packet_hash, :string)
    field(:prompt_config_hash, :string)
    field(:output_hash, :string)
    field(:field_provenance_manifest_id, :string)
    field(:title, :map, default: %{})
    field(:deck, :map, default: %{})
    field(:summary, :map, default: %{})
    field(:freshness, :map, default: %{})
    field(:field_completeness, :map, default: %{})
    field(:topic_salience, :map, default: %{})
    field(:provenance, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :story_version,
      :card_version,
      :status,
      :supersedes_id,
      :refresh_reason,
      :producing_agent_run_id,
      :packet_hash,
      :prompt_config_hash,
      :output_hash,
      :field_provenance_manifest_id,
      :title,
      :deck,
      :summary,
      :freshness,
      :field_completeness,
      :topic_salience,
      :provenance
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :story_version,
      :card_version,
      :status,
      :refresh_reason,
      :producing_agent_run_id,
      :packet_hash,
      :prompt_config_hash,
      :output_hash,
      :field_provenance_manifest_id,
      :title,
      :deck,
      :summary,
      :freshness,
      :field_completeness,
      :topic_salience,
      :provenance
    ])
    |> validate_inclusion(:status, @statuses)
    |> validate_inclusion(:refresh_reason, @refresh_reasons)
    |> validate_number(:story_version, greater_than_or_equal_to: 1)
    |> validate_number(:card_version, greater_than_or_equal_to: 1)
  end
end

defmodule Primeradiant.StorageHarness.StorySourceCoverage do
  use Primeradiant.StorageHarness.Schema

  schema "story_source_coverage" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:story_card_version_id, :binary_id)
    field(:source_ref, :string)
    field(:article_ref, :string)
    field(:canonical_public_url, :map, default: %{})
    field(:source_domain, :map, default: %{})
    field(:source_label, :map, default: %{})
    field(:publication, :map, default: %{})
    field(:source_posture, :map, default: %{})
    field(:contribution_reason, :map, default: %{})
    field(:materiality, :string)
    field(:source_weight, :map, default: %{})
    field(:first_observed_at, :utc_datetime_usec)
    field(:last_observed_at, :utc_datetime_usec)
    field(:evidence_refs, {:array, :string}, default: [])
    field(:provenance_refs, {:array, :string}, default: [])
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :story_card_version_id,
      :source_ref,
      :article_ref,
      :canonical_public_url,
      :source_domain,
      :source_label,
      :publication,
      :source_posture,
      :contribution_reason,
      :materiality,
      :source_weight,
      :first_observed_at,
      :last_observed_at,
      :evidence_refs,
      :provenance_refs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :story_card_version_id,
      :source_ref,
      :canonical_public_url,
      :source_domain,
      :source_label,
      :publication,
      :contribution_reason,
      :materiality,
      :first_observed_at,
      :last_observed_at,
      :evidence_refs,
      :provenance_refs
    ])
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.StoryKeyClaim do
  use Primeradiant.StorageHarness.Schema

  @statuses ~w(current disputed stale background unresolved)

  schema "story_key_claims" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:story_card_version_id, :binary_id)
    field(:claim_ref, :string)
    field(:text, :string)
    field(:status, :string)
    field(:materiality, :string)
    field(:evidence_refs, {:array, :string}, default: [])
    field(:conflict_refs, {:array, :string}, default: [])
    field(:uncertainty, :map, default: %{})
    field(:appears_in_current_card, :boolean, default: true)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :story_card_version_id,
      :claim_ref,
      :text,
      :status,
      :materiality,
      :evidence_refs,
      :conflict_refs,
      :uncertainty,
      :appears_in_current_card
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :story_card_version_id,
      :claim_ref,
      :text,
      :status,
      :materiality,
      :evidence_refs,
      :conflict_refs,
      :uncertainty,
      :appears_in_current_card
    ])
    |> validate_inclusion(:status, @statuses)
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.StoryCardChangeSet do
  use Primeradiant.StorageHarness.Schema

  schema "story_card_change_sets" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:prior_card_version_id, :binary_id)
    field(:new_card_version_id, :binary_id)
    field(:changed_field_keys, {:array, :string}, default: [])
    field(:added_claim_refs, {:array, :string}, default: [])
    field(:removed_claim_refs, {:array, :string}, default: [])
    field(:changed_claim_refs, {:array, :string}, default: [])
    field(:changed_source_coverage_refs, {:array, :string}, default: [])
    field(:refresh_reason, :string)
    field(:change_summary, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :prior_card_version_id,
      :new_card_version_id,
      :changed_field_keys,
      :added_claim_refs,
      :removed_claim_refs,
      :changed_claim_refs,
      :changed_source_coverage_refs,
      :refresh_reason,
      :change_summary
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :new_card_version_id,
      :changed_field_keys,
      :added_claim_refs,
      :removed_claim_refs,
      :changed_claim_refs,
      :changed_source_coverage_refs,
      :refresh_reason,
      :change_summary
    ])
  end
end

defmodule Primeradiant.StorageHarness.StoryReaderDelta do
  use Primeradiant.StorageHarness.Schema

  schema "story_reader_deltas" do
    field(:tenant_id, :binary_id)
    field(:user_id, :string)
    field(:story_id, :binary_id)
    field(:seen_state_id, :binary_id)
    field(:prior_seen_story_version, :integer)
    field(:prior_seen_card_version_id, :binary_id)
    field(:current_story_version, :integer)
    field(:current_card_version_id, :binary_id)
    field(:material_unseen_deltas, {:array, :map}, default: [])
    field(:nonmaterial_exclusions, {:array, :map}, default: [])
    field(:producing_agent_run_id, :binary_id)
    field(:evidence_refs, {:array, :string}, default: [])
    field(:provenance_refs, {:array, :string}, default: [])
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :user_id,
      :story_id,
      :seen_state_id,
      :prior_seen_story_version,
      :prior_seen_card_version_id,
      :current_story_version,
      :current_card_version_id,
      :material_unseen_deltas,
      :nonmaterial_exclusions,
      :producing_agent_run_id,
      :evidence_refs,
      :provenance_refs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :user_id,
      :story_id,
      :prior_seen_story_version,
      :current_story_version,
      :current_card_version_id,
      :material_unseen_deltas,
      :nonmaterial_exclusions,
      :producing_agent_run_id,
      :evidence_refs,
      :provenance_refs
    ])
  end
end

defmodule Primeradiant.StorageHarness.StoryCardProjectionAudit do
  use Primeradiant.StorageHarness.Schema

  schema "story_card_projection_audits" do
    field(:tenant_id, :binary_id)
    field(:projection_id, :string)
    field(:consumer, :string)
    field(:query_time, :utc_datetime_usec)
    field(:cursor, :string)
    field(:story_card_version_ids, {:array, :string}, default: [])
    field(:omitted_story_reasons, {:array, :map}, default: [])
    field(:visibility_scope, :map, default: %{})
    field(:status, :string)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :projection_id,
      :consumer,
      :query_time,
      :cursor,
      :story_card_version_ids,
      :omitted_story_reasons,
      :visibility_scope,
      :status
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :projection_id,
      :consumer,
      :query_time,
      :cursor,
      :story_card_version_ids,
      :omitted_story_reasons,
      :visibility_scope,
      :status
    ])
    |> validate_inclusion(:status, ["complete", "partial", "stale"])
  end
end

defmodule Primeradiant.StorageHarness.Conflict do
  use Primeradiant.StorageHarness.Schema

  schema "conflicts" do
    field(:tenant_id, :binary_id)
    field(:story_id, :binary_id)
    field(:fact_key, :string)
    field(:prior_value, :string)
    field(:incoming_value, :string)
    field(:status, :string, default: "open")
    field(:input_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:graph_commit_id, :binary_id)
    field(:agent_run_id, :binary_id)
    field(:confidence, :decimal)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :story_id,
      :fact_key,
      :prior_value,
      :incoming_value,
      :status,
      :input_id,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :agent_run_id,
      :confidence
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :story_id,
      :fact_key,
      :prior_value,
      :incoming_value,
      :status,
      :input_id,
      :proposal_id,
      :proposal_op_id,
      :graph_commit_id,
      :agent_run_id,
      :confidence
    ])
    |> validate_inclusion(:status, ["open", "accepted_correction", "resolved", "rejected"])
    |> validate_confidence()
  end
end

defmodule Primeradiant.StorageHarness.AuthoredOutput do
  use Primeradiant.StorageHarness.Schema

  schema "authored_outputs" do
    field(:tenant_id, :binary_id)
    field(:user_id, :string)
    field(:story_id, :binary_id)
    field(:output_type, :string)
    field(:content, :string)
    field(:evidence_packet, :map, default: %{})
    field(:verified, :boolean, default: false)
    field(:story_version, :integer)
    field(:status, :string, default: "recorded")
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :user_id,
      :story_id,
      :output_type,
      :content,
      :evidence_packet,
      :verified,
      :story_version,
      :status
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :user_id,
      :output_type,
      :content,
      :evidence_packet,
      :verified,
      :status
    ])
    |> validate_inclusion(:status, ["recorded"])
  end
end

defmodule Primeradiant.StorageHarness.AuthoredOutputUnit do
  use Primeradiant.StorageHarness.Schema

  schema "authored_output_units" do
    field(:tenant_id, :binary_id)
    field(:authored_output_id, :binary_id)
    field(:position, :integer)
    field(:unit_type, :string)
    field(:content, :string)
    field(:story_id, :binary_id)
    field(:evidence_refs, {:array, :map}, default: [])
    field(:claim_refs, {:array, :map}, default: [])
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :authored_output_id,
      :position,
      :unit_type,
      :content,
      :story_id,
      :evidence_refs,
      :claim_refs
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :authored_output_id,
      :position,
      :unit_type,
      :content,
      :evidence_refs,
      :claim_refs
    ])
    |> validate_non_empty_list(:evidence_refs)
  end
end

defmodule Primeradiant.StorageHarness.SeenState do
  use Primeradiant.StorageHarness.Schema

  schema "seen_states" do
    field(:tenant_id, :binary_id)
    field(:user_id, :string)
    field(:story_id, :binary_id)
    field(:seen_story_version, :integer)
    field(:last_authored_output_id, :binary_id)
    field(:seen_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :user_id,
      :story_id,
      :seen_story_version,
      :last_authored_output_id,
      :seen_at
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :user_id,
      :story_id,
      :seen_story_version,
      :last_authored_output_id,
      :seen_at
    ])
  end
end

defmodule Primeradiant.StorageHarness.SeenStateRef do
  use Primeradiant.StorageHarness.Schema

  @kinds ~w(input claim story edge conflict authored_output)

  schema "seen_state_refs" do
    field(:tenant_id, :binary_id)
    field(:seen_state_id, :binary_id)
    field(:ref_kind, :string)
    field(:ref_id, :string)
    field(:authored_output_id, :binary_id)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:id, :tenant_id, :seen_state_id, :ref_kind, :ref_id, :authored_output_id])
    |> put_id()
    |> validate_required([:tenant_id, :seen_state_id, :ref_kind, :ref_id, :authored_output_id])
    |> validate_inclusion(:ref_kind, @kinds)
  end
end

defmodule Primeradiant.StorageHarness.EvidenceRef do
  use Primeradiant.StorageHarness.Schema

  schema "evidence_refs" do
    field(:tenant_id, :binary_id)
    field(:subject_type, :string)
    field(:subject_id, :binary_id)
    field(:input_id, :binary_id)
    field(:soup_node_id, :binary_id)
    field(:proposal_id, :binary_id)
    field(:proposal_op_id, :binary_id)
    field(:edge_id, :binary_id)
    field(:conflict_id, :binary_id)
    field(:authored_output_id, :binary_id)
    field(:authored_output_unit_id, :binary_id)
    field(:span_start, :integer)
    field(:span_end, :integer)
    field(:evidence_label, :string)
    field(:evidence_hash, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :subject_type,
      :subject_id,
      :input_id,
      :soup_node_id,
      :proposal_id,
      :proposal_op_id,
      :edge_id,
      :conflict_id,
      :authored_output_id,
      :authored_output_unit_id,
      :span_start,
      :span_end,
      :evidence_label,
      :evidence_hash
    ])
    |> put_id()
    |> validate_required([:tenant_id, :subject_type, :subject_id, :input_id])
    |> validate_subject_fk()
  end

  defp validate_subject_fk(changeset) do
    subject_type = get_field(changeset, :subject_type)

    subject_contract =
      case subject_type do
        "soup_node" -> {:direct, :soup_node_id}
        "proposal" -> {:direct, :proposal_id}
        "proposal_op" -> {:direct, :proposal_op_id}
        "edge" -> {:direct, :edge_id}
        "conflict" -> {:direct, :conflict_id}
        "authored_output" -> {:direct, :authored_output_id}
        "authored_output_unit" -> {:direct, :authored_output_unit_id}
        "story_fact_version" -> {:via_proposal_op, [:proposal_id, :proposal_op_id]}
        "story_event" -> {:via_proposal_op, [:proposal_id, :proposal_op_id]}
        "graph_commit" -> {:via_proposal_op, [:proposal_id, :proposal_op_id]}
        _ -> :unknown
      end

    case subject_contract do
      :unknown ->
        add_error(changeset, :subject_type, "is not supported")

      {:direct, required_fk} ->
        cond do
          is_nil(get_field(changeset, required_fk)) ->
            add_error(changeset, required_fk, "must be present for #{subject_type} evidence")

          get_field(changeset, required_fk) != get_field(changeset, :subject_id) ->
            add_error(
              changeset,
              required_fk,
              "must match subject_id for #{subject_type} evidence"
            )

          true ->
            changeset
        end

      {:via_proposal_op, required_fks} ->
        Enum.reduce(required_fks, changeset, fn field, acc ->
          if is_nil(get_field(acc, field)) do
            add_error(acc, field, "must be present for #{subject_type} evidence")
          else
            acc
          end
        end)
    end
  end
end

defmodule Primeradiant.StorageHarness.RepairRun do
  use Primeradiant.StorageHarness.Schema

  schema "repair_runs" do
    field(:tenant_id, :binary_id)
    field(:plan_hash, :string)
    field(:snapshot_hash, :string)
    field(:snapshot_path, :string)
    field(:source_db_path, :string)
    field(:source_commit, :string)
    field(:approval_evidence, :string)
    field(:actor, :string)
    field(:status, :string)
    field(:started_at, :utc_datetime_usec)
    field(:finished_at, :utc_datetime_usec)
    field(:mutation_ids, {:array, :string}, default: [])
    field(:rollback_proof, :map, default: %{})
    field(:validation, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :plan_hash,
      :snapshot_hash,
      :snapshot_path,
      :source_db_path,
      :source_commit,
      :approval_evidence,
      :actor,
      :status,
      :started_at,
      :finished_at,
      :mutation_ids,
      :rollback_proof,
      :validation
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :plan_hash,
      :snapshot_hash,
      :snapshot_path,
      :source_db_path,
      :source_commit,
      :approval_evidence,
      :actor,
      :status,
      :started_at,
      :mutation_ids,
      :rollback_proof,
      :validation
    ])
    |> validate_inclusion(:status, ["running", "succeeded", "failed", "rolled_back"])
  end
end

defmodule Primeradiant.StorageHarness.StoryQuarantine do
  use Primeradiant.StorageHarness.Schema

  schema "story_quarantines" do
    field(:tenant_id, :binary_id)
    field(:repair_run_id, :binary_id)
    field(:story_id, :binary_id)
    field(:reason, :string)
    field(:original_story_key, :string)
    field(:original_story_state, :string)
    field(:preserved_ids, :map, default: %{})
    field(:source_refs, {:array, :string}, default: [])
    field(:quarantined_at, :utc_datetime_usec)
    field(:rollback_status, :string, default: "snapshot_available")
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :repair_run_id,
      :story_id,
      :reason,
      :original_story_key,
      :original_story_state,
      :preserved_ids,
      :source_refs,
      :quarantined_at,
      :rollback_status
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :repair_run_id,
      :story_id,
      :reason,
      :original_story_key,
      :original_story_state,
      :preserved_ids,
      :source_refs,
      :quarantined_at,
      :rollback_status
    ])
    |> validate_inclusion(:rollback_status, ["snapshot_available", "restored", "not_required"])
  end
end

defmodule Primeradiant.StorageHarness.SourceRegistration do
  use Primeradiant.StorageHarness.Schema

  schema "source_registrations" do
    field(:tenant_id, :binary_id)
    field(:source_key, :string)
    field(:adapter_module, :string)
    field(:adapter_version, :string)
    field(:mode, :string)
    field(:resolution_policy, :map, default: %{})
    field(:policy_version, :string)
    field(:policy_hash, :string)
    field(:budgets, :map, default: %{})
    field(:config, :map, default: %{})
    field(:cursor, :map, default: %{})
    field(:last_received_at, :utc_datetime_usec)
    field(:last_resolution_terminal_at, :utc_datetime_usec)
    field(:last_admission_at, :utc_datetime_usec)
    field(:gap_count, :integer, default: 0)
    field(:refusal_count, :integer, default: 0)
    field(:unresolved_count, :integer, default: 0)
    field(:quarantine_count, :integer, default: 0)
    field(:circuit_state, :map, default: %{})
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields = [
      :id,
      :tenant_id,
      :source_key,
      :adapter_module,
      :adapter_version,
      :mode,
      :resolution_policy,
      :policy_version,
      :policy_hash,
      :budgets,
      :config,
      :cursor,
      :last_received_at,
      :last_resolution_terminal_at,
      :last_admission_at,
      :gap_count,
      :refusal_count,
      :unresolved_count,
      :quarantine_count,
      :circuit_state
    ]

    struct
    |> cast(attrs, fields)
    |> put_id()
    |> validate_required(
      fields -- [:id, :last_received_at, :last_resolution_terminal_at, :last_admission_at]
    )
  end
end

defmodule Primeradiant.StorageHarness.SourceGapRecord do
  use Primeradiant.StorageHarness.Schema

  schema "source_gap_records" do
    field(:tenant_id, :binary_id)
    field(:source_key, :string)
    field(:source_position, :string)
    field(:status, :string)
    field(:opened_at, :utc_datetime_usec)
    field(:closed_at, :utc_datetime_usec)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :source_key,
      :source_position,
      :status,
      :opened_at,
      :closed_at
    ])
    |> put_id()
    |> validate_required([:tenant_id, :source_key, :source_position, :status, :opened_at])
  end
end

defmodule Primeradiant.StorageHarness.RawEnvelope do
  use Primeradiant.StorageHarness.Schema

  schema "raw_envelopes" do
    field(:tenant_id, :binary_id)
    field(:source_key, :string)
    field(:adapter_version, :string)
    field(:source_event_external_id, :string)
    field(:received_at, :utc_datetime_usec)
    field(:content_digest, :string)
    field(:integrity_metadata, :map, default: %{})
    field(:raw_object_ref, :string)
    field(:retained_bytes, :string)
    field(:visibility, :string)
    field(:correlation_id, :string)
    field(:idempotency_key, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :source_key,
      :adapter_version,
      :source_event_external_id,
      :received_at,
      :content_digest,
      :integrity_metadata,
      :raw_object_ref,
      :retained_bytes,
      :visibility,
      :correlation_id,
      :idempotency_key
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :source_key,
      :adapter_version,
      :source_event_external_id,
      :received_at,
      :content_digest,
      :integrity_metadata,
      :visibility,
      :correlation_id,
      :idempotency_key
    ])
    |> validate_raw_material()
  end

  defp validate_raw_material(changeset) do
    if get_field(changeset, :raw_object_ref) || get_field(changeset, :retained_bytes) do
      changeset
    else
      add_error(changeset, :raw_object_ref, "or retained_bytes must be present")
    end
  end
end

defmodule Primeradiant.StorageHarness.ResolutionCase do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_cases" do
    field(:tenant_id, :binary_id)
    field(:raw_envelope_id, :binary_id)
    field(:policy_version, :string)
    field(:state, :string)
    field(:attempt_count, :integer, default: 0)
    field(:next_retry_at, :utc_datetime_usec)
    field(:outcome_code, :string)
    field(:config_policy_hash, :string)
    field(:policy_snapshot, :map)
    field(:trace_id, :string)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :raw_envelope_id,
      :policy_version,
      :state,
      :attempt_count,
      :next_retry_at,
      :outcome_code,
      :config_policy_hash,
      :policy_snapshot,
      :trace_id
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :raw_envelope_id,
      :policy_version,
      :state,
      :attempt_count,
      :config_policy_hash,
      :trace_id
    ])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionEvidence do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_evidence" do
    field(:tenant_id, :binary_id)
    field(:resolution_case_id, :binary_id)
    field(:kind, :string)
    field(:value, :string)
    field(:protected_ref, :string)
    field(:source, :string)
    field(:locator, :map, default: %{})
    field(:span_start, :integer)
    field(:span_end, :integer)
    field(:digest, :string)
    field(:retrieved_at, :utc_datetime_usec)
    field(:visibility, :string)
    field(:provenance, :map, default: %{})
    field(:transformation_chain, {:array, :map}, default: [])
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :resolution_case_id,
      :kind,
      :value,
      :protected_ref,
      :source,
      :locator,
      :span_start,
      :span_end,
      :digest,
      :retrieved_at,
      :visibility,
      :provenance,
      :transformation_chain
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :resolution_case_id,
      :kind,
      :source,
      :locator,
      :digest,
      :retrieved_at,
      :visibility,
      :provenance,
      :transformation_chain
    ])
    |> validate_evidence_material()
  end

  defp validate_evidence_material(changeset) do
    if get_field(changeset, :value) || get_field(changeset, :protected_ref) do
      changeset
    else
      add_error(changeset, :value, "or protected_ref must be present")
    end
  end
end

defmodule Primeradiant.StorageHarness.ResolvedSourceField do
  use Primeradiant.StorageHarness.Schema

  schema "resolved_source_fields" do
    field(:tenant_id, :binary_id)
    field(:resolution_case_id, :binary_id)
    field(:field_name, :string)
    field(:normalized_value, :string)
    field(:confidence, :decimal)
    field(:evidence_refs, {:array, :string}, default: [])
    field(:derivation_evidence_ref, :string)
    field(:resolver_provenance, {:array, :map}, default: [])
    field(:transform, :string)
    field(:contradiction_status, :string)
    field(:selected, :boolean, default: false)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :resolution_case_id,
      :field_name,
      :normalized_value,
      :confidence,
      :evidence_refs,
      :derivation_evidence_ref,
      :resolver_provenance,
      :transform,
      :contradiction_status,
      :selected
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :resolution_case_id,
      :field_name,
      :normalized_value,
      :confidence,
      :evidence_refs,
      :derivation_evidence_ref,
      :resolver_provenance,
      :transform,
      :contradiction_status,
      :selected
    ])
    |> validate_confidence()
    |> validate_non_empty_list(:evidence_refs)
    |> validate_non_empty_list(:resolver_provenance)
  end
end

defmodule Primeradiant.StorageHarness.ResolutionAttempt do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_attempts" do
    field(:tenant_id, :binary_id)
    field(:resolution_case_id, :binary_id)
    field(:raw_envelope_id, :binary_id)
    field(:raw_envelope_digest, :string)
    field(:attempt_key, :string)
    field(:stage, :string)
    field(:resolver, :string)
    field(:input_hash, :string)
    field(:attempt_ordinal, :integer)
    field(:budgets_consumed, :map, default: %{})
    field(:outcome, :string)
    field(:error_class, :string)
    field(:response_evidence_refs, {:array, :string}, default: [])
    field(:started_at, :utc_datetime_usec)
    field(:ended_at, :utc_datetime_usec)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields = [
      :id,
      :tenant_id,
      :resolution_case_id,
      :raw_envelope_id,
      :raw_envelope_digest,
      :attempt_key,
      :stage,
      :resolver,
      :input_hash,
      :attempt_ordinal,
      :budgets_consumed,
      :outcome,
      :error_class,
      :response_evidence_refs,
      :started_at,
      :ended_at
    ]

    struct
    |> cast(attrs, fields)
    |> put_id()
    |> validate_required(fields -- [:id, :resolver, :error_class])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionOutcome do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_outcomes" do
    field(:tenant_id, :binary_id)
    field(:resolution_case_id, :binary_id)
    field(:outcome_code, :string)
    field(:reason, :string)
    field(:retryable, :boolean)
    field(:quarantine_ref, :string)
    field(:validator_version, :string)
    field(:admission_material_ref, :string)
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :resolution_case_id,
      :outcome_code,
      :reason,
      :retryable,
      :quarantine_ref,
      :validator_version,
      :admission_material_ref
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :resolution_case_id,
      :outcome_code,
      :reason,
      :retryable,
      :validator_version
    ])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionBackfillPlan do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_backfill_plans" do
    field(:tenant_id, :binary_id)
    field(:selection, :map)
    field(:selection_version, :string)
    field(:candidates, {:array, :map}, default: [])
    field(:source_versions, :map)
    field(:policy_snapshots, :map)
    field(:estimated_budgets, :map)
    field(:exclusion_rules, {:array, :string}, default: [])
    field(:content_hash, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields =
      ~w(id tenant_id selection selection_version candidates source_versions policy_snapshots estimated_budgets exclusion_rules content_hash inserted_at)a

    struct |> cast(attrs, fields) |> put_id() |> validate_required(fields -- [:id, :inserted_at])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionBackfillApproval do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_backfill_approvals" do
    field(:tenant_id, :binary_id)
    field(:plan_id, :binary_id)
    field(:plan_hash, :string)
    field(:actor_kind, :string)
    field(:actor_id, :string)
    field(:approved_at, :utc_datetime_usec)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields = ~w(id tenant_id plan_id plan_hash actor_kind actor_id approved_at inserted_at)a
    struct |> cast(attrs, fields) |> put_id() |> validate_required(fields -- [:id, :inserted_at])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionBackfillApplication do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_backfill_applications" do
    field(:tenant_id, :binary_id)
    field(:run_id, :string)
    field(:plan_id, :binary_id)
    field(:raw_envelope_id, :binary_id)
    field(:historical_case_id, :binary_id)
    field(:resolution_case_id, :binary_id)
    field(:policy_hash, :string)
    field(:idempotency_key, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields =
      ~w(id tenant_id run_id plan_id raw_envelope_id historical_case_id resolution_case_id policy_hash idempotency_key inserted_at)a

    struct |> cast(attrs, fields) |> put_id() |> validate_required(fields -- [:id, :inserted_at])
  end
end

defmodule Primeradiant.StorageHarness.ResolutionBackfillRun do
  use Primeradiant.StorageHarness.Schema

  schema "resolution_backfill_runs" do
    field(:tenant_id, :binary_id)
    field(:run_id, :string)
    field(:plan_id, :binary_id)
    field(:applied_plan_hash, :string)
    field(:status, :string)
    field(:counts, :map)
    field(:dispositions, {:array, :map}, default: [])
    field(:duplicate_count, :integer, default: 0)
    field(:residual_proof, :map)
    field(:completed_at, :utc_datetime_usec)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    fields =
      ~w(id tenant_id run_id plan_id applied_plan_hash status counts dispositions duplicate_count residual_proof completed_at inserted_at)a

    struct
    |> cast(attrs, fields)
    |> put_id()
    |> validate_required(fields -- [:id, :inserted_at, :completed_at])
  end
end

defmodule Primeradiant.StorageHarness.PackageAcknowledgement do
  use Primeradiant.StorageHarness.Schema

  schema "package_acknowledgements" do
    field(:tenant_id, :binary_id)
    field(:package_id, :string)
    field(:manifest_digest, :string)
    field(:source_position_range, :map, default: %{})
    field(:status, :string)
    field(:envelope_disposition_refs, {:array, :map}, default: [])
    field(:policy_hash, :string)
    field(:completed_at, :utc_datetime_usec)
    field(:trace_id, :string)
    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :id,
      :tenant_id,
      :package_id,
      :manifest_digest,
      :source_position_range,
      :status,
      :envelope_disposition_refs,
      :policy_hash,
      :completed_at,
      :trace_id
    ])
    |> put_id()
    |> validate_required([
      :tenant_id,
      :package_id,
      :manifest_digest,
      :source_position_range,
      :status,
      :envelope_disposition_refs,
      :policy_hash,
      :completed_at,
      :trace_id
    ])
    |> validate_non_empty_list(:envelope_disposition_refs)
  end
end
