defmodule Mix.Tasks.Primeradiant.T1421CleanupApply do
  @moduledoc false

  use Mix.Task

  alias Primeradiant.StorageHarness.LiveDataCleanupApply

  @shortdoc "Apply an approved T1421 cleanup plan to a soup DB"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          soup_db: :string,
          tenant: :string,
          plan: :string,
          plan_hash: :string,
          approved_by: :string,
          approval_ref: :string,
          actor: :string,
          snapshot_hash: :string
        ]
      )

    Mix.Task.run("app.start")

    report =
      LiveDataCleanupApply.apply!(
        soup_db: Keyword.fetch!(opts, :soup_db),
        tenant: Keyword.fetch!(opts, :tenant),
        plan: Keyword.fetch!(opts, :plan),
        plan_hash: Keyword.fetch!(opts, :plan_hash),
        approved_by: Keyword.get(opts, :approved_by),
        approval_ref: Keyword.get(opts, :approval_ref),
        actor: Keyword.get(opts, :actor),
        snapshot_hash: Keyword.get(opts, :snapshot_hash)
      )

    Mix.shell().info(Jason.encode!(report))
  end
end
