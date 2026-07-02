defmodule Mix.Tasks.Primeradiant.T1421CleanupPlan do
  @moduledoc false

  use Mix.Task

  alias Primeradiant.StorageHarness.LiveDataCleanupPlan

  @shortdoc "Emit the T1421 live-data cleanup dry-run plan for a soup DB snapshot"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          soup_db: :string,
          tenant: :string,
          limit: :integer,
          statuses: :string,
          inserted_before: :string
        ]
      )

    Mix.Task.run("app.start")

    plan =
      LiveDataCleanupPlan.build(
        soup_db: Keyword.fetch!(opts, :soup_db),
        tenant: Keyword.fetch!(opts, :tenant),
        limit: Keyword.get(opts, :limit, 80),
        statuses: parse_statuses(Keyword.get(opts, :statuses, "incomplete,refused")),
        inserted_before: Keyword.get(opts, :inserted_before)
      )

    Mix.shell().info(Jason.encode!(plan))
  end

  defp parse_statuses(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
  end
end
