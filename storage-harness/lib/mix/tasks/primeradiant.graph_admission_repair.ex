defmodule Mix.Tasks.Primeradiant.GraphAdmissionRepair do
  @moduledoc false

  use Mix.Task

  alias Primeradiant.StorageHarness.GraphAdmissionRepair

  @shortdoc "Plan or apply the protected graph-admission repair"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, invalid} =
      OptionParser.parse(args,
        strict: [
          dry_run: :boolean,
          apply: :boolean,
          source_db: :string,
          snapshot: :string,
          soup_db: :string,
          tenant: :string,
          source_commit: :string,
          plan: :string,
          approved_plan_hash: :string,
          approval_evidence: :string,
          actor: :string
        ]
      )

    if invalid != [], do: raise(ArgumentError, "invalid options: #{inspect(invalid)}")
    Mix.Task.run("app.start")

    case {Keyword.get(opts, :dry_run, false), Keyword.get(opts, :apply, false)} do
      {true, false} -> dry_run(opts)
      {false, true} -> apply(opts)
      _ -> raise ArgumentError, "choose exactly one of --dry-run or --apply"
    end
  end

  defp dry_run(opts) do
    plan =
      GraphAdmissionRepair.build_plan(
        source_db: Keyword.get(opts, :source_db),
        snapshot: Keyword.fetch!(opts, :snapshot),
        tenant: Keyword.fetch!(opts, :tenant),
        source_commit: Keyword.get(opts, :source_commit)
      )

    output = Jason.encode!(plan)

    case Keyword.get(opts, :plan) do
      nil -> :ok
      path -> File.write!(path, output <> "\n")
    end

    Mix.shell().info(output)
  end

  defp apply(opts) do
    report =
      GraphAdmissionRepair.apply!(
        soup_db: Keyword.fetch!(opts, :soup_db),
        plan: Keyword.fetch!(opts, :plan),
        approved_plan_hash: Keyword.get(opts, :approved_plan_hash),
        approval_evidence: Keyword.get(opts, :approval_evidence),
        actor: Keyword.get(opts, :actor)
      )

    Mix.shell().info(Jason.encode!(report))
  end
end
