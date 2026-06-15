defmodule Mix.Tasks.Primeradiant.UvealResearchSoupIngest do
  @moduledoc false
  use Mix.Task

  alias Primeradiant.StorageHarness.UvealResearchSoupIngest

  @shortdoc "Ingest uveal cancer research morning metadata into Prime Radiant soup"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          meta: :string,
          tenant: :string,
          actor: :string,
          soup_db: :string
        ]
      )

    meta_path = Keyword.fetch!(opts, :meta)

    ingest_opts = [
      tenant_id: Keyword.fetch!(opts, :tenant),
      actor_id: Keyword.get(opts, :actor, "flynn"),
      soup_db_path: Keyword.get(opts, :soup_db, UvealResearchSoupIngest.default_soup_db_path())
    ]

    case UvealResearchSoupIngest.ingest_meta(meta_path, ingest_opts) do
      {:ok, _state, report} ->
        Mix.shell().info(Jason.encode!(report, pretty: true))

      {:error, reason} ->
        Mix.raise("uveal research soup ingest failed: #{inspect(reason)}")
    end
  end
end
