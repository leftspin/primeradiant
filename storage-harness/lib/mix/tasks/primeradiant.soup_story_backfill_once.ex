defmodule Mix.Tasks.Primeradiant.SoupStoryBackfillOnce do
  @moduledoc false
  use Mix.Task

  alias Primeradiant.StorageHarness.{DurableSoupDb, LiveStoryAgentLoop}

  @shortdoc "Run one bounded story-agent pass over admitted soup inputs without story events"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          soup_db: :string,
          tenant: :string,
          actor: :string,
          limit: :integer
        ]
      )

    soup_db = Keyword.fetch!(opts, :soup_db)
    tenant = Keyword.fetch!(opts, :tenant)
    actor = Keyword.get(opts, :actor, "flynn")
    limit = Keyword.get(opts, :limit, 8)

    revision = DurableSoupDb.tenant_revision(soup_db, tenant)
    state = DurableSoupDb.load_tenant(soup_db, tenant)

    {state, report} =
      LiveStoryAgentLoop.run_unstoried_inputs(state, actor, limit: limit)

    DurableSoupDb.persist!(soup_db, state, %{
      source_kind: "admitted-soup-story-backfill",
      source_db_path: soup_db,
      source_row_count: 0,
      expected_tenant_revision: revision
    })

    Mix.shell().info(Jason.encode!(report, pretty: true))
  end
end
