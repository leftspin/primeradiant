defmodule Mix.Tasks.Primeradiant.DaemonNewsReplay do
  @moduledoc false
  use Mix.Task

  alias Primeradiant.StorageHarness.DaemonNewsReplay

  @shortdoc "Replay daemon news DB rows through Primeradiant storage harness"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          db: :string,
          raw_root: :string,
          tenant: :string,
          actor: :string,
          soup_db: :string,
          limit: :integer
        ]
      )

    tenant_id = Keyword.get(opts, :tenant) || Ecto.UUID.generate()

    replay_opts = [
      db_path: Keyword.get(opts, :db, DaemonNewsReplay.default_db_path()),
      raw_root: Keyword.get(opts, :raw_root),
      tenant_id: tenant_id,
      actor_id: Keyword.get(opts, :actor, "flynn"),
      soup_db_path: Keyword.get(opts, :soup_db, DaemonNewsReplay.default_soup_db_path()),
      limit: Keyword.get(opts, :limit)
    ]

    case DaemonNewsReplay.replay(replay_opts) do
      {:ok, _state, report} ->
        Mix.shell().info(Jason.encode!(report, pretty: true))

      {:error, reason} ->
        Mix.raise("daemon news replay failed: #{inspect(reason)}")
    end
  end
end
