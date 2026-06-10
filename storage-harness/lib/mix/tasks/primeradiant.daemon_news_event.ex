defmodule Mix.Tasks.Primeradiant.DaemonNewsEvent do
  @moduledoc false
  use Mix.Task

  alias Primeradiant.StorageHarness.DaemonNewsEvent

  @shortdoc "Consume daemon-news committed-source-item event envelopes"

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          event: :string,
          raw_root: :string,
          tenant: :string,
          actor: :string,
          soup_db: :string,
          story_agents: :boolean
        ]
      )

    event_path = Keyword.fetch!(opts, :event)
    event = event_path |> File.read!() |> Jason.decode!()

    consume_opts = [
      raw_root: Keyword.get(opts, :raw_root),
      tenant_id: Keyword.get(opts, :tenant) || get_in(event, ["source", "tenant_id"]),
      actor_id: Keyword.get(opts, :actor, "flynn"),
      soup_db_path: Keyword.get(opts, :soup_db, DaemonNewsEvent.default_soup_db_path()),
      story_agent_loop?: Keyword.get(opts, :story_agents, false)
    ]

    case DaemonNewsEvent.consume_event(event, consume_opts) do
      {:ok, _state, report} ->
        Mix.shell().info(Jason.encode!(report, pretty: true))

      {:error, reason} ->
        Mix.raise("daemon news event ingest failed: #{inspect(reason)}")
    end
  end
end
