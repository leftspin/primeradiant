defmodule Mix.Tasks.Primeradiant.StorySubstrateApi do
  @moduledoc false

  use Mix.Task

  alias Primeradiant.StorySubstrate.Router

  @shortdoc "Run the internal story-substrate HTTP JSON API"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          soup_db: :string,
          tenant: :string,
          token: :string,
          ack_log: :string,
          port: :integer
        ]
      )

    Mix.Task.run("app.start")

    soup_db = Keyword.fetch!(opts, :soup_db)
    tenant = Keyword.fetch!(opts, :tenant)
    token = Keyword.fetch!(opts, :token)
    ack_log = Keyword.fetch!(opts, :ack_log)
    port = Keyword.get(opts, :port, 4084)

    {:ok, _pid} =
      Plug.Cowboy.http(
        Router,
        [state: {:durable_soup_db, soup_db, tenant}, token: token, ack_log_path: ack_log],
        ref: :"primeradiant_story_substrate_#{port}",
        ip: {127, 0, 0, 1},
        port: port
      )

    Mix.shell().info("story-substrate API listening on #{port}")
    Process.sleep(:infinity)
  end
end
