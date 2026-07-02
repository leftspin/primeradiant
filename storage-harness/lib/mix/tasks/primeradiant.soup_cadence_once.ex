defmodule Mix.Tasks.Primeradiant.SoupCadenceOnce do
  @moduledoc false

  use Mix.Task

  alias Primeradiant.StorageHarness.{DurableSoupDb, LiveStoryAgentLoop}

  @shortdoc "Run one bounded Prime Radiant recurring soup-agent cadence pass"

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          soup_db: :string,
          tenant: :string,
          actor: :string,
          cadence: :string,
          limit: :integer
        ]
      )

    Mix.Task.run("app.start")

    soup_db = Keyword.fetch!(opts, :soup_db)
    tenant = Keyword.fetch!(opts, :tenant)
    actor = Keyword.get(opts, :actor, "flynn")
    cadence = opts |> Keyword.get(:cadence, "hourly_story_card_synthesis") |> parse_cadence!()
    limit = Keyword.get(opts, :limit, 8)

    loaded = DurableSoupDb.load_soup_ready_projection(soup_db, tenant)

    {state, report} =
      LiveStoryAgentLoop.refresh_story_cards(loaded, actor, cadence: cadence, limit: limit)

    DurableSoupDb.persist_delta!(soup_db, loaded, state, %{
      source_kind: "recurring-soup-cadence",
      source_db_path: soup_db,
      source_row_count: 0,
      expected_tenant_revision: DurableSoupDb.tenant_revision(soup_db, tenant)
    })

    Mix.shell().info(Jason.encode!(report))
  end

  defp parse_cadence!("active_story_transform_detect_link_15m"),
    do: :active_story_transform_detect_link_15m

  defp parse_cadence!("hourly_story_card_synthesis"), do: :hourly_story_card_synthesis
  defp parse_cadence!("daily_deep_soup_sweep"), do: :daily_deep_soup_sweep

  defp parse_cadence!(value) do
    raise ArgumentError,
          "unsupported --cadence #{inspect(value)}; expected active_story_transform_detect_link_15m, hourly_story_card_synthesis, or daily_deep_soup_sweep"
  end
end
