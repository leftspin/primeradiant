defmodule Primeradiant.Runtime.Endpoint do
  @moduledoc false

  alias Primeradiant.Soup.Router

  def child_spec(options) do
    router_options = Keyword.fetch!(options, :router_options)
    transport_options = Keyword.fetch!(options, :transport_options)

    [
      scheme: :http,
      plug: {Router, router_options},
      options: transport_options
    ]
    |> Plug.Cowboy.child_spec()
    |> Map.put(:id, __MODULE__)
  end
end
