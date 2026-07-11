defmodule Primeradiant.Ingestion.Resolvers.HttpTransport do
  @moduledoc """
  Single-request HTTP transport seam for bounded evidence resolvers.

  Implementations perform exactly one request and never follow redirects.
  """

  @callback request(
              method :: atom(),
              url :: String.t(),
              headers :: [{String.t(), String.t()}],
              budget_limits :: map()
            ) ::
              {:ok,
               %{
                 required(:status) => non_neg_integer(),
                 required(:headers) => list(),
                 required(:body) => binary()
               }}
              | {:error, term()}

  @callback resolve_addresses(host :: String.t()) ::
              {:ok, [:inet.ip_address()]} | {:error, term()}
end
