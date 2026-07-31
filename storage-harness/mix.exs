defmodule PrimeradiantStorageHarness.MixProject do
  use Mix.Project

  def project do
    [
      app: :primeradiant_storage_harness,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Primeradiant.Runtime.Application, []},
      extra_applications: [:logger, :crypto]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.13"},
      {:ecto_sql, "~> 3.13"},
      {:jason, "~> 1.4"},
      {:plug, "~> 1.17"},
      {:plug_cowboy, "~> 2.7"},
      {:postgrex, ">= 0.0.0"},
      {:primeradiant_proof_harness, path: "../proof-harness"}
    ]
  end
end
