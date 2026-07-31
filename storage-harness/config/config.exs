import Config

config :primeradiant_storage_harness,
  ecto_repos: [Primeradiant.StorageHarness.Repo],
  runtime_mode: :off,
  runtime_endpoint: false

config :primeradiant_storage_harness, Primeradiant.StorageHarness.Repo,
  adapter: Ecto.Adapters.Postgres,
  database: System.get_env("PRIMERADIANT_STORAGE_DB", "primeradiant_storage_harness_dev"),
  hostname: System.get_env("PRIMERADIANT_STORAGE_HOST", "localhost"),
  username: System.get_env("PRIMERADIANT_STORAGE_USER", System.get_env("USER", "postgres")),
  password: System.get_env("PRIMERADIANT_STORAGE_PASSWORD", ""),
  pool_size: 5
