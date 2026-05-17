defmodule Primeradiant.StorageHarness.Repo do
  use Ecto.Repo,
    otp_app: :primeradiant_storage_harness,
    adapter: Ecto.Adapters.Postgres
end
