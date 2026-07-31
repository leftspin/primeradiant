defmodule Primeradiant.Runtime.Application do
  @moduledoc false

  use Application

  alias Primeradiant.Runtime.Endpoint

  @application :primeradiant_storage_harness
  @supervisor Primeradiant.Runtime.Supervisor

  @impl Application
  def start(_type, _args) do
    children =
      case Application.fetch_env!(@application, :runtime_endpoint) do
        false -> []
        options -> [{Endpoint, options}]
      end

    Supervisor.start_link(children, strategy: :rest_for_one, name: @supervisor)
  end

  def await_shutdown do
    supervisor = Process.whereis(@supervisor)
    monitor = Process.monitor(supervisor)

    receive do
      {:DOWN, ^monitor, :process, ^supervisor, reason} -> exit(reason)
    end
  end
end
