defmodule Primeradiant.Runtime.ApplicationTest do
  use ExUnit.Case, async: false

  alias Primeradiant.Runtime.Endpoint

  @application :primeradiant_storage_harness
  @supervisor Primeradiant.Runtime.Supervisor

  test "boots a rest-for-one runtime shell with resident behavior off" do
    assert Application.fetch_env!(@application, :runtime_mode) == :off
    assert Application.fetch_env!(@application, :runtime_endpoint) == false

    assert Process.alive?(Process.whereis(@supervisor))
    assert Supervisor.which_children(@supervisor) == []
    assert @supervisor |> :sys.get_state() |> elem(2) == :rest_for_one
  end

  test "endpoint is a supervised Cowboy child" do
    endpoint =
      start_supervised!(
        {Endpoint,
         router_options: [
           state: %Primeradiant.StorageHarness.State{},
           token: "runtime-test-token",
           ack_log_path: Path.join(System.tmp_dir!(), "primeradiant-runtime-test-ack.jsonl")
         ],
         transport_options: [ref: :primeradiant_runtime_test, ip: {127, 0, 0, 1}, port: 0]}
      )

    assert Process.alive?(endpoint)
  end
end
