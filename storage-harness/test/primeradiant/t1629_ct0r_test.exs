Code.require_file("../support/t1629_ct0r_janus_client.ex", __DIR__)

defmodule Primeradiant.T1629CT0RTest do
  use ExUnit.Case, async: false

  alias Primeradiant.T1629CT0R.JanusClient

  @accepted_boundaries %{
    "T1654" => %{state: "Verifiable", proof_ids: ["PP005777"]},
    "T1656" => %{state: "Verifiable", proof_ids: ["PP005778"]},
    "T1670" => %{state: "In Progress", proof_ids: ["PP005779", "PP006369"]}
  }

  @prohibited_actions [
    :watcher_mutation,
    :runtime_mutation,
    :source_admission_acknowledgement_mutation,
    :cursor_mutation,
    :replay,
    :registration_mutation,
    :source_mutation,
    :soup_mutation,
    :recovery_repair,
    :deploy,
    :quiet_period_classification
  ]

  test "TP-CT0R pins the current accepted Janus proof states through hosted REST" do
    for {ticket_id, expected} <- @accepted_boundaries do
      assert {:ok, ticket} = JanusClient.execute({:get_ticket, ticket_id})

      assert ticket["canonical_state"] == expected.state

      for proof_id <- expected.proof_ids do
        assert accepted_proof?(ticket, proof_id),
               "expected #{ticket_id} to expose accepted proof #{proof_id}"
      end
    end
  end

  test "TP-CT0R rejects every T1629 recovery or live-state action" do
    for action <- @prohibited_actions do
      command_runner = fn command, args, opts ->
        send(self(), {:command_executed, action, command, args, opts})
        {"unexpected command", 1}
      end

      assert JanusClient.execute(action, command_runner: command_runner) ==
               {:error, {:prohibited_ct0r_action, action}}

      refute_received {:command_executed, ^action, _, _, _}
    end
  end

  test "TP-CT0R rejects non-allowlisted ticket reads before command execution" do
    command_runner = fn command, args, opts ->
      send(self(), {:command_executed, command, args, opts})
      {"unexpected command", 1}
    end

    assert JanusClient.execute({:get_ticket, "T1629"}, command_runner: command_runner) ==
             {:error, {:ticket_not_allowlisted, "T1629"}}

    refute_received {:command_executed, _, _, _}
  end

  test "the architecture record carries the pinned boundary and dependent-proof limit" do
    record =
      __DIR__
      |> Path.join("../../../architecture/t1629-ct0r-evidence-reconciliation.md")
      |> Path.expand()
      |> File.read!()

    for token <- [
          "T1654",
          "Verifiable",
          "PP005777",
          "T1656",
          "PP005778",
          "T1670",
          "In Progress",
          "PP005779",
          "PP006369",
          "8c3b7fa",
          "does not claim that T1670 recovery is verified",
          "does not classify it"
        ] do
      assert record =~ token
    end
  end

  defp accepted_proof?(value, proof_id) when is_map(value) do
    (value["id"] == proof_id and value["status"] == "accepted") or
      Enum.any?(value, fn {_key, nested} -> accepted_proof?(nested, proof_id) end)
  end

  defp accepted_proof?(value, proof_id) when is_list(value) do
    Enum.any?(value, &accepted_proof?(&1, proof_id))
  end

  defp accepted_proof?(_value, _proof_id), do: false
end
