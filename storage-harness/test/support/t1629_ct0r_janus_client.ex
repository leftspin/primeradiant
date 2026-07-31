defmodule Primeradiant.T1629CT0R.JanusClient do
  @moduledoc false

  @janus_item_url "https://tars.tail4105e8.ts.net:19443/tracker/item"
  @tars_local_item_url "http://127.0.0.1:18803/tracker/item"
  @allowlisted_ticket_ids ~w(T1654 T1656 T1670)

  def execute(action, opts \\ [])

  def execute({:get_ticket, ticket_id}, opts) when ticket_id in @allowlisted_ticket_ids do
    runner = Keyword.get(opts, :command_runner, &System.cmd/3)
    public_url = item_url(@janus_item_url, ticket_id)
    {body, status} = runner.("curl", ["-kfsS", public_url], stderr_to_stdout: true)

    {body, status} =
      if status == 0 do
        {body, status}
      else
        fallback_url = item_url(@tars_local_item_url, ticket_id)

        runner.(
          "ssh",
          ["tars", "curl -fsS '#{fallback_url}'"],
          stderr_to_stdout: true
        )
      end

    if status == 0 do
      Jason.decode(body)
    else
      {:error, {:janus_rest_read_failed, ticket_id, body}}
    end
  end

  def execute({:get_ticket, ticket_id}, _opts),
    do: {:error, {:ticket_not_allowlisted, ticket_id}}

  def execute(action, _opts), do: {:error, {:prohibited_ct0r_action, action}}

  defp item_url(base, ticket_id),
    do: base <> "?id=" <> URI.encode_www_form(ticket_id)
end
