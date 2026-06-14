defmodule Primeradiant.StorySubstrate.Router do
  @moduledoc false

  use Plug.Router

  alias Primeradiant.StorageHarness.{DurableSoupDb, State}
  alias Primeradiant.StorySubstrate

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["application/json"], json_decoder: Jason)
  plug(:dispatch)

  get "/api/v1/story-substrate/ready" do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> StorySubstrate.ready(state, conn.params) end)
  end

  get "/api/v1/story-substrate/feed" do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> StorySubstrate.feed(state, conn.params) end)
  end

  get "/api/v1/story-substrate/delta" do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> StorySubstrate.delta(state, conn.params) end)
  end

  post "/api/v1/story-substrate/ack" do
    conn
    |> authorize()
    |> respond(fn state, assigns ->
      StorySubstrate.ack(state, conn.body_params,
        ack_log_path: assigns.story_substrate_ack_log_path
      )
    end)
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  def init(opts), do: opts

  def call(conn, opts) do
    conn
    |> assign(:story_substrate_state, Keyword.fetch!(opts, :state))
    |> assign(:story_substrate_token, Keyword.fetch!(opts, :token))
    |> assign(:story_substrate_ack_log_path, Keyword.fetch!(opts, :ack_log_path))
    |> super(opts)
  end

  defp authorize(%Plug.Conn{assigns: %{story_substrate_token: token}} = conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> ^token] -> conn
      _ -> conn |> send_json(401, %{error: "unauthorized"}) |> halt()
    end
  end

  defp respond(%Plug.Conn{halted: true} = conn, _fun), do: conn

  defp respond(conn, fun) do
    conn.assigns.story_substrate_state
    |> load_state()
    |> fun.(conn.assigns)
    |> then(&send_json(conn, 200, &1))
  end

  defp load_state(%State{} = state), do: state

  defp load_state({:durable_soup_db, soup_db, tenant}) do
    DurableSoupDb.load_tenant(soup_db, tenant)
  end

  defp send_json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end
end
