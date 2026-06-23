defmodule Primeradiant.Soup.Router do
  @moduledoc false

  use Plug.Router

  alias Primeradiant.StorageHarness.{DurableSoupDb, State}
  alias Primeradiant.Soup

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["application/json"], json_decoder: Jason)
  plug(:dispatch)

  get("/api/v1/soup/ready", do: ready(conn))
  get("/api/v1/soup/feed", do: feed(conn))
  get("/api/v1/soup/delta", do: delta(conn))
  post("/api/v1/soup/ack", do: ack(conn))

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  def init(opts), do: opts

  def call(conn, opts) do
    conn
    |> assign(:soup_state, Keyword.fetch!(opts, :state))
    |> assign(:soup_token, Keyword.fetch!(opts, :token))
    |> assign(:soup_ack_log_path, Keyword.fetch!(opts, :ack_log_path))
    |> super(opts)
  end

  defp authorize(%Plug.Conn{assigns: %{soup_token: token}} = conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> ^token] -> conn
      _ -> conn |> send_json(401, %{error: "unauthorized"}) |> halt()
    end
  end

  defp ready(conn) do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> Soup.ready(state, conn.params) end)
  end

  defp feed(conn) do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> Soup.feed(state, conn.params) end)
  end

  defp delta(conn) do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> Soup.delta(state, conn.params) end)
  end

  defp ack(conn) do
    conn
    |> authorize()
    |> respond(fn state, assigns ->
      Soup.ack(state, conn.body_params, ack_log_path: assigns.soup_ack_log_path)
    end)
  end

  defp respond(%Plug.Conn{halted: true} = conn, _fun), do: conn

  defp respond(conn, fun) do
    conn.assigns.soup_state
    |> load_state()
    |> fun.(conn.assigns)
    |> then(&send_json(conn, 200, &1))
  end

  defp load_state(%State{} = state), do: state

  defp load_state({:durable_soup_db, soup_db, tenant}) do
    DurableSoupDb.load_soup_projection(soup_db, tenant)
  end

  defp send_json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end
end
