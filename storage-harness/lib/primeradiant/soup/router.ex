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
    |> respond(fn state, _assigns -> Soup.ready(state, conn.params) end, :ready)
  end

  defp feed(conn) do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> Soup.feed(state, conn.params) end, {:feed, conn.params})
  end

  defp delta(conn) do
    conn
    |> authorize()
    |> respond(fn state, _assigns -> Soup.delta(state, conn.params) end, {:delta, conn.params})
  end

  defp ack(conn) do
    conn
    |> authorize()
    |> respond(
      fn state, assigns ->
        Soup.ack(state, conn.body_params, ack_log_path: assigns.soup_ack_log_path)
      end,
      :ack
    )
  end

  defp respond(%Plug.Conn{halted: true} = conn, _fun, _projection), do: conn

  defp respond(conn, fun, projection) do
    conn.assigns.soup_state
    |> load_state(projection)
    |> fun.(conn.assigns)
    |> then(&send_json(conn, 200, &1))
  end

  defp load_state(%State{} = state, _projection), do: state

  defp load_state({:durable_soup_db, soup_db, tenant}, :ready) do
    DurableSoupDb.load_soup_ready_projection(soup_db, tenant)
  end

  defp load_state({:durable_soup_db, soup_db, tenant}, {:feed, params}) do
    DurableSoupDb.load_soup_feed_projection(soup_db, tenant, params)
  end

  defp load_state({:durable_soup_db, soup_db, tenant}, {:delta, params}) do
    DurableSoupDb.load_soup_feed_projection(soup_db, tenant, params)
  end

  defp load_state({:durable_soup_db, _soup_db, tenant}, :ack) do
    State.new(tenant_id: tenant, user_id: "flynn")
  end

  defp load_state({:durable_soup_db, soup_db, tenant}, :full) do
    DurableSoupDb.load_soup_projection(soup_db, tenant)
  end

  defp send_json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end
end
