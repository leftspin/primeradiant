defmodule Primeradiant.StorySubstrateTest do
  use ExUnit.Case, async: false
  import Plug.Conn
  import Plug.Test

  alias Primeradiant.StorySubstrate
  alias Primeradiant.StorySubstrate.Router
  alias Primeradiant.StorageHarness.{DurableSoupDb, FixtureImporter}

  @fixture_path Path.expand("../../../proof-harness/priv/fixtures/primeradiant_golden", __DIR__)
  @ack_log_path Path.join(System.tmp_dir!(), "primeradiant-soup-test-ack.jsonl")
  @opts [token: "internal-token", ack_log_path: @ack_log_path]

  setup_all do
    {:ok, state, _report} = FixtureImporter.import_fixture_corpus(@fixture_path)
    {:ok, state: state}
  end

  test "ready returns ready soup metadata for Reporter news morning", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/ready?consumer=reporter&projection=news-morning")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert body["status"] in ["ready", "degraded"]
    assert is_binary(body["substrate_cursor"])
    assert is_binary(body["substrate_epoch"])
    assert body["freshness"]["latest_source_at"]
    assert body["blockers"] == []
  end

  test "ready blocks unsupported projection instead of allowing render", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/ready?consumer=reporter&projection=other")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["status"] == "blocked"
    assert [%{"code" => "unsupported_projection"}] = body["blockers"]
  end

  test "legacy story-substrate route is not exposed", %{state: state} do
    conn =
      :get
      |> conn("/api/v1/story-substrate/ready?consumer=reporter&projection=news-morning")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))

    assert conn.status == 404
    assert json(conn)["error"] == "not_found"
  end

  test "feed returns required story item fields without authored projection framing", %{
    state: state
  } do
    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=2")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert length(body["items"]) == 2
    assert body["blockers"] == []

    item = hd(body["items"])
    assert item["story_id"]
    assert is_list(item["admitted_item_ids"])
    assert %{"text" => _, "source" => _, "evidence_refs" => refs} = item["title"]
    assert is_list(refs)
    assert item["summary"]["source"] == "primeradiant_facts"
    assert item["timestamps"]["last_material_change_at"]
    assert [%{"source_domain" => _, "evidence_refs" => _} | _] = item["provenance"]
    assert item["confidence"]["label"] in ["high", "medium", "low"]
    assert item["freshness"]["state"] in ["active", "background", "stale", "resolved"]
    assert item["change"]["kind"]
    assert is_number(item["ranking"]["score"])
  end

  test "feed does not return material for blocked readiness params", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=other&limit=2")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert [%{"code" => "unsupported_projection"}] = body["blockers"]
  end

  test "delta returns explicit gap for unknown cursor and no silent gap", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=unknown")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "cursor_unknown"
    assert body["gap"]["recovery"] == "full_feed_required"
  end

  test "delta returns explicit gap for expired cursor", %{state: state} do
    expired_cursor = StorySubstrate.cursor_for(state, -1)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=#{expired_cursor}"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "cursor_expired"
    assert body["gap"]["requested_cursor"] == expired_cursor
  end

  test "delta returns explicit gap for malformed decoded cursor", %{state: state} do
    malformed_cursor =
      %{"v" => 1, "epoch" => StorySubstrate.epoch(state), "event_index" => "0"}
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=#{malformed_cursor}"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "cursor_unknown"
    assert body["gap"]["requested_cursor"] == malformed_cursor
  end

  test "delta returns items after an opaque PR cursor", %{state: state} do
    after_cursor = StorySubstrate.cursor_for(state, 0)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=#{after_cursor}&limit=3"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["gap"] == nil
    assert body["items"] != []
    assert is_binary(body["next_cursor"])
  end

  test "delta does not return material for blocked readiness params", %{state: state} do
    after_cursor = StorySubstrate.cursor_for(state, 0)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=other&after=#{after_cursor}&limit=3"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"] == nil
    assert [%{"code" => "unsupported_projection"}] = body["blockers"]
  end

  test "delta returns explicit gap for epoch rotation", %{state: state} do
    rotated_cursor =
      %{"v" => 1, "epoch" => "other-epoch", "event_index" => 0}
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=#{rotated_cursor}"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "epoch_rotated"
    assert body["gap"]["requested_cursor"] == rotated_cursor
  end

  test "ack records consumption semantics without mutating story truth", %{state: state} do
    cursor = StorySubstrate.cursor_for(state)
    story_count = length(state.stories)

    ack_log_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-ack-#{System.unique_integer([:positive])}.jsonl"
      )

    body =
      :post
      |> conn(
        "/api/v1/soup/ack",
        Jason.encode!(%{
          consumer: "reporter",
          projection: "news-morning",
          substrate_cursor: cursor,
          substrate_epoch: StorySubstrate.epoch(state),
          rendered_at: "2026-06-14T12:00:00Z",
          projection_id: "news-morning-2026-06-14",
          status: "rendered",
          reason: nil
        })
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> put_req_header("content-type", "application/json")
      |> Router.call(
        @opts
        |> Keyword.put(:state, state)
        |> Keyword.put(:ack_log_path, ack_log_path)
      )
      |> json()

    assert body["ack"]["status"] == "rendered"
    assert body["ack"]["substrate_cursor"] == cursor
    refute Map.has_key?(body, "recorded")
    assert length(state.stories) == story_count

    [ack_record] = ack_log_path |> File.read!() |> String.split("\n", trim: true)
    assert Jason.decode!(ack_record)["projection_id"] == "news-morning-2026-06-14"
  end

  test "feed can read from durable soup DB runtime source", %{state: state} do
    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "story_substrate_test",
      source_db_path: "fixture_importer",
      source_row_count: length(state.inputs)
    })

    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, {:durable_soup_db, db_path, state.tenant_id}))
      |> json()

    assert [_item] = body["items"]
    loaded_state = DurableSoupDb.load_tenant(db_path, state.tenant_id)
    assert length(loaded_state.stories) == length(state.stories)
  end

  test "internal service auth rejects missing bearer token", %{state: state} do
    conn =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning")
      |> Router.call(Keyword.put(@opts, :state, state))

    assert conn.status == 401
    assert json(conn)["error"] == "unauthorized"
  end

  defp json(conn), do: Jason.decode!(conn.resp_body)
end
