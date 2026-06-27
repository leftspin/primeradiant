defmodule Primeradiant.SoupApiTest do
  use ExUnit.Case, async: false
  import Plug.Conn
  import Plug.Test

  alias Primeradiant.Soup
  alias Primeradiant.Soup.Router

  alias Primeradiant.StorageHarness.{
    DurableSoupDb,
    FixtureImporter,
    LiveStoryAgentLoop,
    RealIngestion
  }

  @fixture_path Path.expand("../../../proof-harness/priv/fixtures/primeradiant_golden", __DIR__)
  @ack_log_path Path.join(System.tmp_dir!(), "primeradiant-soup-test-ack.jsonl")
  @opts [token: "internal-token", ack_log_path: @ack_log_path]

  setup_all do
    {:ok, state, _report} = FixtureImporter.import_fixture_corpus(@fixture_path)
    {:ok, state: state}
  end

  test "ready returns ready soup metadata for Reporter story cards", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/ready?consumer=reporter&projection=story_cards")
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

  test "ready accepts Reporter news-morning as the story-card projection", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/ready?consumer=reporter&projection=news-morning")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert body["status"] in ["ready", "degraded"]
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

  test "feed returns explicit incomplete story-card fields without legacy projection framing", %{
    state: state
  } do
    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=story_cards&limit=2")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert length(body["items"]) == 2
    assert body["blockers"] == []

    item = hd(body["items"])
    assert item["story_id"]
    assert is_integer(item["story_version"])
    assert item["change_kind"]
    assert is_list(item["admitted_item_ids"])
    assert is_list(item["key_claims"])
    assert is_list(item["source_coverage"])
    assert item["refresh_reason"]
    assert is_map(item["field_completeness"])
    assert item["projection_provenance"]["story_card_version_id"] == nil
    assert item["projection_provenance"]["projection_id"] == "news-morning"
    assert is_map(item["projection_provenance"]["field_provenance"])
    assert item["section"] == "story_card"
    assert item["status"] == "incomplete"
    assert item["refresh_reason"] == "story_card_not_synthesized"
    assert item["story_card_version_id"] == nil
    assert item["title"]["state"] == "unavailable"
    assert item["deck"]["state"] == "unavailable"
    assert item["summary"]["state"] == "unavailable"
    assert item["provenance"]["reason"] == "story_card_not_synthesized"
    assert item["freshness"]["state"] in ["active", "background", "stale", "resolved"]
    assert item["changed_since_seen"]["state"] == "unavailable"
    assert is_number(item["ranking"]["score"])
    assert is_integer(item["ranking"]["hints"]["source_count"])
    assert item["ranking"]["hints"]["seen_state"]["status"] == "unavailable"

    assert %{
             "summary_text" => _,
             "deck_text" => _,
             "no_summary_reason" => _,
             "no_deck_reason" => _,
             "source_domains" => source_domains,
             "no_source_domains_reason" => _,
             "sources" => sources
           } = item["magazine_contract"]

    assert is_list(source_domains)

    assert [
             %{
               "source_ref" => _,
               "article_ref" => _,
               "article_title" => _,
               "article_name" => _,
               "no_article_title_reason" => _,
               "source_label" => _,
               "no_source_label_reason" => _,
               "publication" => _,
               "no_publication_reason" => _,
               "source_domain" => _,
               "no_source_domain_reason" => _,
               "link_status" => _,
               "no_summary_reason" => _,
               "favicon_url" => _,
               "no_favicon_reason" => _,
               "image_url" => _,
               "no_image_reason" => _,
               "contribution_summary" => _,
               "contribution_type" => _,
               "contribution_link_basis" => _,
               "no_contribution_summary_reason" => _,
               "edge_provenance" => _
             }
             | _
           ] = sources
  end

  test "feed exposes unavailable field reasons when only raw archive refs are committed", %{
    state: state
  } do
    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source = hd(item["magazine_contract"]["sources"])

    assert item["magazine_contract"]["summary_text"] == nil
    assert item["magazine_contract"]["deck_text"] == nil
    assert item["magazine_contract"]["no_summary_reason"] == "story_card_not_synthesized"
    assert item["magazine_contract"]["no_deck_reason"] == "story_card_not_synthesized"

    assert item["magazine_contract"]["source_domains"] == []

    assert item["magazine_contract"]["no_source_domains_reason"] ==
             "no_public_canonical_source_domains_committed"

    assert source["canonical_public_url"] == nil
    assert source["url"] == nil
    assert source["url_kind"] == nil
    assert source["link_status"] == "unavailable"

    assert source["unavailable_reason"] in [
             "only_raw_archive_reference_available",
             "no_source_url_committed"
           ]

    assert source["source_domain"] == nil

    assert source["no_source_domain_reason"] in [
             "no_canonical_uri_committed",
             "canonical_uri_is_not_public_http_url"
           ]

    assert source["source_label"] == nil
    assert source["no_source_label_reason"] == "no_source_label_committed"
    assert source["publication"] == nil
    assert source["no_publication_reason"] == "no_publication_committed"
    assert source["favicon_url"] == nil
    assert source["no_favicon_reason"] == "no_favicon_metadata_committed"
    assert source["image_url"] == nil
    assert source["no_image_reason"] == "no_image_metadata_committed"
  end

  test "feed exposes public source URL fields without raw archive links when canonical URLs exist",
       %{
         state: _state
       } do
    state =
      source_ready_state([
        source_item("story-one",
          canonical_uri: "https://example.test/news/story",
          source_name: "Example Daily"
        )
      ])

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source =
      Enum.find(item["magazine_contract"]["sources"], fn source ->
        source["canonical_public_url"] == "https://example.test/news/story"
      end)

    assert source["canonical_public_url"] =~ ~r/^https?:\/\//
    assert source["url"] == source["canonical_public_url"]
    assert source["url_kind"] == "canonical_public_url"
    assert source["link_status"] == "public"
    assert source["source_domain"] in item["magazine_contract"]["source_domains"]
    assert source["source_domain"] == "example.test"
    refute source["url"] =~ "#offset="
  end

  test "feed exposes article title, source label, media metadata, and contribution summaries",
       %{state: _state} do
    state =
      source_ready_state([
        source_item("story-one",
          title: "Reporter-ready source article",
          canonical_uri: "https://example.test/news/story",
          source_name: "Example Daily",
          metadata: %{
            "favicon_url" => "https://example.test/favicon.ico",
            "image_url" => "https://example.test/images/story.jpg"
          }
        )
      ])

    [input] = state.inputs

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source =
      Enum.find(item["magazine_contract"]["sources"], fn source ->
        source["canonical_public_url"] == "https://example.test/news/story"
      end)

    assert source["source_ref"] == "#{input.source_type}:#{input.external_id}"
    assert source["article_ref"] == input.id
    assert source["article_external_id"] == input.external_id
    assert source["article_title"] == "Reporter-ready source article"
    assert source["article_name"] == "Reporter-ready source article"
    assert source["source_label"] == "Example Daily"
    assert source["publication"] == "Example Daily"
    assert source["favicon_url"] == "https://example.test/favicon.ico"
    assert source["image_url"] == "https://example.test/images/story.jpg"
    assert is_binary(source["contribution_summary"])
    assert source["contribution_summary"] =~ source["contribution_type"]
    assert is_binary(source["contribution_link_basis"])
    assert source["no_contribution_summary_reason"] == nil
    assert source["edge_provenance"]["edge_id"]
    assert source["edge_provenance"]["agent_prompt_version"]
  end

  test "feed preserves separate same-domain article source rows", %{state: _state} do
    state =
      source_ready_state([
        source_item("story-one",
          canonical_uri: "https://same.example.test/news/one",
          source_name: "Same Example"
        ),
        source_item("story-two",
          canonical_uri: "https://same.example.test/news/two",
          source_name: "Same Example",
          observed_at: "2026-05-17T10:05:00Z"
        )
      ])

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    same_domain_sources =
      Enum.filter(item["magazine_contract"]["sources"], fn source ->
        source["source_domain"] == "same.example.test"
      end)

    assert length(same_domain_sources) == 2

    assert Enum.map(same_domain_sources, & &1["canonical_public_url"]) |> Enum.sort() == [
             "https://same.example.test/news/one",
             "https://same.example.test/news/two"
           ]

    assert item["magazine_contract"]["source_domains"] == ["same.example.test"]
  end

  test "feed normalizes source domains from canonical URLs", %{state: _state} do
    state =
      source_ready_state([
        source_item("story-one", canonical_uri: "https://Example.TEST/news/story")
      ])

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source =
      Enum.find(item["magazine_contract"]["sources"], fn source ->
        source["canonical_public_url"] == "https://Example.TEST/news/story"
      end)

    assert source["source_domain"] == "example.test"
    assert "example.test" in item["magazine_contract"]["source_domains"]
  end

  test "feed does not promote source-envelope fragments as public article URLs", %{
    state: state
  } do
    [input | rest] = state.inputs

    input = %{
      input
      | normalized:
          Map.put(
            input.normalized,
            "canonical_uri",
            "https://example.test/raw/source-envelope.json#offset=10&length=20"
          )
    }

    state = %{state | inputs: [input | rest]}

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source =
      Enum.find(item["magazine_contract"]["sources"], fn source ->
        source["unavailable_reason"] == "canonical_uri_is_raw_archive_reference"
      end)

    assert source["canonical_public_url"] == nil
    assert source["url"] == nil
    assert source["link_status"] == "unavailable"
    assert source["no_source_domain_reason"] == "canonical_uri_is_raw_archive_reference"
  end

  test "feed does not promote private provenance or error URLs as public article URLs", %{
    state: state
  } do
    [input | rest] = state.inputs

    input = %{
      input
      | normalized:
          Map.put(
            input.normalized,
            "canonical_uri",
            "https://example.test/provenance/story-source#error"
          )
    }

    state = %{state | inputs: [input | rest]}

    item =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()
      |> get_in(["items"])
      |> hd()

    source =
      Enum.find(item["magazine_contract"]["sources"], fn source ->
        source["unavailable_reason"] == "canonical_uri_is_raw_archive_reference"
      end)

    assert source["canonical_public_url"] == nil
    assert source["url"] == nil
    assert source["link_status"] == "unavailable"
  end

  test "feed serves Reporter news-morning from Prime Radiant story cards", %{state: state} do
    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=2")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert length(body["items"]) == 2
    assert body["blockers"] == []
    assert hd(body["items"])["section"] == "story_card"
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
      |> conn("/api/v1/soup/delta?consumer=reporter&projection=story_cards&after=unknown")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "cursor_unknown"
    assert body["gap"]["recovery"] == "full_feed_required"
  end

  test "delta returns explicit gap for expired cursor", %{state: state} do
    expired_cursor = Soup.cursor_for(state, -1)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=story_cards&after=#{expired_cursor}"
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
      %{"v" => 1, "epoch" => Soup.epoch(state), "event_index" => "0"}
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=story_cards&after=#{malformed_cursor}"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "cursor_unknown"
    assert body["gap"]["requested_cursor"] == malformed_cursor
  end

  test "delta returns story-card changes after an opaque PR cursor", %{state: state} do
    after_cursor = Soup.cursor_for(state, 0)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=story_cards&after=#{after_cursor}&limit=3"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["gap"] == nil
    assert body["items"] == []
    assert is_binary(body["next_cursor"])
  end

  test "delta accepts Reporter news-morning as the story-card projection", %{state: state} do
    after_cursor = Soup.cursor_for(state, 0)

    body =
      :get
      |> conn(
        "/api/v1/soup/delta?consumer=reporter&projection=news-morning&after=#{after_cursor}&limit=3"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["gap"] == nil
    assert body["items"] == []
    assert is_binary(body["next_cursor"])
  end

  test "delta does not return material for blocked readiness params", %{state: state} do
    after_cursor = Soup.cursor_for(state, 0)

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
        "/api/v1/soup/delta?consumer=reporter&projection=story_cards&after=#{rotated_cursor}"
      )
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, state))
      |> json()

    assert body["items"] == []
    assert body["gap"]["code"] == "epoch_rotated"
    assert body["gap"]["requested_cursor"] == rotated_cursor
  end

  test "ack records consumption semantics without mutating story truth", %{state: state} do
    cursor = Soup.cursor_for(state)
    story_count = length(state.stories)

    ack_log_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-ack-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.jsonl"
      )

    body =
      :post
      |> conn(
        "/api/v1/soup/ack",
        Jason.encode!(%{
          consumer: "reporter",
          projection: "story_cards",
          substrate_cursor: cursor,
          substrate_epoch: Soup.epoch(state),
          rendered_at: "2026-06-14T12:00:00Z",
          projection_id: "story_cards-2026-06-14",
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
    assert Jason.decode!(ack_record)["projection_id"] == "story_cards-2026-06-14"
  end

  test "feed can read from durable soup DB runtime source", %{state: state} do
    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "soup_api_test",
      source_db_path: "fixture_importer",
      source_row_count: length(state.inputs)
    })

    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=story_cards&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, {:durable_soup_db, db_path, state.tenant_id}))
      |> json()

    assert [_item] = body["items"]
    loaded_state = DurableSoupDb.load_tenant(db_path, state.tenant_id)
    assert length(loaded_state.stories) == length(state.stories)
  end

  test "durable soup API runtime source avoids proposal op payload hydration", %{state: state} do
    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-heavy-proposal-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "soup_api_test",
      source_db_path: "fixture_importer",
      source_row_count: length(state.inputs)
    })

    loaded_projection = DurableSoupDb.load_soup_projection(db_path, state.tenant_id)
    loaded_full = DurableSoupDb.load_tenant(db_path, state.tenant_id)

    assert loaded_projection.proposal_ops == []
    assert length(loaded_full.proposal_ops) == length(state.proposal_ops)

    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, {:durable_soup_db, db_path, state.tenant_id}))
      |> json()

    assert [_item] = body["items"]
  end

  test "durable ready API avoids reader delta hydration", %{state: _state} do
    state =
      source_ready_state([
        source_item("ready-reader-delta-1", title: "Ready should not load reader deltas")
      ])

    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-ready-lightweight-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "soup_api_test",
      source_db_path: "real_ingestion",
      source_row_count: length(state.inputs)
    })

    projection_state = DurableSoupDb.load_soup_ready_projection(db_path, state.tenant_id)
    full_state = DurableSoupDb.load_soup_projection(db_path, state.tenant_id)

    assert projection_state.story_reader_deltas == []
    assert length(full_state.story_reader_deltas) == length(state.story_reader_deltas)
    assert projection_state.inputs != []
    assert projection_state.stories != []
    assert projection_state.story_events != []
    assert projection_state.story_card_change_sets != []

    body =
      :get
      |> conn("/api/v1/soup/ready?consumer=reporter&projection=news-morning")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, {:durable_soup_db, db_path, state.tenant_id}))
      |> json()

    assert body["contract_version"] == "soup.v1"
    assert body["blockers"] == []
    assert body["freshness"]["latest_source_at"]
    assert body["freshness"]["latest_story_event_at"]
    assert is_binary(body["substrate_cursor"])
  end

  test "durable feed API bounds source hydration for limited Reporter feed", %{state: _state} do
    state =
      source_ready_state([
        source_item("feed-bounded-1", title: "Bounded feed source one"),
        source_item("feed-bounded-2", title: "Bounded feed source two")
      ])

    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-feed-bounded-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "soup_api_test",
      source_db_path: "real_ingestion",
      source_row_count: length(state.inputs)
    })

    inflate_unrelated_feed_rows!(db_path, state.tenant_id, 200)

    projection_state =
      DurableSoupDb.load_soup_feed_projection(db_path, state.tenant_id, %{
        "consumer" => "reporter",
        "projection" => "news-morning",
        "limit" => "1"
      })

    assert length(projection_state.stories) == 1
    assert projection_state.story_reader_deltas == []

    assert length(projection_state.inputs) <
             DurableSoupDb.table_count(db_path, "inputs", state.tenant_id)

    assert length(projection_state.stories) <
             DurableSoupDb.table_count(db_path, "stories", state.tenant_id)

    assert length(projection_state.story_card_versions) <
             DurableSoupDb.table_count(db_path, "story_card_versions", state.tenant_id)

    body =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=news-morning&limit=1")
      |> put_req_header("authorization", "Bearer internal-token")
      |> Router.call(Keyword.put(@opts, :state, {:durable_soup_db, db_path, state.tenant_id}))
      |> json()

    assert [item] = body["items"]
    assert item["story_card_version_id"]
    assert [_source | _] = item["magazine_contract"]["sources"]
    assert item["changed_since_seen"]["state"] == "unavailable"
  end

  test "durable soup API projection loader preserves story-card feed shape", %{state: _state} do
    state =
      source_ready_state([
        source_item("projection-parity-1", title: "Projection parity article")
      ])

    db_path =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-soup-projection-parity-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}.sqlite3"
      )

    DurableSoupDb.persist!(db_path, state, %{
      source_kind: "soup_api_test",
      source_db_path: "real_ingestion",
      source_row_count: length(state.inputs)
    })

    full_state = DurableSoupDb.load_tenant(db_path, state.tenant_id)
    projection_state = DurableSoupDb.load_soup_projection(db_path, state.tenant_id)
    params = %{"consumer" => "reporter", "projection" => "news-morning", "limit" => "1"}

    assert Soup.feed(projection_state, params) == Soup.feed(full_state, params)

    assert Soup.delta(projection_state, Map.put(params, "after", Soup.cursor_for(full_state))) ==
             Soup.delta(full_state, Map.put(params, "after", Soup.cursor_for(full_state)))

    [item] = Soup.feed(projection_state, params).items

    assert item.story_card_version_id
    assert [_claim] = item.key_claims
    assert [_coverage] = item.source_coverage
    assert [_link] = item.source_links
    assert is_map(item.provenance)
    assert is_map(item.projection_provenance)
    assert item.change_set
    assert item.changed_since_seen.state == "unavailable"
    assert item.magazine_contract.summary_text
    assert [_source] = item.magazine_contract.sources
  end

  test "internal service auth rejects missing bearer token", %{state: state} do
    conn =
      :get
      |> conn("/api/v1/soup/feed?consumer=reporter&projection=story_cards")
      |> Router.call(Keyword.put(@opts, :state, state))

    assert conn.status == 401
    assert json(conn)["error"] == "unauthorized"
  end

  test "feed hides missing-agent source coverage from reader-facing source links" do
    state =
      source_ready_state([
        source_item("missing-agent-source-link", title: "Missing salience source")
      ])

    state =
      update_in(state.story_source_coverage, fn rows ->
        Enum.map(rows, fn row ->
          %{
            row
            | contribution_reason: %{
                "state" => "unavailable",
                "reason" => "story_synthesis_agent_did_not_supply_field",
                "text" => nil
              }
          }
        end)
      end)

    [item] = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items
    [_coverage] = item.source_coverage
    assert item.source_links == []
  end

  test "feed keeps explicit non-missing unavailable source reason in reader-facing source links" do
    state =
      source_ready_state([
        source_item("unavailable-source-link", title: "Unavailable salience source")
      ])

    state =
      update_in(state.story_source_coverage, fn rows ->
        Enum.map(rows, fn row ->
          %{
            row
            | contribution_reason: %{
                "state" => "unavailable",
                "reason" => "source_text_withheld",
                "text" => nil
              }
          }
        end)
      end)

    [item] = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items
    [source_link] = item.source_links
    assert source_link.contribution_reason["reason"] == "source_text_withheld"
  end

  test "feed hides internal source coverage validation failures from source links" do
    state =
      source_ready_state([
        source_item("validation-failed-source-link", title: "Validation failed source")
      ])

    state =
      update_in(state.story_source_coverage, fn rows ->
        Enum.map(rows, fn row ->
          %{
            row
            | contribution_reason: %{
                "state" => "refused",
                "reason" => "story_synthesis_agent_omitted_required_source_coverage_after_retry",
                "text" => nil
              }
          }
        end)
      end)

    [item] = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items
    [coverage] = item.source_coverage

    assert coverage.contribution_reason["reason"] ==
             "story_synthesis_agent_omitted_required_source_coverage_after_retry"

    assert item.source_links == []
  end

  test "feed does not project invalidated graph edges as active source contribution truth" do
    state =
      source_ready_state([
        source_item("invalidated-graph-edge", title: "Invalidated graph edge")
      ])

    state =
      update_in(state.edges, fn edges ->
        Enum.map(edges, &%{&1 | status: "quarantined"})
      end)

    [item] = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items
    [source] = item.magazine_contract.sources

    assert source.contribution_summary == nil
    assert source.edge_provenance == nil
    assert source.no_contribution_summary_reason == "no_article_story_contribution_committed"
  end

  test "feed projects refused story-card versions as incomplete provenance" do
    state =
      source_ready_state([
        source_item("refused-card-product-truth", title: "Refused card product truth")
      ])

    state =
      update_in(state.story_card_versions, fn cards ->
        Enum.map(cards, &%{&1 | status: "refused"})
      end)

    [item] = Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items

    assert item.status == "incomplete"
    assert item.refresh_reason == "story_card_not_complete"
    assert item.provenance["reason"] == "story_card_not_complete"
    assert item.provenance["suppressed_story_card_status"] == "refused"
    assert item.key_claims == []
    assert item.source_coverage == []
    assert item.source_links == []

    tmp =
      Path.join(
        System.tmp_dir!(),
        "primeradiant-refused-card-product-truth-#{System.unique_integer([:positive])}.sqlite3"
      )

    on_exit(fn -> File.rm_rf!(tmp) end)

    DurableSoupDb.persist!(tmp, state, %{
      source_kind: "soup-api-test",
      source_db_path: tmp,
      source_row_count: 0
    })

    loaded = DurableSoupDb.load_tenant(tmp, state.tenant_id)

    [loaded_item] =
      Soup.feed(loaded, %{"consumer" => "reporter", "projection" => "news-morning"}).items

    assert loaded_item.status == "incomplete"
    assert loaded_item.key_claims == []
    assert loaded_item.source_coverage == []
    assert loaded_item.source_links == []
  end

  test "feed ranks usable complete story cards above newer refused story-card product truth" do
    state =
      source_ready_state([
        source_item("complete-card-product-truth", title: "Complete card product truth")
      ])

    [complete_story] = state.stories
    [complete_card] = state.story_card_versions
    newer_at = DateTime.add(complete_story.updated_at_story, 7 * 86_400, :second)

    refused_story = %{
      complete_story
      | id: "00000000-0000-4000-8000-000000000421",
        story_key: "refused-card-product-truth",
        title: "Refused card product truth",
        version: complete_story.version + 1_000,
        updated_at_story: newer_at,
        last_material_at: newer_at
    }

    refused_card = %{
      complete_card
      | id: "00000000-0000-4000-8000-000000000422",
        story_id: refused_story.id,
        story_version: refused_story.version,
        card_version: 1,
        status: "refused",
        inserted_at: newer_at,
        updated_at: newer_at
    }

    state = %{
      state
      | stories: [refused_story | state.stories],
        story_card_versions: [refused_card | state.story_card_versions]
    }

    [top_item | _] =
      Soup.feed(state, %{"consumer" => "reporter", "projection" => "news-morning"}).items

    assert top_item.story_id == complete_story.id
    assert top_item.status == "complete"
    assert top_item.ranking.score < refused_story.version
  end

  defp source_ready_state(items) do
    {:ok, state, report} = RealIngestion.ingest_items(items)

    {state, _story_report} =
      LiveStoryAgentLoop.run(state, report.admissions, "flynn", adapter: &stub_story_agent/3)

    state
  end

  defp source_item(external_id, overrides) do
    %{
      tenant_id: "tenant-t1311-soup-api",
      ingestion_run_key: "run-t1311-soup-api",
      source_type: "news_article",
      source_mode: "manual_real_ingest_v1",
      external_id: external_id,
      observed_at: Keyword.get(overrides, :observed_at, "2026-05-17T10:00:00Z"),
      retrieved_at: Keyword.get(overrides, :retrieved_at, "2026-05-17T10:01:00Z"),
      occurred_at: nil,
      canonical_uri:
        Keyword.get(overrides, :canonical_uri, "https://example.test/#{external_id}"),
      raw_object_uri: nil,
      source_name: Keyword.get(overrides, :source_name, "Example"),
      source_actor: %{
        kind: "publisher",
        name: Keyword.get(overrides, :source_name, "Example"),
        stable_id: "example"
      },
      title: Keyword.get(overrides, :title, "Reporter-ready source #{external_id}"),
      body_text: Keyword.get(overrides, :body_text, "Harbor Ferry service is halted today."),
      extracted_text: nil,
      metadata: Keyword.get(overrides, :metadata, %{}),
      acl: %{"privacy" => "public"}
    }
  end

  defp stub_story_agent(%{role: :story_identity}, packet, _ctx) do
    %{
      output: %{
        "story_key" => "reporter-ready-source-story",
        "classification" =>
          if(packet.external_id == "story-one", do: "new_story", else: "substantive_update"),
        "confidence" => 0.82,
        "rationale" => "agent selected story identity from bounded packet"
      },
      model: "stub-story-agent",
      model_route: "test://story-identity",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-identity",
      duration_ms: 1
    }
  end

  defp stub_story_agent(%{role: :meaning_update}, packet, _ctx) do
    %{
      output: %{
        "story_key" => packet.story_identity.story_key,
        "operation_family" => "commit_story_meaning",
        "classification" => packet.story_identity.classification,
        "changed_facts" => %{"source" => packet.external_id},
        "confidence" => 0.79,
        "rationale" => "article adds sourced evidence to the reporter-ready story"
      },
      model: "stub-meaning-agent",
      model_route: "test://meaning-update",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-meaning-update",
      duration_ms: 1
    }
  end

  defp stub_story_agent(%{role: :story_synthesis}, packet, _ctx) do
    %{
      output: %{
        "status" => "complete",
        "title" => %{
          "text" => packet.committed_story_state.title,
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "deck" => %{
          "text" => "Reporter-ready deck for #{packet.external_id}",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "summary" => %{
          "text" => "Reporter-ready summary for #{packet.external_id}",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        },
        "key_claims" => [
          %{
            "claim_ref" => "claim:#{packet.external_id}:service-halted",
            "text" => packet.snippet,
            "status" => "current",
            "materiality" => "material",
            "evidence_refs" => packet.evidence_refs,
            "conflict_refs" => [],
            "uncertainty" => %{"state" => "known", "reason" => nil},
            "appears_in_current_card" => true
          }
        ],
        "source_coverage" => [
          %{
            "source_ref" => packet.source_ref,
            "materiality" => "material",
            "source_posture" => %{"state" => "complete", "value" => "reported"},
            "contribution_reason" => %{
              "text" => "This article supplies the packet-grounded evidence for the story.",
              "state" => "complete",
              "provenance_refs" => packet.evidence_refs
            },
            "source_weight" => %{"state" => "complete", "value" => 1.0}
          }
        ],
        "field_completeness" => %{
          "title" => "complete",
          "deck" => "complete",
          "summary" => "complete",
          "key_claims" => "complete",
          "source_coverage" => "complete",
          "topic_salience" => "complete",
          "overall" => "complete"
        },
        "topic_salience" => %{
          "durable_topic_nodes" => %{
            "state" => "complete",
            "topic_refs" => ["topic:reporter-ready"],
            "provenance_refs" => packet.evidence_refs
          },
          "salience_explanation" => %{
            "state" => "complete",
            "text" => "Reporter-ready test story.",
            "provenance_refs" => packet.evidence_refs
          },
          "global_salience" => "test",
          "flynn_priority" => "test"
        },
        "changed_field_keys" => ["deck", "summary", "source_coverage", "key_claims"],
        "change_summary" => %{
          "text" => "Story card synthesized by test stub.",
          "state" => "complete",
          "provenance_refs" => packet.evidence_refs
        }
      },
      model: "stub-story-agent",
      model_route: "test://story-synthesis",
      producer_kind: "test_stub",
      decision_source: "test_stub",
      invocation_transport_id: "stub-story-synthesis",
      duration_ms: 1
    }
  end

  defp inflate_unrelated_feed_rows!(db_path, tenant_id, count) do
    sql = """
    WITH RECURSIVE n(x) AS (
      VALUES(1)
      UNION ALL
      SELECT x + 1 FROM n WHERE x < #{count}
    )
    INSERT INTO inputs (
      id, tenant_id, fixture_id, source_type, external_id, observed_at, title, body_text,
      object_uri, content_sha256, acl, normalized, facts, background, questions, colors,
      topic_tokens, inserted_at, updated_at
    )
    SELECT
      'unrelated-feed-input-' || x,
      tenant_id,
      fixture_id,
      source_type,
      'unrelated-feed-external-' || x,
      observed_at,
      title,
      body_text,
      object_uri,
      'unrelated-feed-sha-' || x,
      acl,
      normalized,
      facts,
      background,
      questions,
      colors,
      topic_tokens,
      inserted_at,
      updated_at
    FROM (SELECT * FROM inputs WHERE tenant_id = #{sql_quote(tenant_id)} LIMIT 1), n;

    WITH RECURSIVE n(x) AS (
      VALUES(1)
      UNION ALL
      SELECT x + 1 FROM n WHERE x < #{count}
    )
    INSERT OR IGNORE INTO stories (
      id, tenant_id, story_key, title, state, version, first_observed_at,
      updated_at_story, last_material_at, structural_facts, background_facts,
      colors, questions, topic_tokens, attrs, inserted_at, updated_at
    )
    SELECT
      'unrelated-feed-story-' || x,
      tenant_id,
      'unrelated-feed-story-key-' || x,
      'Unrelated feed story ' || x,
      state,
      version,
      '2020-01-01T00:00:00Z',
      '2020-01-01T00:00:00Z',
      '2020-01-01T00:00:00Z',
      structural_facts,
      background_facts,
      colors,
      questions,
      topic_tokens,
      attrs,
      inserted_at,
      updated_at
    FROM (SELECT * FROM stories WHERE tenant_id = #{sql_quote(tenant_id)} LIMIT 1), n;

    WITH RECURSIVE n(x) AS (
      VALUES(1)
      UNION ALL
      SELECT x + 1 FROM n WHERE x < #{count}
    )
    INSERT OR IGNORE INTO story_card_versions (
      id, tenant_id, story_id, story_version, card_version, status, supersedes_id,
      refresh_reason, producing_agent_run_id, packet_hash, prompt_config_hash,
      output_hash, field_provenance_manifest_id, title, deck, summary, freshness,
      field_completeness, topic_salience, provenance, inserted_at, updated_at
    )
    SELECT
      'unrelated-feed-card-version-' || x,
      tenant_id,
      'unrelated-feed-story-' || x,
      story_version,
      1,
      'refused',
      NULL,
      refresh_reason,
      producing_agent_run_id,
      'unrelated-feed-packet-hash-' || x,
      prompt_config_hash,
      'unrelated-feed-output-hash-' || x,
      field_provenance_manifest_id,
      title,
      deck,
      summary,
      freshness,
      field_completeness,
      topic_salience,
      provenance,
      inserted_at,
      updated_at
    FROM (SELECT * FROM story_card_versions WHERE tenant_id = #{sql_quote(tenant_id)} LIMIT 1), n;

    WITH RECURSIVE n(x) AS (
      VALUES(1)
      UNION ALL
      SELECT x + 1 FROM n WHERE x < #{count}
    )
    INSERT OR IGNORE INTO story_reader_deltas (
      id, tenant_id, user_id, story_id, seen_state_id, prior_seen_story_version,
      prior_seen_card_version_id, current_story_version, current_card_version_id,
      material_unseen_deltas, nonmaterial_exclusions, producing_agent_run_id,
      evidence_refs, provenance_refs, inserted_at, updated_at
    )
    SELECT
      'unrelated-feed-delta-' || x,
      tenant_id,
      user_id,
      story_id,
      seen_state_id,
      prior_seen_story_version,
      prior_seen_card_version_id,
      current_story_version,
      current_card_version_id,
      material_unseen_deltas,
      nonmaterial_exclusions,
      producing_agent_run_id,
      evidence_refs,
      provenance_refs,
      inserted_at,
      updated_at
    FROM (SELECT * FROM story_reader_deltas WHERE tenant_id = #{sql_quote(tenant_id)} LIMIT 1), n;
    """

    {_, 0} = System.cmd("sqlite3", [db_path, sql], stderr_to_stdout: true)
  end

  defp sql_quote(value), do: "'#{String.replace(to_string(value), "'", "''")}'"

  defp json(conn), do: Jason.decode!(conn.resp_body)
end
