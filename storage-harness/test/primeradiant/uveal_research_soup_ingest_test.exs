defmodule Primeradiant.UvealResearchSoupIngestTest do
  use ExUnit.Case, async: false

  alias Primeradiant.StorageHarness.{DurableSoupDb, UvealResearchSoupIngest}

  @tenant "00000000-0000-0000-0000-00000000t1308"

  test "T1308 admits uveal cancer research items as soup-native inputs" do
    meta_path = write_meta!(tmp_path("vr-ai-news-morning.meta.json"))
    soup_db = tmp_path("uveal-soup.sqlite3")

    {:ok, state, report} =
      UvealResearchSoupIngest.ingest_meta(meta_path,
        tenant_id: @tenant,
        actor_id: "flynn",
        soup_db_path: soup_db
      )

    assert length(state.inputs) == 2
    assert DurableSoupDb.table_count(soup_db, "inputs", @tenant) == 2
    assert report.source.kind == "uveal_research_morning_meta"
    assert report.source.source_category == "cancer"
    assert report.primeradiant_writes.owned_state_only
    assert report.primeradiant_writes.substrate_proof_only
    assert report.ingestion.source_behavior == :evidence_admission_only
    assert report.ingestion.stories == 0
    assert report.ingestion.proposals == 0

    assert Enum.map(state.inputs, & &1.title) == [
             "NCCN guidelines updated: Hepzato Kit recommended for hepatic-dominant UM",
             "ASCO 2026: Darovasertib raises bar in metastatic UM"
           ]

    assert Enum.all?(state.inputs, &is_nil(&1.fixture_id))
    assert Enum.all?(state.inputs, &(get_in(&1.normalized, ["admission_status"]) == "admitted"))

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["source_mode"]) == "manual_real_ingest_v1")
           )

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["meaning_proof"]) == "not_ingest_owned")
           )

    assert Enum.all?(
             state.inputs,
             &(get_in(&1.normalized, ["metadata", "uveal_research_soup_ingest"]) == true)
           )
  end

  test "T1308 refuses morning metadata without uveal cancer research items" do
    meta_path = tmp_path("no-cancer.meta.json")

    File.write!(
      meta_path,
      Jason.encode!(%{
        "date" => "2026-06-14",
        "title" => "Morning Brief",
        "items" => [
          %{
            "category" => "ai",
            "headline" => "AI item",
            "summary" => "AI summary",
            "sources" => []
          }
        ]
      })
    )

    assert {:error, {:no_uveal_research_items, ^meta_path}} =
             UvealResearchSoupIngest.ingest_meta(meta_path, tenant_id: @tenant)
  end

  test "T1308 external ids do not depend on unrelated metadata item order" do
    meta_path = write_meta!(tmp_path("stable-source-ids.meta.json"))
    meta = meta_path |> File.read!() |> Jason.decode!()
    shifted_meta = Map.update!(meta, "items", fn items -> [hd(items) | items] end)

    {:ok, original_items} = UvealResearchSoupIngest.meta_to_items(meta, meta_path, @tenant)
    {:ok, shifted_items} = UvealResearchSoupIngest.meta_to_items(shifted_meta, meta_path, @tenant)

    original_ids = cancer_ids(original_items)
    shifted_ids = cancer_ids(shifted_items)

    assert original_ids == shifted_ids
  end

  defp write_meta!(path) do
    File.write!(
      path,
      Jason.encode!(%{
        "date" => "2026-06-14",
        "title" => "Morning Brief - VR - AI - Apple - Cancer Research",
        "url" =>
          "http://tars.tail4105e8.ts.net:18800/www/news/2026-06-14-vr-ai-news-morning.html",
        "items" => [
          %{
            "category" => "ai",
            "headline" => "AI item is ignored",
            "summary" => "Not a uveal research item",
            "sources" => ["Example"]
          },
          %{
            "category" => "cancer",
            "headline" =>
              "NCCN guidelines updated: Hepzato Kit recommended for hepatic-dominant UM",
            "summary" =>
              "Updated guidelines based on phase 3 trial data for melphalan/hepatic delivery system.",
            "sources" => ["OncLive"]
          },
          %{
            "category" => "cancer",
            "headline" => "ASCO 2026: Darovasertib raises bar in metastatic UM",
            "summary" =>
              "IDEAYA Phase 2/3 OptimUM-02 meets PFS endpoint, targeting NDA submission.",
            "sources" => ["Clinical Trials Arena", "Targeted Oncology"]
          }
        ]
      })
    )

    path
  end

  defp tmp_path(name) do
    dir = Path.join(System.tmp_dir!(), "primeradiant-t1308-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Path.join(dir, name)
  end

  defp cancer_ids(items), do: items |> Enum.map(& &1.external_id) |> Enum.sort()
end
