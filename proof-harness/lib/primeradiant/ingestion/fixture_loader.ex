defmodule Primeradiant.Ingestion.FixtureLoader do
  @moduledoc false

  def load_corpus do
    fixtures_root = fixtures_root()
    manifest = read_json!(Path.join(fixtures_root, "manifest.json"))

    inputs =
      manifest["inputs"]
      |> Enum.map(&read_json!(Path.join([fixtures_root, "inputs", &1])))
      |> Enum.sort_by(& &1["observed_at"])

    watches =
      manifest["watches"]
      |> Enum.map(&read_json!(Path.join([fixtures_root, "watches", &1])))

    %{manifest: manifest, inputs: inputs, watches: watches}
  end

  defp fixtures_root do
    :primeradiant_proof_harness
    |> :code.priv_dir()
    |> to_string()
    |> Path.join("fixtures/primeradiant_golden")
  end

  defp read_json!(path) do
    path |> File.read!() |> Jason.decode!()
  end
end
