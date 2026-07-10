defmodule Primeradiant.Ingestion.Resolution.ResolverPlanner do
  @moduledoc false

  @budget_keys ~w(time_ms requests bytes redirects result_count)

  def plan(registration, evidence_type) when not is_list(evidence_type),
    do: plan(registration, [evidence_type])

  def plan(registration, evidence_types) do
    config = value(registration, :config) || %{}
    budgets = value(registration, :budgets) || %{}
    requested = MapSet.new(Enum.map(evidence_types, &to_string/1))

    config
    |> value(:resolvers, [])
    |> Enum.filter(fn route ->
      route
      |> value(:evidence_types)
      |> Enum.map(&to_string/1)
      |> Enum.any?(&MapSet.member?(requested, &1))
    end)
    |> Enum.map(fn route ->
      id = value(route, :id) |> to_string()

      matched =
        Enum.filter(value(route, :evidence_types, []), &MapSet.member?(requested, to_string(&1)))

      %{
        id: id,
        module: module(value(route, :module)),
        version: value(route, :version),
        evidence_types: Enum.map(matched, &to_string/1),
        why: "registered evidence type: " <> Enum.map_join(matched, ",", &to_string/1),
        budget: resolver_budget(budgets, id)
      }
    end)
  end

  def exhausted?(consumed, budget, result_count) do
    Enum.find_value(@budget_keys, fn key ->
      used = if key == "result_count", do: result_count, else: value(consumed, key, 0)
      limit = value(budget, key)
      if is_number(limit) and used > limit, do: String.to_atom(key)
    end)
  end

  defp resolver_budget(budgets, id) do
    configured = budgets |> value(:resolvers, %{}) |> value(id, %{})

    Map.new(@budget_keys, fn key -> {String.to_atom(key), value(configured, key)} end)
  end

  defp module(module) when is_atom(module), do: module
  defp module("Elixir." <> _ = module), do: String.to_existing_atom(module)
  defp module(module) when is_binary(module), do: Module.concat([module])

  defp value(map, key, default \\ nil)
  defp value(nil, _key, default), do: default

  defp value(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        case Enum.find(map, fn {existing, _value} -> to_string(existing) == to_string(key) end) do
          nil -> default
          {_existing, value} -> value
        end
    end
  end
end
