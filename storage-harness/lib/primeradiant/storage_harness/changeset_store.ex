defmodule Primeradiant.StorageHarness.ChangesetStore do
  @moduledoc false

  def insert!(module, attrs) do
    attrs = stamp(attrs)

    module
    |> apply(:changeset, [struct(module), attrs])
    |> case do
      %{valid?: true} = changeset ->
        Ecto.Changeset.apply_changes(changeset)

      changeset ->
        raise ArgumentError, "invalid #{inspect(module)} changeset: #{inspect(changeset.errors)}"
    end
  end

  def validate!(module, attrs) do
    module
    |> apply(:changeset, [struct(module), attrs])
    |> case do
      %{valid?: true} = changeset ->
        Ecto.Changeset.apply_changes(changeset)

      changeset ->
        raise ArgumentError, "invalid #{inspect(module)} changeset: #{inspect(changeset.errors)}"
    end
  end

  def update!(struct, attrs) do
    attrs = stamp(attrs)

    struct.__struct__
    |> apply(:changeset, [struct, attrs])
    |> case do
      %{valid?: true} = changeset ->
        Ecto.Changeset.apply_changes(changeset)

      changeset ->
        raise ArgumentError,
              "invalid #{inspect(struct.__struct__)} changeset: #{inspect(changeset.errors)}"
    end
  end

  def evidence_maps(refs) do
    Enum.map(refs, fn
      %{} = ref -> ref
      ref -> %{"input_ref" => ref}
    end)
  end

  def decimal(value) when is_float(value), do: Decimal.from_float(value)
  def decimal(%Decimal{} = value), do: value

  def iso!(value) when is_binary(value) do
    value
    |> DateTime.from_iso8601()
    |> elem(1)
    |> DateTime.truncate(:microsecond)
  end

  def iso!(%DateTime{} = value), do: DateTime.truncate(value, :microsecond)

  def hash(body) do
    :sha256 |> :crypto.hash(body) |> Base.encode16(case: :lower)
  end

  defp stamp(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    attrs
    |> Map.put_new(:inserted_at, now)
    |> Map.put_new(:updated_at, now)
  end
end
