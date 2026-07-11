defmodule Primeradiant.Ingestion.SourceDeliveryReader do
  @moduledoc false

  @typedoc "Canonical read-only delivery event descriptor. At least one raw material value is present."
  @type event_descriptor :: %{
          required(:source_key) => String.t(),
          required(:source_event_external_id) => String.t(),
          required(:source_position) => integer(),
          required(:content_digest) => String.t(),
          required(:received_at) => DateTime.t() | String.t(),
          optional(:raw_object_ref) => String.t() | nil,
          optional(:retained_bytes) => String.t() | nil,
          required(:visibility) => String.t(),
          required(:correlation_id) => String.t(),
          optional(:integrity_metadata) => map() | nil
        }

  @callback events_after(cursor_position :: integer(), head :: integer()) :: [event_descriptor()]
  @callback events_at(positions :: [integer()]) :: [event_descriptor()]
end
