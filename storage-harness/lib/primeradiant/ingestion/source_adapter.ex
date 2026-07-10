defmodule Primeradiant.Ingestion.SourceAdapter do
  @moduledoc """
  Thin source-envelope adapter contract.

  Implementations may decode and verify the preserved envelope, declare its cursor
  and identity, retain raw references, and map supplied fields into the generic
  candidate shape. They must not perform provider lookups or network calls, resolve
  redirects, apply confidence or story policy, inspect downstream concerns, or admit
  material.
  """

  @type typed_reason :: atom() | String.t() | {atom(), term()}
  @type candidate :: %{
          required(:raw_refs) => [term()],
          required(:declared_identity) => term(),
          optional(:declared_cursor) => term(),
          optional(:raw_fields) => map(),
          optional(:feed_metadata) => map(),
          optional(:headers) => map(),
          optional(:visible_links) => list(),
          optional(:source_declared_metadata) => map(),
          required(:visibility) => term(),
          required(:adapter_provenance) => map()
        }

  @callback to_candidate(raw_envelope :: struct() | map(), ctx :: map()) ::
              {:ok, candidate()} | {:refused, typed_reason()}
end
