defmodule Primeradiant.Ingestion.Resolution.EvidenceResolver do
  @moduledoc """
  Contract for a registered, bounded evidence resolver.

  A resolver receives only the packet and the registration-owned budget. It returns
  immutable evidence material and actual budget consumption, or a typed outcome. It
  does not select fields or perform admission.
  """

  @type budget :: %{
          optional(:time_ms) => non_neg_integer(),
          optional(:requests) => non_neg_integer(),
          optional(:bytes) => non_neg_integer(),
          optional(:redirects) => non_neg_integer(),
          optional(:result_count) => non_neg_integer()
        }

  @callback resolve(packet :: map(), budget()) ::
              {:ok, evidence_list :: [map()], consumed_budget :: map()}
              | {:outcome, typed_outcome :: term()}
end
