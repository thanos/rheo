defmodule Rheo.Page do
  @moduledoc """
  One page of query results with an optional continuation cursor.

  Returned by `Rheo.query_page/2`. When `next_cursor` is non-nil, pass it as
  `cursor:` on the next `Rheo.Query` (or via `Rheo.query_page/2` opts) to fetch
  the following page. Cursor pagination assumes ascending sequence order.

  `next_cursor` is a composite `%{partition => after_sequence}` map so a page
  boundary never splits a sequence across partitions. The legacy
  `%{after_sequence: n}` shape is still accepted as a global lower bound.
  """

  @enforce_keys [:events]
  defstruct events: [], next_cursor: nil

  @type cursor :: %{optional(non_neg_integer() | atom() | String.t()) => term()}
  @type t :: %__MODULE__{
          events: [Rheo.Event.t()],
          next_cursor: cursor() | nil
        }
end
