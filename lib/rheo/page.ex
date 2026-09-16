defmodule Rheo.Page do
  @moduledoc """
  One page of query results with an optional continuation cursor.

  Returned by `Rheo.query_page/2`. When `next_cursor` is non-nil, pass it as
  `cursor:` on the next `Rheo.Query` (or via `Rheo.query_page/2` opts) to fetch
  the following page. Cursor pagination assumes ascending sequence order.
  """

  @enforce_keys [:events]
  defstruct events: [], next_cursor: nil

  @type cursor :: %{optional(atom()) => term()}
  @type t :: %__MODULE__{
          events: [Rheo.Event.t()],
          next_cursor: cursor() | nil
        }
end
