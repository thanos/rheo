defmodule Rheo.Page do
  @moduledoc """
  One page of query results with an optional continuation cursor.

  Returned by `Rheo.query_page/2`. When `next_cursor` is non-nil, pass it as
  `cursor:` on the next `Rheo.Query` (or via `Rheo.query_page/2` opts) to fetch
  the following page. Cursor pagination assumes ascending sequence order.

  `next_cursor` is a composite `%{partition => after_sequence}` map so a page
  boundary never splits a sequence across partitions. The legacy
  `%{after_sequence: n}` shape is still accepted as a global lower bound when
  applying a cursor.

  ## Fields

  | Field | Type | Meaning |
  |---|---|---|
  | `events` | `[Rheo.Event.t()]` | Events on this page (may be empty) |
  | `next_cursor` | `cursor() \\| nil` | Pass as `cursor:` for the next page; `nil` means done |

  ## Cursor shapes

      # Composite (preferred): exclusive lower bound per partition
      %{0 => 10, 1 => 4}

      # Legacy global lower bound (still accepted on apply)
      %{after_sequence: 10}

  ## Example

      iex> page = %Rheo.Page{
      ...>   events: [],
      ...>   next_cursor: %{0 => 5, 1 => 2}
      ...> }
      iex> page.next_cursor[0]
      5
  """

  @enforce_keys [:events]
  defstruct events: [], next_cursor: nil

  @typedoc """
  Continuation cursor for `Rheo.query_page/2`.

  Prefer `%{partition => after_sequence}`. Legacy `%{after_sequence: n}` remains
  accepted by `Rheo.Query.apply_cursor/1` as a global lower bound.
  """
  @type cursor :: %{optional(non_neg_integer() | atom() | String.t()) => term()}

  @typedoc """
  One page of events plus an optional `next_cursor`.

  See the module documentation for field meanings and cursor shapes.
  """
  @type t :: %__MODULE__{
          events: [Rheo.Event.t()],
          next_cursor: cursor() | nil
        }
end
