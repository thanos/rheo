defmodule Rheo.Query do
  @moduledoc """
  Portable query against a Rheo event stream.

  Backends translate this struct into native filters. Prefer `Rheo.query/1` or
  `Rheo.query/2` rather than constructing backend-specific documents.

  ## Fields

    * `:stream` — stream name (required)
    * `:where` — keyword filters (type, key, partition, payload/metadata fields)
    * `:from` / `:to` — optional `DateTime` bounds on `timestamp`
    * `:order_by` — `[{field, :asc | :desc}]` (default `[sequence: :asc]`)
    * `:limit` — max events (default `100`)
  """

  @enforce_keys [:stream]
  defstruct stream: nil,
            where: [],
            from: nil,
            to: nil,
            order_by: [sequence: :asc],
            limit: 100

  @type order_dir :: :asc | :desc
  @type t :: %__MODULE__{
          stream: String.t(),
          where: keyword(),
          from: DateTime.t() | nil,
          to: DateTime.t() | nil,
          order_by: [{atom(), order_dir()}],
          limit: pos_integer()
        }

  @doc """
  Builds a query from a stream name and keyword options.

  Recognized options: `:where`, `:from`, `:to`, `:order_by`, `:limit`, plus
  legacy flat filters (`:type`, `:key`, `:currency`, …) merged into `:where`.
  Options `:rheo` and `:sort` are ignored (use `:order_by`).

  ## Examples

      iex> q = Rheo.Query.new("market-events", type: "curve_update", currency: "EUR", limit: 10)
      iex> {q.stream, q.where[:type], q.where[:currency], q.limit}
      {"market-events", "curve_update", "EUR", 10}

  ## Returns

  `%Rheo.Query{}`
  """
  @spec new(String.t(), keyword()) :: t()
  def new(stream, opts \\ []) when is_binary(stream) and is_list(opts) do
    {known, rest} = Keyword.split(opts, [:where, :from, :to, :order_by, :limit, :rheo, :sort])
    where = Keyword.merge(Keyword.get(known, :where, []), rest)

    %__MODULE__{
      stream: stream,
      where: where,
      from: Keyword.get(known, :from),
      to: Keyword.get(known, :to),
      order_by: Keyword.get(known, :order_by, sequence: :asc),
      limit: Keyword.get(known, :limit, 100)
    }
  end
end
