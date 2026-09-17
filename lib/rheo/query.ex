defmodule Rheo.Query do
  @moduledoc """
  Portable query against a Rheo event stream.

  Backends translate this struct into native filters. Prefer `Rheo.query/1`,
  `Rheo.query/2`, `Rheo.query_page/2`, or `Rheo.stream_query/2` rather than
  constructing backend-specific documents (Mongo filters, SQL, …).

  ## Fields

    * `:stream` — stream name (required)
    * `:where` — keyword filters (type, key, partition, payload/metadata fields)
    * `:from` / `:to` — optional `DateTime` bounds on `timestamp`
    * `:after_sequence` — exclusive lower bound on `sequence` (like `Rheo.read/2` `:after`)
    * `:until_sequence` — inclusive upper bound on `sequence`
    * `:order_by` — `[{field, :asc | :desc}]` (default `[sequence: :asc]`);
      `1` / `-1` are accepted and normalized
    * `:limit` — max events (default `100`)
    * `:cursor` — opaque page cursor from `%Rheo.Page{next_cursor}` (sequence-asc pages)

  ## Building queries

  Keyword options at the top level are merged into `:where` (except recognized
  control keys). These are equivalent:

      iex> a = Rheo.Query.new("market-events", type: "curve_update", currency: "EUR")
      iex> b = Rheo.Query.new("market-events", where: [type: "curve_update", currency: "EUR"])
      iex> a.where == b.where
      true

  Explicit `:where` plus flat filters:

      iex> q = Rheo.Query.new("market-events", where: [type: "curve_update"], currency: "USD", limit: 5)
      iex> {q.where[:type], q.where[:currency], q.limit}
      {"curve_update", "USD", 5}

  ## Common filters

  Event fields:

      iex> q = Rheo.Query.new("market-events", type: "curve_update", key: "EUR-EURIBOR-6M", partition: 0)
      iex> {q.where[:type], q.where[:key], q.where[:partition]}
      {"curve_update", "EUR-EURIBOR-6M", 0}

  Payload fields (any atom other than the reserved ones becomes a payload match,
  e.g. `currency`, `curve`, `price`):

      iex> q = Rheo.Query.new("s", currency: "EUR", curve: "EUR-EURIBOR-6M")
      iex> {q.where[:currency], q.where[:curve]}
      {"EUR", "EUR-EURIBOR-6M"}

  Lineage / metadata (see `Rheo.Event.Lineage`):

      iex> q = Rheo.Query.new("s", correlation_id: "trade-42", producer: "pricing-v3", schema: "curve_update")
      iex> {q.where[:correlation_id], q.where[:producer], q.where[:schema]}
      {"trade-42", "pricing-v3", "curve_update"}

  ## Sequence and time ranges

      iex> q = Rheo.Query.new("s", after_sequence: 100, until_sequence: 200, limit: 50)
      iex> {q.after_sequence, q.until_sequence, q.limit}
      {100, 200, 50}

      iex> from = ~U[2026-01-01 00:00:00.000Z]
      iex> to = ~U[2026-01-31 23:59:59.000Z]
      iex> q = Rheo.Query.new("s", from: from, to: to, order_by: [sequence: :desc])
      iex> {q.from, q.to, q.order_by}
      {~U[2026-01-01 00:00:00.000Z], ~U[2026-01-31 23:59:59.000Z], [sequence: :desc]}

  ## Ordering and limits

      iex> Rheo.Query.new("s").order_by
      [sequence: :asc]

      iex> Rheo.Query.new("s", order_by: [timestamp: :desc, sequence: :desc], limit: 10).limit
      10

  ## Pagination cursors

  `Rheo.query_page/2` returns `%Rheo.Page{next_cursor: …}`. Pass that map back as
  `:cursor` (usually with ascending sequence order):

      iex> q = Rheo.Query.new("s", limit: 100, cursor: %{after_sequence: 50})
      iex> q.cursor
      %{after_sequence: 50}

      iex> q = Rheo.Query.apply_cursor(Rheo.Query.new("s", after_sequence: 10, cursor: %{after_sequence: 50}))
      iex> {q.after_sequence, q.cursor}
      {50, nil}

  ## Running queries (facade)

  These call the configured backend (not doctested here):

      Rheo.query("market-events", type: "curve_update", currency: "EUR")

      Rheo.query(%Rheo.Query{
        stream: "market-events",
        where: [type: "curve_update", currency: "EUR"],
        after_sequence: 1_000,
        order_by: [sequence: :asc],
        limit: 50
      })

      {:ok, page} = Rheo.query_page("market-events", type: "curve_update", limit: 100)
      {:ok, page2} = Rheo.query_page("market-events", type: "curve_update", limit: 100, cursor: page.next_cursor)

      Rheo.stream_query("market-events", type: "curve_update", limit: 100)
      |> Enum.take(250)

  Named instance:

      Rheo.query("market-events", type: "curve_update", rheo: MyRheo)

  ## Non-goals

  Query never ACKs, leases, or deletes events. Consumer progress is
  `Rheo.fetch/3` / replay — see `Rheo.replay/3` and ADR 015.
  """

  @enforce_keys [:stream]
  defstruct stream: nil,
            where: [],
            from: nil,
            to: nil,
            after_sequence: nil,
            until_sequence: nil,
            order_by: [sequence: :asc],
            limit: 100,
            cursor: nil

  @type order_dir :: :asc | :desc
  @type t :: %__MODULE__{
          stream: String.t(),
          where: keyword(),
          from: DateTime.t() | nil,
          to: DateTime.t() | nil,
          after_sequence: non_neg_integer() | nil,
          until_sequence: pos_integer() | nil,
          order_by: [{atom(), order_dir()}],
          limit: pos_integer(),
          cursor: map() | nil
        }

  @doc """
  Builds a query from a stream name and keyword options.

  Recognized options: `:where`, `:from`, `:to`, `:after_sequence`,
  `:until_sequence`, `:order_by`, `:limit`, `:cursor`, plus flat filters
  (`:type`, `:key`, `:currency`, `:correlation_id`, …) merged into `:where`.
  Options `:rheo` and `:sort` are ignored (use `:order_by`; pass `:rheo` to
  `Rheo.query/2` instead).

  ## Examples

      iex> q = Rheo.Query.new("market-events", type: "curve_update", currency: "EUR", limit: 10)
      iex> {q.stream, q.where[:type], q.where[:currency], q.limit}
      {"market-events", "curve_update", "EUR", 10}

      iex> q = Rheo.Query.new("orders", after_sequence: 5, until_sequence: 9, order_by: [sequence: :desc])
      iex> {q.after_sequence, q.until_sequence, q.order_by}
      {5, 9, [sequence: :desc]}

      iex> q = Rheo.Query.new("s", where: [type: "x"], key: "k1", partition: 0)
      iex> q.where
      [type: "x", key: "k1", partition: 0]

      iex> Rheo.Query.new("s", rheo: MyRheo, sort: %{"sequence" => -1}).order_by
      [sequence: :asc]
  """
  @spec new(String.t(), keyword()) :: t()
  def new(stream, opts \\ []) when is_binary(stream) and is_list(opts) do
    {known, rest} =
      Keyword.split(opts, [
        :where,
        :from,
        :to,
        :after_sequence,
        :until_sequence,
        :order_by,
        :limit,
        :cursor,
        :rheo,
        :sort
      ])

    where = Keyword.merge(Keyword.get(known, :where, []), rest)

    %__MODULE__{
      stream: stream,
      where: where,
      from: Keyword.get(known, :from),
      to: Keyword.get(known, :to),
      after_sequence: Keyword.get(known, :after_sequence),
      until_sequence: Keyword.get(known, :until_sequence),
      order_by: normalize_order_by(Keyword.get(known, :order_by, sequence: :asc)),
      limit: Keyword.get(known, :limit, 100),
      cursor: Keyword.get(known, :cursor)
    }
  end

  defp normalize_order_by(order_by) when is_list(order_by) do
    Enum.map(order_by, fn
      {field, dir} when dir in [:asc, 1] -> {field, :asc}
      {field, dir} when dir in [:desc, -1] -> {field, :desc}
    end)
  end

  @doc """
  Applies an opaque page `:cursor` onto `:after_sequence` and clears `:cursor`.

  Used by backends and `Rheo.query_page/2`. Prefer passing `cursor:` into
  `Rheo.Query.new/2` or `Rheo.query_page/2` rather than calling this directly.

  ## Examples

      iex> q = Rheo.Query.new("s", after_sequence: 10, cursor: %{after_sequence: 50})
      iex> applied = Rheo.Query.apply_cursor(q)
      iex> {applied.after_sequence, applied.cursor}
      {50, nil}

      iex> Rheo.Query.apply_cursor(Rheo.Query.new("s")).cursor
      nil
  """
  @spec apply_cursor(t()) :: t()
  def apply_cursor(%__MODULE__{cursor: nil} = query), do: query

  def apply_cursor(%__MODULE__{cursor: cursor} = query) when is_map(cursor) do
    after_seq = cursor[:after_sequence] || cursor["after_sequence"]

    if is_integer(after_seq) do
      current = query.after_sequence || 0
      %{query | after_sequence: max(current, after_seq), cursor: nil}
    else
      %{query | cursor: nil}
    end
  end
end
