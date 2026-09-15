defmodule Rheo.QueryTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  test "Query struct and keyword query agree" do
    stream = unique_stream("query")
    :ok = Rheo.create_stream(stream)

    {:ok, _} =
      Rheo.append(stream, %{type: "curve_update", currency: "EUR", curve: "A", price: 1})

    {:ok, _} =
      Rheo.append(stream, %{type: "curve_update", currency: "USD", curve: "B", price: 2})

    q = Rheo.Query.new(stream, where: [type: "curve_update", currency: "EUR"], limit: 10)
    assert {:ok, [eur]} = Rheo.query(q)
    assert eur.payload["currency"] == "EUR"

    assert {:ok, [same]} = Rheo.query(stream, type: "curve_update", currency: "EUR")
    assert same.id == eur.id

    desc = Rheo.Query.new(stream, order_by: [sequence: :desc], limit: 1)
    assert {:ok, [last]} = Rheo.query(desc)
    assert last.payload["currency"] == "USD"
  end
end
