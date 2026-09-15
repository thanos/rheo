defmodule Rheo.InstanceTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  test "two named instances share a Mongo DB with isolated handles" do
    url = mongo_url()
    a = String.to_atom("rheo_a_#{System.unique_integer([:positive])}")
    b = String.to_atom("rheo_b_#{System.unique_integer([:positive])}")

    {:ok, _} =
      start_supervised(
        {Rheo, name: a, backend: {Rheo.Backend.Mongo, url: url, name: Module.concat(a, Mongo)}}
      )

    {:ok, _} =
      start_supervised(
        {Rheo, name: b, backend: {Rheo.Backend.Mongo, url: url, name: Module.concat(b, Mongo)}}
      )

    stream = unique_stream("multi")
    assert :ok = Rheo.create_stream(stream, rheo: a)
    assert :ok = Rheo.create_group(stream, "risk", rheo: a)
    assert {:ok, event} = Rheo.append(stream, %{type: "x"}, rheo: a)

    assert {:ok, [^event]} = Rheo.read(stream, rheo: a)
    # Same DB, so instance B can also read if pointing at same collections
    assert {:ok, [_]} = Rheo.read(stream, rheo: b)

    ia = Rheo.Instance.fetch!(a)
    ib = Rheo.Instance.fetch!(b)
    assert ia.handle != ib.handle
    assert ia.backend == Rheo.Backend.Mongo
  end
end
