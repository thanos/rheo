defmodule Rheo.PartitionFrontierTest do
  use ExUnit.Case, async: false

  alias Rheo.{Lag, Partition}

  setup do
    name = :"pf_#{System.unique_integer([:positive])}"
    {:ok, _} = start_supervised({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)
    stream = "pf-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: name, partition_count: 3)
    :ok = Rheo.create_group(stream, "g", rheo: name)
    %{rheo: name, stream: stream}
  end

  test "same key routes to same partition", %{rheo: rheo, stream: stream} do
    assert {:ok, a} = Rheo.append(stream, %{type: "t", key: "EUR-1"}, rheo: rheo)
    assert {:ok, b} = Rheo.append(stream, %{type: "t", key: "EUR-1"}, rheo: rheo)
    assert a.partition == b.partition
    assert b.sequence == a.sequence + 1
  end

  test "invalid partition rejected", %{rheo: rheo, stream: stream} do
    assert {:error, :invalid_partition} =
             Rheo.append(stream, %{type: "t"}, partition: 99, rheo: rheo)
  end

  test "contiguous frontier hole rule", %{rheo: rheo, stream: stream} do
    assert {:ok, e1} = Rheo.append(stream, %{type: "t"}, partition: 0, rheo: rheo)
    assert {:ok, e2} = Rheo.append(stream, %{type: "t"}, partition: 0, rheo: rheo)
    assert {:ok, e3} = Rheo.append(stream, %{type: "t"}, partition: 0, rheo: rheo)
    assert [e1.sequence, e2.sequence, e3.sequence] == [1, 2, 3]

    assert {:ok, [l1, l2, l3]} =
             Rheo.fetch(stream, "g", limit: 3, partition: 0, rheo: rheo)

    assert :ok = Rheo.ack(l1, rheo: rheo)
    assert :ok = Rheo.ack(l3, rheo: rheo)

    assert {:ok, %Lag{partitions: parts, lag: lag}} = Rheo.lag(stream, "g", rheo: rheo)
    assert parts[0].frontier == 1
    assert parts[0].high_watermark == 3
    assert parts[0].lag == 2
    assert lag >= 2

    assert :ok = Rheo.ack(l2, rheo: rheo)
    assert {:ok, %Lag{partitions: parts2}} = Rheo.lag(stream, "g", rheo: rheo)
    assert parts2[0].frontier == 3
    assert parts2[0].lag == 0
  end

  test "fetch respects partitions assignment", %{rheo: rheo, stream: stream} do
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, partition: 0, rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, partition: 1, rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, partition: 2, rheo: rheo)

    assert {:ok, only1} = Rheo.fetch(stream, "g", partitions: [1], limit: 10, rheo: rheo)
    assert Enum.all?(only1, &(&1.event.partition == 1))
  end

  test "replay partition scope", %{rheo: rheo, stream: stream} do
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, partition: 0, rheo: rheo)
    assert {:ok, _} = Rheo.append(stream, %{type: "t"}, partition: 1, rheo: rheo)

    assert {:ok, leases} = Rheo.fetch(stream, "g", limit: 10, rheo: rheo)
    Enum.each(leases, &Rheo.ack(&1, rheo: rheo))

    assert :ok = Rheo.replay(stream, "g", from_sequence: 0, partition: 1, rheo: rheo)
    assert {:ok, again} = Rheo.fetch(stream, "g", limit: 10, rheo: rheo)
    assert Enum.all?(again, &(&1.event.partition == 1))
  end

  test "Partition helpers", %{rheo: _rheo, stream: _stream} do
    assert {:ok, 0} = Partition.resolve(%{}, [], 4)
    assert {:ok, p} = Partition.resolve(%{key: "x"}, [], 4)
    assert p in 0..3
    assert {:error, :invalid_partition} = Partition.validate(9, 3)
    assert {:ok, [0, 2]} = Partition.normalize_assignment([2, 0, 2], 3)
  end
end
