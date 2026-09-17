defmodule Rheo.Backend.CapabilitiesTest do
  use ExUnit.Case, async: true

  alias Rheo.Backend.{Capabilities, Ecto, ETS, Mongo, NativeStreamDouble}

  doctest Capabilities

  test "new splits guarantees and mechanisms and fills every key" do
    caps = Capabilities.new(durable: true, notifications: true, native_consumer_groups: true)

    assert caps.guarantees.durable
    refute caps.guarantees.distributed
    assert caps.guarantees.at_least_once
    assert caps.guarantees.lease_fencing
    assert caps.mechanisms.notifications
    assert caps.mechanisms.native_consumer_groups
    refute caps.mechanisms.blocking_reads

    assert Enum.sort(Map.keys(caps.guarantees)) == Enum.sort(Capabilities.guarantee_keys())
    assert Enum.sort(Map.keys(caps.mechanisms)) == Enum.sort(Capabilities.mechanism_keys())
  end

  test "accepts maps as well as keyword lists" do
    assert Capabilities.new(%{durable: true}) == Capabilities.new(durable: true)
  end

  test "rejects unknown keys" do
    assert_raise ArgumentError, ~r/unknown capability key: :exactly_once/, fn ->
      Capabilities.new(exactly_once: true)
    end
  end

  test "rejects non-boolean values" do
    assert_raise ArgumentError, ~r/:partitions must be a boolean/, fn ->
      Capabilities.new(partitions: 4)
    end
  end

  test "rejects disabling Rheo invariants" do
    for key <- [:at_least_once, :lease_fencing] do
      assert_raise ArgumentError, ~r/is a Rheo invariant/, fn ->
        Capabilities.new([{key, false}])
      end
    end

    assert Capabilities.new(at_least_once: true, lease_fencing: true).guarantees.lease_fencing
  end

  test "guarantee? and mechanism? only accept known keys" do
    caps = Capabilities.new(durable: true, batch_writes: true)
    assert Capabilities.guarantee?(caps, :durable)
    assert Capabilities.mechanism?(caps, :batch_writes)

    # Keys are checked in guards; build the wrong key at runtime so the type
    # checker does not reject the call outright.
    wrong = String.to_atom("batch_writes")
    assert_raise FunctionClauseError, fn -> Capabilities.guarantee?(caps, wrong) end
  end

  describe "backend declarations" do
    test "ETS is ephemeral, single-node, full-featured" do
      caps = ETS.capabilities()
      refute caps.guarantees.durable
      refute caps.guarantees.distributed
      assert caps.guarantees.partitions
      assert caps.guarantees.contiguous_frontier
      assert caps.guarantees.replay
      refute caps.mechanisms.secondary_indexes
    end

    test "Mongo is durable with secondary indexes" do
      caps = Mongo.capabilities()
      assert caps.guarantees.durable
      assert caps.guarantees.partitions
      assert caps.mechanisms.secondary_indexes
      refute caps.mechanisms.native_consumer_groups
    end

    test "Ecto depends on dialect and notify option" do
      assert Ecto.capabilities(:postgres).guarantees.distributed
      refute Ecto.capabilities(:sqlite).guarantees.distributed
      refute Ecto.capabilities(:postgres).mechanisms.notifications
      assert Ecto.capabilities() == Ecto.capabilities(:postgres)
    end

    test "native-stream double declares native mechanisms without claiming durability" do
      caps = NativeStreamDouble.capabilities()
      refute caps.guarantees.durable
      assert caps.guarantees.lease_fencing
      assert caps.mechanisms.native_consumer_groups
      assert caps.mechanisms.native_pending_list
      assert caps.mechanisms.native_reclaim
    end
  end
end
