defmodule Rheo.Backend.CapabilitiesTest do
  use ExUnit.Case, async: true

  alias Rheo.Backend.Capabilities

  test "new splits guarantees and mechanisms" do
    caps =
      Capabilities.new(
        durable: true,
        notifications: true,
        native_consumer_groups: true
      )

    assert caps.guarantees.durable
    assert caps.mechanisms.notifications
    assert caps.mechanisms.native_consumer_groups
    assert caps.guarantees.at_least_once
    assert caps.guarantees.lease_fencing
  end

  test "normalize accepts legacy flat maps" do
    caps = Capabilities.normalize(%{durable: false, atomic_compare_and_set: true})
    refute Capabilities.guarantee?(caps, :durable)
    assert Capabilities.mechanism?(caps, :atomic_compare_and_set)
  end

  test "to_legacy_map merges both layers" do
    flat =
      Capabilities.new(durable: true, blocking_reads: true)
      |> Capabilities.to_legacy_map()

    assert flat.durable
    assert flat.blocking_reads
  end
end
