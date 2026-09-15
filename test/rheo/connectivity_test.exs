defmodule Rheo.ConnectivityTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  test "pings MongoDB through Rheo" do
    assert :ok = Rheo.ping()
  end

  test "ensures indexes" do
    assert :ok = Rheo.ensure_indexes()
  end
end
