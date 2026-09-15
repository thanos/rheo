defmodule Rheo.PersistErrorTest do
  use Rheo.Case, async: false

  @moduletag :mongo

  test "failed ACK returns stale_lease and is not silent" do
    stream = unique_stream("ackfail")
    :ok = Rheo.create_stream(stream)
    :ok = Rheo.create_group(stream, "risk")
    {:ok, _} = Rheo.append(stream, %{type: "x"})
    {:ok, [lease]} = Rheo.fetch(stream, "risk", limit: 1, consumer_id: "c1")
    assert :ok = Rheo.ack(lease)
    assert {:error, :stale_lease} = Rheo.ack(lease)
  end
end
