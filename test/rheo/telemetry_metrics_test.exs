defmodule Rheo.Telemetry.MetricsTest do
  use ExUnit.Case, async: true

  alias Rheo.Telemetry.Metrics, as: RheoMetrics

  defp definitions, do: RheoMetrics.metrics()

  test "every definition is a Telemetry.Metrics struct with a description" do
    refute Enum.empty?(definitions())

    for metric <- definitions() do
      assert metric.__struct__ in [Telemetry.Metrics.Counter, Telemetry.Metrics.Summary]
      assert is_binary(metric.description) and metric.description != ""
      assert is_list(metric.event_name) and metric.event_name != []
    end
  end

  test "metric names are unique" do
    names = Enum.map(definitions(), & &1.name)
    assert names == Enum.uniq(names)
  end

  test "durations are reported in milliseconds" do
    for %Telemetry.Metrics.Summary{} = metric <- definitions() do
      assert metric.unit == :millisecond,
             "#{inspect(metric.name)} should convert :native to :millisecond"
    end
  end

  # The definitions are only useful if Rheo actually emits the events they
  # listen on. Drive a real instance and assert each one fires.
  test "every declared event is emitted by a real Rheo instance" do
    events = Enum.map(definitions(), & &1.event_name) |> Enum.uniq()
    handler = "rheo-metrics-test-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        handler,
        events,
        fn event, measurements, _meta, _ -> send(parent, {:event, event, measurements}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    name = :"metrics_#{System.unique_integer([:positive])}"
    {:ok, _} = Rheo.start_link(name: name, backend: Rheo.Backend.ETS)
    stream = "metrics-#{System.unique_integer([:positive])}"

    :ok = Rheo.create_stream(stream, rheo: name)
    :ok = Rheo.create_group(stream, "g", rheo: name, max_attempts: 5)

    {:ok, _} = Rheo.append(stream, %{type: "e"}, rheo: name)
    {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: name)
    :ok = Rheo.ack(lease, rheo: name)

    # A nack then a re-fetch is attempt 2, which emits [:rheo, :redelivery];
    # rejecting that second lease emits [:rheo, :dead_letter].
    {:ok, _} = Rheo.append(stream, %{type: "e2"}, rheo: name)
    {:ok, [first]} = Rheo.fetch(stream, "g", limit: 1, rheo: name)
    :ok = Rheo.nack(first, :boom, rheo: name)
    {:ok, [retried]} = Rheo.fetch(stream, "g", limit: 1, rheo: name)
    assert retried.attempt == 2
    :ok = Rheo.reject(retried, :poison, rheo: name)

    for event <- events do
      assert_received {:event, ^event, _measurements},
                      "no #{inspect(event)} emitted; the metric definition is dead"
    end
  end
end
