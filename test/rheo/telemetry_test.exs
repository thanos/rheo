defmodule Rheo.TelemetryTest do
  use ExUnit.Case, async: false

  test "span emits exception events for throw and exit" do
    parent = self()
    handler = "span-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:rheo, :span_test, :exception],
      fn _e, _m, meta, _ -> send(parent, {:exception, meta.kind}) end,
      nil
    )

    try do
      assert :thrown =
               catch_throw(
                 Rheo.Telemetry.span([:rheo, :span_test], %{}, fn -> throw(:thrown) end)
               )

      assert_receive {:exception, :throw}

      assert :bye =
               catch_exit(Rheo.Telemetry.span([:rheo, :span_test], %{}, fn -> exit(:bye) end))

      assert_receive {:exception, :exit}
    after
      :telemetry.detach(handler)
    end
  end
end
