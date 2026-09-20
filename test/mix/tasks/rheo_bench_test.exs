defmodule Mix.Tasks.Rheo.BenchTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  defp run(args) do
    capture_io(fn -> Mix.Tasks.Rheo.Bench.run(args) end)
  end

  test "reports append and consume rates and tears the instance down" do
    output = run(["--count", "25"])

    assert output =~ "backend=ETS count=25"
    assert [_, append] = Regex.run(~r/append_per_s=([\d.]+)/, output)
    assert [_, consume] = Regex.run(~r/consume_per_s=([\d.]+)/, output)
    assert String.to_float(append) > 0.0
    assert String.to_float(consume) > 0.0

    refute Process.whereis(Rheo.Bench), "the bench instance should be stopped"
  end

  test "defaults to 2_000 events when --count is omitted" do
    assert run([]) =~ "backend=ETS count=2000"
  end

  test "is repeatable, so the named instance is not leaked between runs" do
    assert run(["--count", "5"]) =~ "count=5"
    assert run(["--count", "5"]) =~ "count=5"
  end

  # Guards the fix for the O(n^2) ETS claim path: consume throughput must not
  # collapse as the stream grows. A quadratic backend fails this comfortably —
  # before the fix the rate dropped ~4x between these two sizes.
  @tag :slow
  test "consume throughput stays flat as the event count grows" do
    small = consume_rate(run(["--count", "500"]))
    large = consume_rate(run(["--count", "8000"]))

    assert large > small / 2,
           "consume rate degraded from #{small}/s at 500 events to #{large}/s at 8000"
  end

  defp consume_rate(output) do
    [_, rate] = Regex.run(~r/consume_per_s=([\d.]+)/, output)
    String.to_float(rate)
  end
end
