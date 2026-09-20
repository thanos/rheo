defmodule Mix.Tasks.Rheo.InspectTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Rheo.{DeadLetters, GroupInfo, Lag, Streams}

  setup do
    name = Module.concat(["RheoMix#{System.unique_integer([:positive])}"])
    start_supervised!({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)
    stream = "s-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: name)
    :ok = Rheo.create_group(stream, "g", rheo: name)
    %{name: name, stream: stream}
  end

  test "rheo.streams lists streams and rejects a missing instance", %{name: name, stream: stream} do
    Mix.Task.reenable("rheo.streams")

    output =
      capture_io(fn ->
        Streams.run(["--rheo", Atom.to_string(name)])
      end)

    assert output =~ stream

    Mix.Task.reenable("rheo.streams")

    assert_raise Mix.Error, ~r/not running/, fn ->
      Streams.run(["--rheo", "MissingRheoInstance"])
    end
  end

  test "invalid switches and wrong argv fail", %{name: name} do
    Mix.Task.reenable("rheo.streams")

    assert_raise Mix.Error, ~r/invalid option/, fn ->
      Streams.run(["--limitt", "1", "--rheo", Atom.to_string(name)])
    end

    Mix.Task.reenable("rheo.lag")

    assert_raise Mix.Error, ~r/expected 2 argument/, fn ->
      Lag.run(["--rheo", Atom.to_string(name)])
    end
  end

  test "lag, group_info, and dead_letters happy paths", %{name: name, stream: stream} do
    rheo = Atom.to_string(name)

    Mix.Task.reenable("rheo.lag")
    lag = capture_io(fn -> Lag.run([stream, "g", "--rheo", rheo]) end)
    assert lag =~ "lag="

    Mix.Task.reenable("rheo.group_info")
    info = capture_io(fn -> GroupInfo.run([stream, "g", "--rheo", rheo]) end)
    assert info =~ "inflight="

    Mix.Task.reenable("rheo.dead_letters")
    letters = capture_io(fn -> DeadLetters.run([stream, "g", "--rheo", rheo]) end)
    assert is_binary(letters)
  end
end
