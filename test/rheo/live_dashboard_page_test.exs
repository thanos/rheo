defmodule Rheo.LiveDashboard.PageTest do
  use ExUnit.Case, async: false

  setup do
    name = Module.concat(["RheoDash#{System.unique_integer([:positive])}"])
    start_supervised!({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)
    previous = Application.get_env(:rheo, Rheo.LiveDashboard)
    Application.put_env(:rheo, Rheo.LiveDashboard, rheo: name)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, Rheo.LiveDashboard, previous)
      else
        Application.delete_env(:rheo, Rheo.LiveDashboard)
      end
    end)

    stream = "dash-#{System.unique_integer([:positive])}"
    :ok = Rheo.create_stream(stream, rheo: name)
    :ok = Rheo.create_group(stream, "g", rheo: name)
    %{stream: stream}
  end

  test "fetch_health honours sort and limit", %{stream: stream} do
    {rows, total} = Rheo.LiveDashboard.Page.fetch_health(%{limit: 1, sort_by: :stream}, :ignored)
    assert total >= 1
    assert length(rows) == 1
    assert hd(rows).stream == stream
    assert hd(rows).lag == 0
  end
end
