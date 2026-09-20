defmodule Rheo.LiveDashboard.PageTest do
  use ExUnit.Case, async: false

  import Mox

  alias Rheo.Backend.Mock
  alias Rheo.LiveDashboard.Page
  alias Rheo.Test.MoxBackend

  setup :verify_on_exit!

  setup do
    previous = Application.get_env(:rheo, Rheo.LiveDashboard)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, Rheo.LiveDashboard, previous)
      else
        Application.delete_env(:rheo, Rheo.LiveDashboard)
      end
    end)

    :ok
  end

  defp start_ets! do
    name = Module.concat(["RheoDash#{System.unique_integer([:positive])}"])
    start_supervised!({Rheo, name: name, backend: Rheo.Backend.ETS}, id: name)
    Application.put_env(:rheo, Rheo.LiveDashboard, rheo: name)
    name
  end

  defp fetch(params, state \\ nil), do: Page.fetch_health(params, :ignored, state)

  describe "with a live ETS instance" do
    setup do
      rheo = start_ets!()
      stream = "dash-#{System.unique_integer([:positive])}"
      :ok = Rheo.create_stream(stream, rheo: rheo)
      :ok = Rheo.create_group(stream, "g", rheo: rheo)
      %{rheo: rheo, stream: stream}
    end

    test "honours sort and limit", %{stream: stream} do
      {rows, total, _state} = fetch(%{limit: 1, sort_by: :stream})
      assert total >= 1
      assert length(rows) == 1
      assert hd(rows).stream == stream
      assert hd(rows).lag == 0
    end

    test "sort_dir is honoured in both directions", %{rheo: rheo, stream: stream} do
      other = "dash-aaa-#{System.unique_integer([:positive])}"
      :ok = Rheo.create_stream(other, rheo: rheo)
      :ok = Rheo.create_group(other, "g", rheo: rheo)

      {asc, _, _} = fetch(%{sort_by: :stream, sort_dir: :asc})
      {desc, _, _} = fetch(%{sort_by: :stream, sort_dir: :desc})

      assert Enum.map(asc, & &1.stream) == Enum.sort([stream, other])
      assert Enum.map(desc, & &1.stream) == Enum.sort([stream, other], :desc)
    end

    test "string params from the query string work too", %{stream: stream} do
      {rows, _total, _state} = fetch(%{"sort_by" => :stream, "limit" => 1})
      assert hd(rows).stream == stream
    end

    test "reported lag reflects real group state", %{rheo: rheo, stream: stream} do
      {:ok, _} = Rheo.append_batch(stream, [%{type: "a"}, %{type: "b"}], rheo: rheo)
      {:ok, [lease]} = Rheo.fetch(stream, "g", limit: 1, rheo: rheo)

      {rows, _total, _state} = fetch(%{})
      row = Enum.find(rows, &(&1.stream == stream))
      assert row.lag == 2
      assert row.inflight == 1

      :ok = Rheo.reject(lease, :poison, rheo: rheo)
      {rows, _total, _state} = fetch(%{})
      row = Enum.find(rows, &(&1.stream == stream))
      assert row.dead_letters == 1
    end
  end

  describe "caching" do
    setup do
      rheo = start_ets!()
      stream = "dash-#{System.unique_integer([:positive])}"
      :ok = Rheo.create_stream(stream, rheo: rheo)
      :ok = Rheo.create_group(stream, "g", rheo: rheo)
      %{rheo: rheo, stream: stream}
    end

    test "a second call inside the TTL does not re-walk the backend", %{rheo: rheo} do
      {_rows, total, {_cached, fetched_at} = state} = fetch(%{})
      assert is_integer(fetched_at)

      # A stream created after the first fetch must not appear until the TTL
      # lapses; that is what proves the second call served from cache.
      other = "dash-new-#{System.unique_integer([:positive])}"
      :ok = Rheo.create_stream(other, rheo: rheo)
      :ok = Rheo.create_group(other, "g", rheo: rheo)

      {rows, ^total, {_, ^fetched_at}} = fetch(%{sort_by: :lag, sort_dir: :desc}, state)
      refute Enum.any?(rows, &(&1.stream == other))
    end

    test "a stale entry is refetched", %{stream: stream} do
      stale_at = System.monotonic_time(:millisecond) - 60_000
      stale = {[], stale_at}

      {rows, total, {_, fetched_at}} = fetch(%{}, stale)

      assert total >= 1
      assert fetched_at > stale_at
      assert Enum.any?(rows, &(&1.stream == stream))
    end
  end

  describe "when the backend cannot answer" do
    setup do
      %{rheo: rheo, handle: handle, child: child} = MoxBackend.start_opts!("dash")
      start_supervised!(child, id: rheo)
      Application.put_env(:rheo, Rheo.LiveDashboard, rheo: rheo)
      set_mox_global()
      %{rheo: rheo, handle: handle}
    end

    test "a group whose info fails renders :error rather than a healthy zero" do
      stub(Mock, :list_streams, fn _handle, _opts -> {:ok, ["s"]} end)
      stub(Mock, :list_groups, fn _handle, "s", _opts -> {:ok, ["g"]} end)
      stub(Mock, :group_info, fn _handle, "s", "g", _opts -> {:error, :backend_unavailable} end)

      {[row], 1, _state} = fetch(%{})

      assert row.stream == "s"
      assert row.group == "g"
      assert row.lag == :error
      assert row.inflight == :error
      assert row.dead_letters == :error
    end

    test "an unreachable instance yields no rows instead of crashing the page" do
      stub(Mock, :list_streams, fn _handle, _opts -> {:error, :backend_unavailable} end)

      assert {[], 0, _state} = fetch(%{})
    end
  end
end
