defmodule Rheo.Backend.Mnesia.StoreTest do
  use ExUnit.Case, async: false

  import Mox

  alias Rheo.Backend.Mnesia
  alias Rheo.Backend.Mnesia.StoreMock

  setup :verify_on_exit!

  setup do
    previous = Application.get_env(:rheo, :mnesia_store)
    Application.put_env(:rheo, :mnesia_store, StoreMock)

    on_exit(fn ->
      if previous do
        Application.put_env(:rheo, :mnesia_store, previous)
      else
        Application.delete_env(:rheo, :mnesia_store)
      end
    end)

    :ok
  end

  test "bootstrap raises when create_schema fails" do
    stub(StoreMock, :running?, fn -> false end)

    expect(StoreMock, :create_schema, fn [_node] -> {:error, :nope} end)

    assert_raise RuntimeError, ~r/create_schema failed/, fn ->
      start_supervised!(
        {Mnesia, name: :"mnesia_schema_fail_#{System.unique_integer([:positive])}"}
      )
    end
  end

  test "bootstrap raises when start fails" do
    stub(StoreMock, :running?, fn -> false end)
    expect(StoreMock, :create_schema, fn [_node] -> :ok end)
    expect(StoreMock, :start, fn -> {:error, :cannot_start} end)

    assert_raise RuntimeError, ~r/start failed/, fn ->
      start_supervised!(
        {Mnesia, name: :"mnesia_start_fail_#{System.unique_integer([:positive])}"}
      )
    end
  end

  test "bootstrap raises when schema has no disc_copies" do
    stub(StoreMock, :running?, fn -> false end)
    expect(StoreMock, :create_schema, fn [_node] -> {:error, {:x, {:already_exists, :y}}} end)
    expect(StoreMock, :start, fn -> :ok end)
    expect(StoreMock, :disc_schema?, fn -> false end)

    assert_raise RuntimeError, ~r/no disc_copies/, fn ->
      start_supervised!({Mnesia, name: :"mnesia_nodisc_#{System.unique_integer([:positive])}"})
    end
  end

  test "create_table abort raises" do
    stub(StoreMock, :running?, fn -> true end)
    stub(StoreMock, :disc_schema?, fn -> true end)

    expect(StoreMock, :create_table, fn _name, _opts -> {:aborted, :badarg} end)

    assert_raise RuntimeError, ~r/create_table/, fn ->
      start_supervised!(
        {Mnesia, name: :"mnesia_table_fail_#{System.unique_integer([:positive])}"}
      )
    end
  end

  test "wait_for_tables timeout raises" do
    stub(StoreMock, :running?, fn -> true end)
    stub(StoreMock, :disc_schema?, fn -> true end)
    stub(StoreMock, :create_table, fn _name, _opts -> {:atomic, :ok} end)
    expect(StoreMock, :wait_for_tables, fn _tables, _timeout -> {:timeout, [:t]} end)

    assert_raise RuntimeError, ~r/not ready/, fn ->
      start_supervised!({Mnesia, name: :"mnesia_wait_fail_#{System.unique_integer([:positive])}"})
    end
  end

  test "recycles mnesia when running without disc schema" do
    {:ok, _} =
      Agent.start_link(fn -> %{running?: true, disc?: false} end, name: __MODULE__.DiscFlag)

    on_exit(fn ->
      if pid = Process.whereis(__MODULE__.DiscFlag) do
        Process.exit(pid, :kill)
      end
    end)

    stub(StoreMock, :running?, fn ->
      Agent.get(__MODULE__.DiscFlag, & &1.running?)
    end)

    stub(StoreMock, :disc_schema?, fn ->
      Agent.get(__MODULE__.DiscFlag, & &1.disc?)
    end)

    expect(StoreMock, :stop, fn ->
      Agent.update(__MODULE__.DiscFlag, &%{&1 | running?: false})
      :stopped
    end)

    expect(StoreMock, :create_schema, fn [_node] -> :ok end)

    expect(StoreMock, :start, fn ->
      Agent.update(__MODULE__.DiscFlag, &%{&1 | running?: true, disc?: true})
      {:error, {:already_started, :mnesia}}
    end)

    stub(StoreMock, :create_table, fn _name, _opts -> {:atomic, :ok} end)
    stub(StoreMock, :wait_for_tables, fn _tables, _timeout -> :ok end)
    stub(StoreMock, :dirty_read, fn _t, _k -> [] end)
    stub(StoreMock, :dirty_write, fn _rec -> :ok end)
    stub(StoreMock, :dirty_select, fn _t, _ms -> [] end)

    name = :"mnesia_recycle_#{System.unique_integer([:positive])}"
    assert {:ok, _} = start_supervised({Mnesia, name: name}, id: name)
    assert :ok = Mnesia.ping(name)
  end
end
