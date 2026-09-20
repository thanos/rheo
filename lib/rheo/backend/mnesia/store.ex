defmodule Rheo.Backend.Mnesia.Store do
  @moduledoc false
  # Thin behaviour over `:mnesia` ops. Default impl is
  # `Rheo.Backend.Mnesia.Store.Mnesia`; tests may swap via
  # `Application.put_env(:rheo, :mnesia_store, Mock)`.

  @type table :: atom()
  @type key :: term()
  @type row :: tuple()

  @callback running?() :: boolean()
  @callback stop() :: :stopped | term()
  @callback create_schema([node()]) :: :ok | {:error, term()}
  @callback start() :: :ok | {:error, term()}
  @callback disc_schema?() :: boolean()
  @callback create_table(table(), keyword()) :: {:atomic, :ok} | {:aborted, term()}
  @callback wait_for_tables([table()], timeout()) :: :ok | {:timeout, [table()]}
  @callback dirty_read(table(), key()) :: [row()]
  @callback dirty_write(row()) :: :ok
  @callback dirty_delete(table(), key()) :: :ok
  @callback dirty_select(table(), term()) :: [term()]
  @callback dirty_next(table(), key()) :: key() | :"$end_of_table"

  @doc false
  @spec current() :: module()
  def current, do: Application.get_env(:rheo, :mnesia_store, Rheo.Backend.Mnesia.Store.Mnesia)
end

defmodule Rheo.Backend.Mnesia.Store.Mnesia do
  @moduledoc false
  @behaviour Rheo.Backend.Mnesia.Store

  @impl true
  def running?, do: :mnesia.system_info(:is_running) == :yes

  @impl true
  def stop, do: :mnesia.stop()

  @impl true
  def create_schema(nodes), do: :mnesia.create_schema(nodes)

  @impl true
  def start, do: :mnesia.start()

  @impl true
  def disc_schema? do
    :mnesia.table_info(:schema, :disc_copies) != []
  rescue
    _ -> false
  end

  @impl true
  def create_table(name, opts), do: :mnesia.create_table(name, opts)

  @impl true
  def wait_for_tables(tables, timeout), do: :mnesia.wait_for_tables(tables, timeout)

  @impl true
  def dirty_read(table, key), do: :mnesia.dirty_read(table, key)

  @impl true
  def dirty_write(record), do: :mnesia.dirty_write(record)

  @impl true
  def dirty_delete(table, key), do: :mnesia.dirty_delete(table, key)

  @impl true
  def dirty_select(table, match_spec), do: :mnesia.dirty_select(table, match_spec)

  @impl true
  def dirty_next(table, key), do: :mnesia.dirty_next(table, key)
end
