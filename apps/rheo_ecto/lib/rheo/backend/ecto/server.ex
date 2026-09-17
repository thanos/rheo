defmodule Rheo.Backend.Ecto.Server do
  @moduledoc """
  Configuration holder for `Rheo.Backend.Ecto`; the opaque backend handle.

  The host application owns and supervises the `Ecto.Repo`. This process only
  resolves the repo's adapter into a Rheo SQL dialect once at start and answers
  `config/1` for every backend call, so the handle stays a registered name like
  it is for the Mongo and ETS backends.

  Started for you by `Rheo.Backend.Ecto.child_spec/1`.
  """

  use GenServer

  alias Rheo.Backend.Ecto.Migrations

  @typedoc "Resolved backend configuration."
  @type config :: %{
          repo: module(),
          dialect: :postgres | :sqlite,
          notify?: boolean(),
          prefix: String.t() | nil
        }

  defstruct [:name, :config]

  @doc false
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Fetches resolved configuration for a backend handle.

  ## Arguments

    * `handle` — the registered name (or pid) of this process

  ## Returns

  `{:ok, config}`, or `{:error, :backend_unavailable}` when the process is not
  running or does not answer in time.
  """
  @spec config(Rheo.Backend.handle()) :: {:ok, config()} | {:error, :backend_unavailable}
  def config(handle) do
    {:ok, GenServer.call(handle, :config, timeout())}
  catch
    :exit, {:noproc, _} -> {:error, :backend_unavailable}
    :exit, {:timeout, _} -> {:error, :backend_unavailable}
  end

  @impl true
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    dialect = Keyword.get_lazy(opts, :dialect, fn -> Migrations.dialect_for(repo) end)

    config = %{
      repo: repo,
      dialect: dialect,
      # LISTEN/NOTIFY is PostgreSQL-only; SQLite silently opts out.
      notify?: Keyword.get(opts, :notify, false) and dialect == :postgres,
      prefix: Keyword.get(opts, :prefix)
    }

    {:ok, %__MODULE__{name: Keyword.fetch!(opts, :name), config: config}}
  end

  @impl true
  def handle_call(:config, _from, state), do: {:reply, state.config, state}

  defp timeout, do: Application.get_env(:rheo, :ecto_call_timeout, 5_000)
end
