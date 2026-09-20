defmodule Rheo.Instance do
  @moduledoc """
  Runtime metadata for a named Rheo instance.

  Holds the backend module and opaque handle used by public `Rheo` APIs.
  Started under the Rheo instance supervisor; not authoritative for durable state.
  """
  use GenServer

  @type t :: %__MODULE__{
          name: atom(),
          backend: module(),
          handle: Rheo.Backend.handle()
        }

  defstruct [:name, :backend, :handle]

  @doc false
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: Rheo.Names.instance(name))
  end

  @doc """
  Fetches instance metadata for a Rheo instance name.

  ## Examples

      iex> %Rheo.Instance{name: Rheo, backend: Rheo.Backend.Mongo} = Rheo.Instance.fetch!(Rheo)

  ## Returns

  `%Rheo.Instance{}`

  ## Errors / raises

  Raises if the instance process is not running.
  """
  @spec fetch!(atom()) :: t()
  def fetch!(rheo) when is_atom(rheo) do
    :persistent_term.get({__MODULE__, rheo})
  rescue
    ArgumentError ->
      GenServer.call(Rheo.Names.instance(rheo), :get)
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      backend: Keyword.fetch!(opts, :backend),
      handle: Keyword.fetch!(opts, :handle)
    }

    :persistent_term.put({__MODULE__, state.name}, state)
    {:ok, state}
  end

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase({__MODULE__, state.name})
    :ok
  end
end
