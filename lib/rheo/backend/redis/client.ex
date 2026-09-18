# Optional integration: compiled only when `:redix` is present.
if Code.ensure_loaded?(Redix) do
  defmodule Rheo.Backend.Redis.Client do
    @moduledoc """
    Thin behaviour over the Redix operations Rheo uses.

    The default implementation is `Redix` itself (`command/3`, `pipeline/3`).
    Tests may swap in a Mox mock via
    `Application.put_env(:rheo, :redis_client, Mock)`.
    """

    @type conn :: GenServer.server()
    @type redis_command :: [binary() | integer()]

    @callback command(conn(), redis_command(), keyword()) :: {:ok, term()} | {:error, term()}

    @callback pipeline(conn(), [redis_command()], keyword()) ::
                {:ok, [term()]} | {:error, term()}

    @doc false
    @spec current() :: module()
    def current, do: Application.get_env(:rheo, :redis_client, Redix)
  end
end
