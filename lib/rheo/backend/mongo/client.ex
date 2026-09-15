defmodule Rheo.Backend.Mongo.Client do
  @moduledoc """
  Thin behaviour over the MongoDB driver operations Rheo uses.

  The default implementation wraps the Mongo driver; tests may swap in a Mox
  mock via `Application.put_env(:rheo, :mongo_client, Mock)`.
  """

  @type handle :: term()
  @type collection :: String.t()
  @type document :: map()
  @type filter :: map()

  @callback command(handle(), keyword() | map()) :: {:ok, term()} | {:error, term()}

  @callback insert_one(handle(), collection(), document()) :: {:ok, term()} | {:error, term()}

  @callback insert_many(handle(), collection(), [document()], keyword()) ::
              {:ok, term()} | {:error, term()}

  @callback find(handle(), collection(), filter(), keyword()) :: Enumerable.t()

  @callback find_one(handle(), collection(), filter()) :: document() | nil

  @callback find_one_and_update(handle(), collection(), filter(), map(), keyword()) ::
              {:ok, term()} | {:error, term()}

  @callback create_indexes(handle(), collection(), [keyword()]) ::
              {:ok, term()} | :ok | {:error, term()}

  @doc false
  @spec current() :: module()
  def current, do: Application.get_env(:rheo, :mongo_client, Rheo.Backend.Mongo.Client.Driver)
end
