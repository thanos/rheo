defmodule Rheo.Application do
  @moduledoc """
  OTP application entry for Rheo.

  By default Rheo does not auto-start an instance. Host applications supervise
  `{Rheo, opts}` themselves.

  Set `config :rheo, start_on_application: true` together with
  `config :rheo, backend: Rheo.Backend.ETS` (or `{module, opts}`) to start the
  default instance under this application. The legacy `:mongo_url` key is
  honoured only when `:backend` is unset and `Rheo.Backend.Mongo` is available.
  """

  use Application

  @doc false
  @impl true
  def start(_type, _args) do
    children =
      case rheo_opts() do
        nil -> []
        opts -> [{Rheo, opts}]
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: Rheo.AppSupervisor)
  end

  defp rheo_opts do
    if Application.get_env(:rheo, :start_on_application, false) do
      backend_opts() &&
        Keyword.put(backend_opts(), :name, Application.get_env(:rheo, :name, Rheo))
    end
  end

  defp backend_opts do
    case {Application.get_env(:rheo, :backend), Application.get_env(:rheo, :mongo_url)} do
      {nil, nil} -> nil
      {nil, url} -> [url: url]
      {backend, _} -> [backend: backend]
    end
  end
end
