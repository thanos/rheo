defmodule Rheo.Application do
  @moduledoc """
  OTP application entry for Rheo.

  By default Rheo does **not** auto-start a backend. Host applications should
  supervise `{Rheo, opts}` themselves.

  Set `config :rheo, start_on_application: true` when you intentionally want Rheo
  to start under this application callback. For Mongo, also set `:mongo_url`.
  For ETS, set `config :rheo, backend: Rheo.Backend.ETS` (no URL required).
  """

  use Application

  @doc false
  @impl true
  def start(_type, _args) do
    children =
      if start_rheo?() do
        [{Rheo, rheo_opts()}]
      else
        []
      end

    opts = [strategy: :one_for_one, name: Rheo.AppSupervisor]
    Supervisor.start_link(children, opts)
  end

  defp start_rheo? do
    Application.get_env(:rheo, :start_on_application, false) and backend_configured?()
  end

  defp backend_configured? do
    case Application.get_env(:rheo, :backend) do
      Rheo.Backend.ETS -> true
      {Rheo.Backend.ETS, _} -> true
      _ -> not is_nil(Application.get_env(:rheo, :mongo_url))
    end
  end

  defp rheo_opts do
    []
    |> put_opt(:name, Application.get_env(:rheo, :name, Rheo))
    |> put_backend()
  end

  defp put_backend(opts) do
    case Application.get_env(:rheo, :backend) do
      nil ->
        put_opt(opts, :url, Application.get_env(:rheo, :mongo_url))

      {mod, backend_opts} when is_list(backend_opts) ->
        Keyword.put(opts, :backend, {mod, backend_opts})

      mod when is_atom(mod) ->
        Keyword.put(opts, :backend, mod)
    end
  end

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)
end
