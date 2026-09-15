defmodule Rheo.Application do
  @moduledoc """
  OTP application entry for Rheo.

  By default Rheo does **not** auto-start a backend. Host applications should
  supervise `{Rheo, opts}` themselves.

  Set `config :rheo, start_on_application: true` and `:mongo_url` only when you
  intentionally want Rheo to start under this application callback.
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
    Application.get_env(:rheo, :start_on_application, false) and
      not is_nil(Application.get_env(:rheo, :mongo_url))
  end

  defp rheo_opts do
    []
    |> put_opt(:url, Application.get_env(:rheo, :mongo_url))
    |> put_opt(:name, Application.get_env(:rheo, :name, Rheo))
  end

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)
end
