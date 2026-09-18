defmodule Rheo.Test.Redis do
  @moduledoc false

  # Helpers for the `:redis`-tagged suites. Redis is durable, so keys survive
  # between runs while `System.unique_integer/1` restarts with each BEAM: tests
  # must clear the `rheo:` namespace instead of relying on fresh names.

  def url, do: System.get_env("RHEO_REDIS_URL", "redis://localhost:6379")

  def available? do
    if Code.ensure_loaded?(Redix) do
      {:ok, _} = Application.ensure_all_started(:redix)

      with_connection(fn conn -> match?({:ok, _}, Redix.command(conn, ["PING"])) end)
    else
      false
    end
  end

  def flush(pattern \\ "rheo:*") do
    with_connection(fn conn -> delete_matching(conn, pattern, "0") end)
  end

  defp with_connection(fun) do
    case Redix.start_link(url()) do
      {:ok, conn} ->
        try do
          fun.(conn)
        after
          Redix.stop(conn)
        end

      _error ->
        false
    end
  end

  defp delete_matching(conn, pattern, cursor) do
    case Redix.command(conn, ["SCAN", cursor, "MATCH", pattern, "COUNT", "1000"]) do
      {:ok, [next, keys]} ->
        if keys != [], do: Redix.command(conn, ["DEL" | keys])
        if next == "0", do: :ok, else: delete_matching(conn, pattern, next)

      _error ->
        :ok
    end
  end
end
