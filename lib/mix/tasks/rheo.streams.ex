defmodule Mix.Tasks.Rheo.Streams do
  @shortdoc "List Rheo stream names"
  @moduledoc """
  Lists registered streams for a Rheo instance (ops inspect, ADR 027).

      mix rheo.streams
      mix rheo.streams --rheo MyRheo
  """

  use Mix.Task

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    opts = Mix.Tasks.Rheo.InspectOpts.parse(args)

    case Rheo.list_streams(opts) do
      {:ok, streams} ->
        Enum.each(streams, fn stream -> Mix.shell().info(stream) end)

      {:error, reason} ->
        Mix.raise("list_streams failed: #{inspect(reason)}")
    end
  end
end
