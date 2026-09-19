defmodule Mix.Tasks.Rheo.DeadLetters do
  @shortdoc "List Rheo dead letters for a group"
  @moduledoc """
  Lists dead-lettered deliveries (ops inspect, ADR 027).

      mix rheo.dead_letters STREAM GROUP
      mix rheo.dead_letters STREAM GROUP --limit 20 --rheo MyRheo
  """

  use Mix.Task

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, [stream, group], _} = Mix.Tasks.Rheo.InspectOpts.parse_with_argv(args)

    case Rheo.dead_letters(stream, group, opts) do
      {:ok, rows} ->
        Enum.each(rows, fn d ->
          Mix.shell().info("#{d.event_id} seq=#{inspect(d.sequence)} reason=#{inspect(d.reason)}")
        end)

      {:error, reason} ->
        Mix.raise("dead_letters failed: #{inspect(reason)}")
    end
  end
end
