defmodule Mix.Tasks.Rheo.GroupInfo do
  @shortdoc "Show Rheo group health"
  @moduledoc """
  Prints lag, inflight, and dead-letter counts (ops inspect, ADR 027).

      mix rheo.group_info STREAM GROUP
      mix rheo.group_info STREAM GROUP --rheo MyRheo
  """

  use Mix.Task

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, [stream, group]} = Mix.Tasks.Rheo.InspectOpts.parse!(args, 2, @moduledoc)

    case Rheo.group_info(stream, group, opts) do
      {:ok, info} ->
        Mix.shell().info("stream=#{info.stream} group=#{info.group}")
        Mix.shell().info("lag=#{info.lag.lag}")
        Mix.shell().info("inflight=#{info.inflight_count}")
        Mix.shell().info("dead_letters=#{info.dead_letter_count}")

      {:error, reason} ->
        Mix.raise("group_info failed: #{inspect(reason)}")
    end
  end
end
