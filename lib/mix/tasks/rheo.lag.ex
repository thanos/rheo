defmodule Mix.Tasks.Rheo.Lag do
  @shortdoc "Show Rheo consumer group lag"
  @moduledoc """
  Prints lag for a stream/group (ops inspect, ADR 027).

      mix rheo.lag STREAM GROUP
      mix rheo.lag STREAM GROUP --rheo MyRheo
  """

  use Mix.Task

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, [stream, group]} = Mix.Tasks.Rheo.InspectOpts.parse!(args, 2, @moduledoc)

    case Rheo.lag(stream, group, opts) do
      {:ok, lag} ->
        Mix.shell().info("lag=#{lag.lag}")

        Enum.each(lag.partitions, fn {p, entry} ->
          Mix.shell().info(
            "  p#{p}: frontier=#{entry.frontier} hw=#{entry.high_watermark} lag=#{entry.lag}"
          )
        end)

      {:error, reason} ->
        Mix.raise("lag failed: #{inspect(reason)}")
    end
  end
end
