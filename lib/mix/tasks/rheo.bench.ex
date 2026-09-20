defmodule Mix.Tasks.Rheo.Bench do
  @shortdoc "Relative Rheo backend throughput (ETS)"
  @moduledoc """
  Runs a small relative benchmark against `Rheo.Backend.ETS`.

  Numbers are comparative only — not SLOs. Optional Redis/Mongo benches stay in
  Livebook (`notebooks/backends.livemd`).

      mix rheo.bench
      mix rheo.bench --count 5000
  """

  use Mix.Task

  @impl true
  def run(args) do
    {parsed, _, _} = OptionParser.parse(args, strict: [count: :integer])
    count = parsed[:count] || 2_000

    {:ok, _} = Rheo.start_link(name: Rheo.Bench, backend: Rheo.Backend.ETS)

    try do
      stream = "bench-" <> Integer.to_string(System.unique_integer([:positive]))
      :ok = Rheo.create_stream(stream, rheo: Rheo.Bench)
      :ok = Rheo.create_group(stream, "g", rheo: Rheo.Bench)

      {append_us, _} =
        :timer.tc(fn ->
          Enum.each(1..count, fn i ->
            {:ok, _} = Rheo.append(stream, %{type: "e", n: i}, rheo: Rheo.Bench)
          end)
        end)

      {consume_us, _} =
        :timer.tc(fn ->
          consume_all(stream, count)
        end)

      Mix.shell().info("backend=ETS count=#{count}")
      Mix.shell().info("append_us=#{append_us} append_per_s=#{rate(count, append_us)}")
      Mix.shell().info("consume_us=#{consume_us} consume_per_s=#{rate(count, consume_us)}")
    after
      _ = Supervisor.stop(Rheo.Bench)
    end
  end

  @consume_deadline_ms 60_000

  defp consume_all(stream, remaining, started \\ System.monotonic_time(:millisecond))

  defp consume_all(_stream, remaining, _started) when remaining <= 0, do: :ok

  defp consume_all(stream, remaining, started) do
    if System.monotonic_time(:millisecond) - started > @consume_deadline_ms do
      Mix.raise("consume deadline exceeded with #{remaining} events remaining")
    end

    limit = min(remaining, 50)

    case Rheo.fetch(stream, "g", limit: limit, rheo: Rheo.Bench) do
      {:ok, []} ->
        :ok

      {:ok, leases} ->
        Enum.each(leases, &Rheo.ack(&1, rheo: Rheo.Bench))
        consume_all(stream, remaining - length(leases), started)

      {:error, reason} ->
        Mix.raise("consume failed: #{inspect(reason)}")
    end
  end

  defp rate(count, us) when us > 0, do: Float.round(count * 1_000_000 / us, 1)
  defp rate(_count, _us), do: 0.0
end
