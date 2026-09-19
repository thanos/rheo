# Optional: compiled only when `:telemetry_metrics` is present.
if Code.ensure_loaded?(Telemetry.Metrics) do
  defmodule Rheo.Telemetry.Metrics do
    @moduledoc """
    Optional `Telemetry.Metrics` definitions for Rheo (v0.10+ / ADR 027).

    Requires `{:telemetry_metrics, "~> 1.0"}` (or compatible) in the host app.
    Wire into LiveDashboard, PromEx, or any reporter that accepts metric specs.

    These are counters / summaries over existing `[:rheo, …]` events — they do
    not change settle semantics.
    """

    import Telemetry.Metrics

    @doc "Canonical Rheo metric definitions for host reporters."
    @spec metrics() :: [Telemetry.Metrics.t()]
    def metrics do
      [
        counter("rheo.stream.create.count",
          event_name: [:rheo, :stream, :create],
          description: "Streams created"
        ),
        counter("rheo.group.create.count",
          event_name: [:rheo, :group, :create],
          description: "Consumer groups created"
        ),
        counter("rheo.lease.count",
          event_name: [:rheo, :lease],
          description: "Leases handed out by fetch",
          tags: [:stream, :group]
        ),
        counter("rheo.dead_letter.count",
          event_name: [:rheo, :dead_letter],
          description: "Dead-lettered deliveries",
          tags: [:stream, :group]
        ),
        counter("rheo.redelivery.count",
          event_name: [:rheo, :redelivery],
          description: "Redeliveries",
          tags: [:stream, :group]
        ),
        summary("rheo.append.stop.duration",
          event_name: [:rheo, :append, :stop],
          unit: {:native, :millisecond},
          description: "Append duration",
          tags: [:stream]
        ),
        summary("rheo.fetch.stop.duration",
          event_name: [:rheo, :fetch, :stop],
          unit: {:native, :millisecond},
          description: "Fetch duration",
          tags: [:stream, :group]
        ),
        summary("rheo.ack.stop.duration",
          event_name: [:rheo, :ack, :stop],
          unit: {:native, :millisecond},
          description: "Ack duration",
          tags: [:stream, :group]
        )
      ]
    end
  end
end
