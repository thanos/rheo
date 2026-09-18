defmodule Rheo.Test.MoxBackend do
  @moduledoc false

  # Starts a named Rheo instance whose storage is `Rheo.Backend.Mock`, so Group /
  # Producer unit tests can drive fetch / settle / renew with Mox expectations.

  import Mox

  alias Rheo.Backend.Mock

  @doc false
  def stub_lifecycle! do
    stub(Mock, :child_spec, fn opts ->
      name = Keyword.fetch!(opts, :name)

      %{
        id: {Mock, name},
        start: {Agent, :start_link, [fn -> :ok end, [name: name]]},
        restart: :permanent,
        type: :worker
      }
    end)

    stub(Mock, :capabilities, fn ->
      Rheo.Backend.Capabilities.new(
        durable: false,
        distributed: false,
        partitions: true,
        contiguous_frontier: true,
        replay: true
      )
    end)

    stub(Mock, :ensure_indexes, fn _handle -> :ok end)
    stub(Mock, :ping, fn _handle -> :ok end)
    :ok
  end

  @doc false
  def start_opts!(prefix \\ "mox") do
    stub_lifecycle!()
    rheo = :"#{prefix}_#{System.unique_integer([:positive])}"
    handle = :"#{rheo}_backend"
    %{rheo: rheo, handle: handle, child: {Rheo, name: rheo, backend: {Mock, name: handle}}}
  end

  @doc false
  def sample_event(opts \\ []) do
    id = Keyword.get(opts, :id, "evt_#{System.unique_integer([:positive])}")
    stream = Keyword.get(opts, :stream, "s")
    partition = Keyword.get(opts, :partition, 0)
    sequence = Keyword.get(opts, :sequence, 1)

    %Rheo.Event{
      id: id,
      stream: stream,
      partition: partition,
      sequence: sequence,
      timestamp: ~U[2026-01-01 00:00:00.000Z],
      type: Keyword.get(opts, :type, "t"),
      payload: Keyword.get(opts, :payload, %{})
    }
  end

  @doc false
  def sample_lease(opts \\ []) do
    event = Keyword.get_lazy(opts, :event, fn -> sample_event(opts) end)
    lease_id = Keyword.get(opts, :lease_id, "lease_#{System.unique_integer([:positive])}")

    %Rheo.Lease{
      lease_id: lease_id,
      stream: event.stream,
      group: Keyword.get(opts, :group, "g"),
      event_id: event.id,
      event: event,
      consumer_id: Keyword.get(opts, :consumer_id, "c1"),
      attempt: Keyword.get(opts, :attempt, 1),
      leased_at: ~U[2026-01-01 00:00:00.000Z],
      expires_at: ~U[2026-01-01 00:00:30.000Z],
      receipt: Keyword.get(opts, :receipt, lease_id)
    }
  end
end
