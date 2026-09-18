# Optional integration: compiled only when `:redix` is present.
if Code.ensure_loaded?(Redix) do
  defmodule Rheo.Backend.Redis.Keys do
    @moduledoc """
    Redis key naming for `Rheo.Backend.Redis` (ADR 026).

    Every key is namespaced with `rheo:{name}:` where `name` is the Redix
    process name that serves as the backend handle, so two Rheo instances may
    share one Redis database.

    | Helper | Type | Purpose |
    |---|---|---|
    | `meta/2` | HASH | `partition_count` |
    | `sequence/3` | STRING | monotonic logical sequence counter |
    | `index/3` | ZSET | `sequence` score → stream entry id |
    | `events/3` | STREAM | immutable events |
    | `group_meta/3` | HASH | `max_attempts`, frontier / cursor per partition |
    | `fence/4` | HASH | active claim: `lease_id`, `attempt`, expiry |
    | `settled/4` | ZSET | settled sequences awaiting frontier collapse |
    | `dlq/3` | STREAM | rejected deliveries |
    """

    @doc """
    Key prefix for a backend handle.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.prefix(MyApp.Redis)
        "rheo:MyApp.Redis:"
    """
    @spec prefix(atom() | String.t()) :: String.t()
    def prefix(name), do: "rheo:" <> namespace(name) <> ":"

    @doc """
    Stream registry hash (`partition_count`).

    ## Examples

        iex> Rheo.Backend.Redis.Keys.meta(MyApp.Redis, "market")
        "rheo:MyApp.Redis:meta:market"
    """
    @spec meta(atom() | String.t(), String.t()) :: String.t()
    def meta(name, stream), do: prefix(name) <> "meta:" <> stream

    @doc """
    Per-partition logical sequence counter.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.sequence(MyApp.Redis, "market", 1)
        "rheo:MyApp.Redis:seq:market:1"
    """
    @spec sequence(atom() | String.t(), String.t(), non_neg_integer()) :: String.t()
    def sequence(name, stream, partition),
      do: prefix(name) <> "seq:" <> stream <> ":" <> part(partition)

    @doc """
    Sorted set mapping logical sequence to stream entry id.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.index(MyApp.Redis, "market", 0)
        "rheo:MyApp.Redis:z:market:0"
    """
    @spec index(atom() | String.t(), String.t(), non_neg_integer()) :: String.t()
    def index(name, stream, partition),
      do: prefix(name) <> "z:" <> stream <> ":" <> part(partition)

    @doc """
    Redis STREAM holding one partition's events.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.events(MyApp.Redis, "market", 0)
        "rheo:MyApp.Redis:s:market:0"
    """
    @spec events(atom() | String.t(), String.t(), non_neg_integer()) :: String.t()
    def events(name, stream, partition),
      do: prefix(name) <> "s:" <> stream <> ":" <> part(partition)

    @doc """
    Consumer group registry hash.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.group_meta(MyApp.Redis, "market", "risk")
        "rheo:MyApp.Redis:gmeta:market:risk"
    """
    @spec group_meta(atom() | String.t(), String.t(), String.t()) :: String.t()
    def group_meta(name, stream, group),
      do: prefix(name) <> "gmeta:" <> stream <> ":" <> group

    @doc """
    Fencing hash for one claimed entry.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.fence(MyApp.Redis, "market", "risk", "1700-0")
        "rheo:MyApp.Redis:fence:market:risk:1700-0"
    """
    @spec fence(atom() | String.t(), String.t(), String.t(), String.t()) :: String.t()
    def fence(name, stream, group, entry_id),
      do: prefix(name) <> "fence:" <> stream <> ":" <> group <> ":" <> entry_id

    @doc """
    `SCAN`/`KEYS` pattern matching every fence of one group.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.fence_pattern(MyApp.Redis, "market", "risk")
        "rheo:MyApp.Redis:fence:market:risk:*"
    """
    @spec fence_pattern(atom() | String.t(), String.t(), String.t()) :: String.t()
    def fence_pattern(name, stream, group),
      do: fence(name, stream, group, "*")

    @doc """
    Sorted set of settled (acked / rejected) sequences pending frontier collapse.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.settled(MyApp.Redis, "market", "risk", 0)
        "rheo:MyApp.Redis:done:market:risk:0"
    """
    @spec settled(atom() | String.t(), String.t(), String.t(), non_neg_integer()) :: String.t()
    def settled(name, stream, group, partition),
      do: prefix(name) <> "done:" <> stream <> ":" <> group <> ":" <> part(partition)

    @doc """
    Dead-letter stream for one group.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.dlq(MyApp.Redis, "market", "risk")
        "rheo:MyApp.Redis:dlq:market:risk"
    """
    @spec dlq(atom() | String.t(), String.t(), String.t()) :: String.t()
    def dlq(name, stream, group), do: prefix(name) <> "dlq:" <> stream <> ":" <> group

    @doc """
    Group meta field holding a partition's committed ACK frontier.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.frontier_field(3)
        "frontier_p3"
    """
    @spec frontier_field(non_neg_integer()) :: String.t()
    def frontier_field(partition), do: "frontier_p" <> part(partition)

    @doc """
    Group meta field holding a partition's last delivered sequence.

    ## Examples

        iex> Rheo.Backend.Redis.Keys.cursor_field(3)
        "cursor_p3"
    """
    @spec cursor_field(non_neg_integer()) :: String.t()
    def cursor_field(partition), do: "cursor_p" <> part(partition)

    defp part(partition) when is_integer(partition), do: Integer.to_string(partition)

    defp namespace(name) when is_atom(name) do
      name |> Atom.to_string() |> String.replace_prefix("Elixir.", "")
    end

    defp namespace(name) when is_binary(name), do: name
    defp namespace(name), do: inspect(name)
  end
end
