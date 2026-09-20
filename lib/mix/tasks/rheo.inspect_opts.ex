defmodule Mix.Tasks.Rheo.InspectOpts do
  @moduledoc false

  @switches [rheo: :string, limit: :integer, after: :string]

  @doc false
  def parse!(args, expected_argv, moduledoc) when is_integer(expected_argv) do
    {parsed, argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      Mix.raise("invalid option #{inspect(invalid)}")
    end

    if length(argv) != expected_argv do
      if is_binary(moduledoc) and moduledoc != "" do
        Mix.shell().info(String.trim(moduledoc))
      end

      Mix.raise("expected #{expected_argv} argument(s), got #{length(argv)}")
    end

    opts = to_rheo_opts(parsed)
    ensure_instance!(opts)
    {opts, argv}
  end

  defp to_rheo_opts(parsed) do
    []
    |> maybe_put(:rheo, parse_rheo(parsed[:rheo]))
    |> maybe_put(:limit, parsed[:limit])
    |> maybe_put(:after, parsed[:after])
  end

  defp parse_rheo(nil), do: nil

  defp parse_rheo(name) when is_binary(name) do
    Module.concat([name])
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp ensure_instance!(opts) do
    name = Keyword.get(opts, :rheo, Application.get_env(:rheo, :name, Rheo))

    unless is_pid(Process.whereis(Rheo.Names.instance(name))) do
      Mix.raise("""
      Rheo instance #{inspect(name)} is not running.

      Set `config :rheo, start_on_application: true` or pass `--rheo MyRheo` after starting that instance.
      """)
    end
  end
end
