defmodule Mix.Tasks.Rheo.InspectOpts do
  @moduledoc false

  @switches [rheo: :string, limit: :integer, after: :string]

  @doc false
  def parse(args) do
    {parsed, _argv, _invalid} = OptionParser.parse(args, strict: @switches)
    to_rheo_opts(parsed)
  end

  @doc false
  def parse_with_argv(args) do
    {parsed, argv, _invalid} = OptionParser.parse(args, strict: @switches)
    {to_rheo_opts(parsed), argv, []}
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
end
