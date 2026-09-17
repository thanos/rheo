defmodule RheoBroadway.MixProject do
  use Mix.Project

  @version "0.8.0"
  @source_url "https://github.com/thanos/rheo"

  def project do
    [
      app: :rheo_broadway,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: "GenStage producer and Broadway integration for Rheo",
      source_url: @source_url
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:rheo, path: "../..", env: Mix.env()},
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.2"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs)
    ]
  end
end
