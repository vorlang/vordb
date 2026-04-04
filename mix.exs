defmodule VorDB.MixProject do
  use Mix.Project

  def project do
    [
      app: :vordb,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: Mix.compilers() ++ [:vor],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {VorDB.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:vor, path: "../vor"},
      {:rocksdb, "~> 1.8"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:stream_data, "~> 1.0", only: :test}
    ]
  end

  defp aliases do
    [
      test: "test --no-start"
    ]
  end
end
