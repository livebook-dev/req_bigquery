defmodule ReqBigQuery.MixProject do
  use Mix.Project

  def project do
    [
      app: :req_bigquery,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:req, github: "wojtekmach/req"},
      {:goth, github: "peburrows/goth"},
      {:plug, "~> 1.13", only: :test},
      {:table, "~> 0.1.1", optional: true}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end
end
