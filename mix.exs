defmodule ReqBigQuery.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/livebook-dev/req_bigquery"

  def project do
    [
      app: :req_bigquery,
      version: @version,
      elixir: "~> 1.12",
      preferred_cli_env: [
        "test.all": :test,
        docs: :docs,
        "hex.publish": :docs
      ],
      start_permanent: Mix.env() == :prod,
      docs: docs(),
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md"]
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
