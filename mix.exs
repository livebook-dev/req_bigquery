defmodule ReqBigQuery.MixProject do
  use Mix.Project

  @version "0.1.3"
  @description "Req plugin for Google BigQuery"

  def project do
    [
      app: :req_bigquery,
      version: @version,
      description: @description,
      name: "ReqBigQuery",
      elixir: "~> 1.12",
      preferred_cli_env: [
        "test.all": :test,
        docs: :docs,
        "hex.publish": :docs
      ],
      start_permanent: Mix.env() == :prod,
      docs: docs(),
      deps: deps(),
      aliases: aliases(),
      package: package()
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
      source_url: "https://github.com/livebook-dev/req_bigquery",
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end

  defp deps do
    [
      {:decimal, "~> 2.0"},
      {:req, "~> 0.3.5 or ~> 0.4"},
      {:goth, "~> 1.3"},
      {:table, "~> 0.1.1", optional: true},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/livebook-dev/req_bigquery"
      }
    ]
  end
end
