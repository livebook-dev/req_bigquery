defmodule ReqBigQuery.MixProject do
  use Mix.Project

  def project do
    [
      app: :req_bigquery,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:goth, github: "peburrows/goth", optional: true}
    ]
  end
end
