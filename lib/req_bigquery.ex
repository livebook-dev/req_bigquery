defmodule ReqBigQuery do
  @moduledoc false

  alias Req.Request

  @allowed_options ~w(goth dataset project_id bigquery)a
  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(bigquery_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
  end

  defp run(%Request{options: options} = request) do
    if query = options[:bigquery] do
      base_url = options[:base_url] || @base_url
      token = Goth.fetch!(options.goth).token
      uri = URI.parse("#{base_url}/projects/#{options.project_id}/queries")

      json = %{
        defaultDataset: %{
          datasetId: options.dataset
        },
        query: query
      }

      %{request | url: uri}
      |> Request.merge_options(auth: {:bearer, token}, json: json)
      |> Request.put_header("content-type", "application/json")
    else
      request
    end
  end
end
