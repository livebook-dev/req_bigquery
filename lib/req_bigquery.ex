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

  defp run(%Request{} = request) do
    request
    |> put_url()
    |> put_goth_token()
    |> put_encoded_body()
  end

  defp put_url(%{options: options} = request) do
    base_url = options[:base_url] || @base_url
    %{request | url: URI.parse("#{base_url}/projects/#{options.project_id}/queries")}
  end

  defp put_goth_token(%{options: options} = request) do
    token = Goth.fetch!(options.goth).token
    Request.put_header(request, "authorization", "Bearer #{token}")
  end

  defp put_encoded_body(%{options: options} = request) do
    json = %{
      defaultDataset: %{
        datasetId: options.dataset
      },
      query: options.bigquery
    }

    %{request | body: Jason.encode!(json)}
  end
end
