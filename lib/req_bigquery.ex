defmodule ReqBigQuery do
  @moduledoc false

  alias Req.Request

  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  # TODO: Add Req.Request.register_options/2
  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(bigquery_run: &run/1)
    |> Request.register_option(:goth)
    |> Request.register_option(:dataset)
    |> Request.register_option(:project_id)
    |> Request.register_option(:bigquery)
    |> Request.register_option(:bigquery_params)
    |> merge_options(options)
  end

  # TODO: Add Req.Request.merge_options/2
  defp merge_options(request, options) do
    Map.update!(request, :options, fn opts ->
      options
      |> Enum.into(%{})
      |> Map.merge(opts)
    end)
  end

  defp run(%Request{} = request) do
    request
    |> put_url()
    |> put_goth_token()
    |> put_encoded_body()
  end

  defp put_url(%{options: options} = request) do
    %{request | url: URI.parse("#{@base_url}/projects/#{options.project_id}/queries")}
  end

  # TODO: Add Req.Request.put_header/3
  defp put_goth_token(%{options: options} = request) do
    token = Goth.fetch!(options.goth).token

    update_in(request.headers, &[{"authorization", "Bearer #{token}"} | &1])
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
