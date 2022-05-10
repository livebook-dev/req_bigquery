defmodule ReqBigQuery do
  # TODO: Add docs
  @moduledoc false

  alias Req.Request

  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  @doc false
  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(bigquery_run: &run/1)
    |> Request.prepend_response_steps(bigquery_parser: &parse/1)
    |> Request.register_option(:goth)
    |> Request.register_option(:dataset)
    |> Request.register_option(:project_id)
    |> Request.register_option(:bigquery)
    |> Request.register_option(:bigquery_params)
    |> merge_options(options)
  end

  defp merge_options(request, options) do
    Map.update!(request, :options, fn opts ->
      options
      |> Enum.into(%{})
      |> Map.merge(opts)
    end)
  end

  defp api_url(options) do
    "#{@base_url}/projects/#{options[:project_id]}/queries"
  end

  defp run(%Request{} = request) do
    request
    |> put_url()
    |> put_goth_token()
    |> put_encoded_body()
  end

  defp put_url(%{options: options} = request) do
    url = api_url(options)
    %{request | url: URI.parse(url)}
  end

  # TODO: Add Req.Request.put_header/3
  defp put_goth_token(%{options: options} = request) do
    goth = Map.fetch!(options, :goth)
    token = Goth.fetch!(goth).token

    update_in(request.headers, &[{"authorization", "Bearer #{token}"} | &1])
  end

  # TODO: Add Req.Request.put_body/2
  defp put_encoded_body(%{options: options} = request) do
    dataset = Map.fetch!(options, :dataset)
    query = Map.fetch!(options, :bigquery)

    json = %{
      defaultDataset: %{
        datasetId: dataset
      },
      query: query
    }

    update_in(request.body, fn _ -> Jason.encode!(json) end)
  end

  defp parser({request, response}) do
    {request, response}
  end
end
