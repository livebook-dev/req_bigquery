defmodule ReqBigQuery do
  @moduledoc false

  alias Req.Request
  alias ReqBigQuery.Result

  @allowed_options ~w(goth dataset project_id bigquery)a
  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(bigquery_run: &run/1)
    |> Request.append_response_steps(bigquery_decode: &decode/1)
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

  defp decode({request, %{status: 200} = response}) do
    {request, update_in(response.body, &decode_body/1)}
  end

  defp decode(any), do: any

  defp decode_body(%{
         "jobReference" => %{"jobId" => job_id},
         "kind" => "bigquery#queryResponse",
         "rows" => rows,
         "schema" => %{"fields" => fields},
         "totalRows" => num_rows
       }) do
    %Result{
      job_id: job_id,
      num_rows: String.to_integer(num_rows),
      rows: prepare_rows(rows, fields),
      columns: prepare_columns(fields)
    }
  end

  defp prepare_rows(rows, fields) do
    Enum.map(rows, fn %{"f" => columns} ->
      Enum.with_index(columns, fn %{"v" => value}, index ->
        field = Enum.at(fields, index)
        convert_value(value, field)
      end)
    end)
  end

  defp prepare_columns(fields) do
    Enum.map(fields, & &1["name"])
  end

  defp convert_value(value, %{"type" => "FLOAT"}), do: String.to_float(value)
  defp convert_value(value, %{"type" => "INTEGER"}), do: String.to_integer(value)
  defp convert_value(value, %{"type" => "BOOLEAN"}), do: String.downcase(value) == "true"
  defp convert_value(value, _), do: value
end
