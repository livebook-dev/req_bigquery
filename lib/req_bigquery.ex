defmodule ReqBigQuery do
  @moduledoc """
  The Google BigQuery plugin for [Req](https://github.com/wojtekmach/req).

  ReqBigQuery uses [Goth](https://github.com/peburrows/goth) to generate the
  OAuth2 Token from Google Credentials, but we don't start the Goth server,
  we only retrieve the token from given Goth server name.

  This plugin also provides a `ReqBigQuery.Result` struct to normalize the
  result from Google BigQuery API and allows the result to be rendered by
  [table](https://github.com/dashbitco/table).

  ## Plugin Options

    * `:goth` - the goth server name.

    * `:project_id` - the project id from Google Cloud Platform.

    * `:dataset` - the dataset from Google BigQuery.

    * `:bigquery` - the statement to be executed by Google's API.

  Every option above is required by default. (for more information about the options, see attach/2)

  ## Examples

      iex> req = Req.new() |> ReqBigQuery.attach(goth: MyGoth, project_id: "foo", dataset: "bar")
      iex> Req.post!(req, bigquery: "SELECT * FROM iris LIMIT 5")
      %Req.Request{body: %ReqBigQuery.Result{}, status: 200}

  For more information and examples, please see our `Guides` page.
  """

  alias Req.Request
  alias ReqBigQuery.Result

  @allowed_options ~w(goth dataset project_id bigquery)a
  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  @doc """
  Attaches the steps from this plugin into an existing Req's request struct.

  It adds the request step to prepare Req to know what URL it should use,
  the request body and the authorization header with the token provided by Goth.
  It allows the user to configure Req with our allowed options (see section below).

  The response from Google's API will be decoded into `ReqBigQuery.Result` struct.

  ## Options

    * `:goth` - The provided name for `Goth` library, it should be an atom.

    * `:project_id` - the id from the project created on Google Cloud Platform,
      it should be a string.

    * `:dataset` - the dataset from Google BigQuery, allowing the plugin to execute
      the query in the correct dataset and it should be a string.

    * `:bigquery` - the query to be exexcuted by Google BigQuery, it should be a string.

  """
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

      Request.merge_options(%{request | url: uri}, auth: {:bearer, token}, json: json)
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
