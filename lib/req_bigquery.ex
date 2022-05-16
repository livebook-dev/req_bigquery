defmodule ReqBigQuery do
  @moduledoc """
  `Req` plugin for [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/rest).

  ReqBigQuery makes it easy to make BigQuery queries. It uses `Goth` for authentication.
  Query results are decoded into the `ReqBigQuery.Result` struct.
  The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.
  """

  alias Req.Request
  alias ReqBigQuery.Result

  @allowed_options ~w(goth default_dataset_id project_id bigquery)a
  @base_url "https://bigquery.googleapis.com/bigquery/v2"

  @doc """
  Attaches to Req request.

  ## Request Options

    * `:goth` - Required. The goth server name.

    * `:project_id` - Required. The GCP project id.

    * `:bigquery` - Required. The query to execute. It can be a plain sql string or
      a `{query, params}` tuple, when query is the plain sql string with positional
      query, using `?`. And `params` is a list of values which will be sent as parameterized
      query to Google BigQuery.

    * `:default_dataset_id` - Optional. If set, the dataset to assume for any unqualified table
      names in the query. If not set, all table names in the query string must be qualified in the
      format 'datasetId.tableId'.

  If you want to set any of these options when attaching the plugin, pass them as the second argument.

  ## Examples

  With plain query string:

      iex> credentials = File.read!("credentials.json") |> Jason.decode!()
      iex> source = {:service_account, credentials, []}
      iex> {:ok, _} = Goth.start_link(name: MyGoth, source: source, http_client: &Req.request/1)
      iex> project_id = System.fetch_env!("PROJECT_ID")
      iex> query = """
      ...> SELECT title, SUM(views) AS views
      ...>   FROM `bigquery-public-data.wikipedia.table_bands`
      ...>  WHERE EXTRACT(YEAR FROM datehour) <= 2021
      ...>  GROUP BY title
      ...>  ORDER BY views DESC
      ...>  LIMIT 10
      ...> """
      iex> req = Req.new() |> ReqBigQuery.attach(goth: MyGoth, project_id: project_id)
      iex> Req.post!(req, bigquery: query).body
      %ReqBigQuery.Result{
        columns: ["title", "views"],
        job_id: "job_JDDZKquJWkY7x0LlDcmZ4nMQqshb",
        num_rows: 10,
        rows: [
          ["The_Beatles", 13758950],
          ["Queen_(band)", 12019563],
          ["Pink_Floyd", 9522503],
          ["AC/DC", 8972364],
          ["Led_Zeppelin", 8294994],
          ["Linkin_Park", 8242802],
          ["The_Rolling_Stones", 7825952],
          ["Red_Hot_Chili_Peppers", 7302904],
          ["Fleetwood_Mac", 7199563],
          ["Twenty_One_Pilots", 6970692]
        ]
      }

  With parameterized query:

      iex> credentials = File.read!("credentials.json") |> Jason.decode!()
      iex> source = {:service_account, credentials, []}
      iex> {:ok, _} = Goth.start_link(name: MyGoth, source: source, http_client: &Req.request/1)
      iex> project_id = System.fetch_env!("PROJECT_ID")
      iex> query = """
      ...> SELECT EXTRACT(YEAR FROM datehour) AS year, SUM(views) AS views
      ...>   FROM `bigquery-public-data.wikipedia.table_bands`
      ...>  WHERE EXTRACT(YEAR FROM datehour) <= 2021
      ...>    AND title = ?
      ...>  GROUP BY 1
      ...>  ORDER BY views DESC
      ...>  LIMIT 10
      ...> """
      iex> req = Req.new() |> ReqBigQuery.attach(goth: MyGoth, project_id: project_id)
      iex> Req.post!(req, bigquery: {query, ["Linkin_Park"]}).body
      %ReqBigQuery.Result{
        columns: ["year", "views"],
        job_id: "job_GXiJvALNsTAoAOJ39Eg3Mw94XMUQ",
        num_rows: 7,
        rows: [[2017, 2895889], [2016, 1173359], [2018, 1133770], [2020, 906538], [2015, 860899], [2019, 790747], [2021, 481600]]
      }

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
      json = build_request_body(query, options[:default_dataset_id])

      Request.merge_options(%{request | url: uri}, auth: {:bearer, token}, json: json)
    else
      request
    end
  end

  defp build_request_body({query, []}, dataset) when is_binary(query) do
    build_request_body(query, dataset)
  end

  defp build_request_body({query, params}, dataset) when is_binary(query) do
    map = build_request_body(query, dataset)

    query_params =
      Enum.reduce(params, [], fn value, acc ->
        {type, value} =
          cond do
            is_boolean(value) -> {"BOOL", value}
            is_integer(value) -> {"INTEGER", value}
            is_float(value) -> {"FLOAT", value}
            true -> {"STRING", to_string(value)}
          end

        param = %{parameterType: %{type: type}, parameterValue: %{value: value}}
        [param | acc]
      end)

    Map.merge(map, %{
      queryParameters: Enum.reverse(query_params),
      useLegacySql: false,
      parameterMode: "POSITIONAL"
    })
  end

  defp build_request_body(query, dataset)
       when is_binary(query) and (dataset == "" or is_nil(dataset)) do
    %{query: query}
  end

  defp build_request_body(query, dataset) when is_binary(query) do
    %{defaultDataset: %{datasetId: dataset}, query: query}
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

  defp decode_body(%{
         "jobReference" => %{"jobId" => job_id},
         "kind" => "bigquery#queryResponse",
         "schema" => %{"fields" => fields},
         "totalRows" => num_rows
       }) do
    %Result{
      job_id: job_id,
      num_rows: String.to_integer(num_rows),
      rows: [],
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

  defp convert_value(nil, _), do: nil
  defp convert_value(value, %{"type" => "FLOAT"}), do: String.to_float(value)
  defp convert_value(value, %{"type" => "INTEGER"}), do: String.to_integer(value)
  defp convert_value(value, %{"type" => "BOOL"}), do: String.downcase(value) == "true"
  defp convert_value(value, _), do: value
end
