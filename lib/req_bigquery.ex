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
      a `{query, params}` tuple, where `query` can contain `?` placeholders and `params`
      is a list of corresponding values.

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
      iex> query = \"""
      ...> SELECT title, SUM(views) AS views
      ...>   FROM `bigquery-public-data.wikipedia.table_bands`
      ...>  WHERE EXTRACT(YEAR FROM datehour) <= 2021
      ...>  GROUP BY title
      ...>  ORDER BY views DESC
      ...>  LIMIT 10
      ...> \"""
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
      iex> query = \"""
      ...> SELECT EXTRACT(YEAR FROM datehour) AS year, SUM(views) AS views
      ...>   FROM `bigquery-public-data.wikipedia.table_bands`
      ...>  WHERE EXTRACT(YEAR FROM datehour) <= 2021
      ...>    AND title = ?
      ...>  GROUP BY 1
      ...>  ORDER BY views DESC
      ...> \"""
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
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
  end

  defp run(%Request{options: options} = request) do
    if query = options[:bigquery] do
      base_url = options[:base_url] || @base_url
      token = Goth.fetch!(options.goth).token
      uri = URI.parse("#{base_url}/projects/#{options.project_id}/queries")
      json = build_request_body(query, options[:default_dataset_id])

      %{request | url: uri}
      |> Request.merge_options(auth: {:bearer, token}, json: json)
      |> Request.append_response_steps(bigquery_decode: &decode/1)
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
      for value <- params do
        {value, type} = encode_value(value)
        %{parameterType: %{type: type}, parameterValue: %{value: value}}
      end

    Map.merge(map, %{
      queryParameters: query_params,
      useLegacySql: false,
      parameterMode: "POSITIONAL"
    })
  end

  defp build_request_body(query, dataset) when dataset in ["", nil] do
    %{query: query, useLegacySql: false}
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
        decode_value(value, field)
      end)
    end)
  end

  defp prepare_columns(fields) do
    Enum.map(fields, & &1["name"])
  end

  defp prepare_json_value(values = [_ | _], fields) do
    Enum.map(values, &prepare_json_value(&1, fields))
  end

  defp prepare_json_value(%{"f" => columns}, fields) do
    for {%{"v" => value}, index} <- Enum.with_index(columns), into: %{} do
      field = Enum.at(fields, index)

      {field["name"], decode_value(value, field)}
    end
  end

  @numeric_types ~w(INTEGER NUMERIC BIGNUMERIC)

  defp decode_value(nil, _), do: nil
  defp decode_value(%{"v" => value}, field), do: decode_value(value, field)

  defp decode_value(values, %{"mode" => "REPEATED"} = field) do
    Enum.map(values, &decode_value(&1, Map.delete(field, "mode")))
  end

  defp decode_value(value, %{"type" => "FLOAT"}), do: String.to_float(value)

  defp decode_value(value, %{"type" => type}) when type in @numeric_types,
    do: String.to_integer(value)

  defp decode_value("true", %{"type" => "BOOLEAN"}), do: true
  defp decode_value("false", %{"type" => "BOOLEAN"}), do: false

  defp decode_value(value, %{"fields" => fields, "type" => "RECORD"}) do
    prepare_json_value(value, fields)
  end

  defp decode_value(value, %{"type" => "DATE"}), do: Date.from_iso8601!(value)

  defp decode_value(value, %{"type" => "DATETIME"}),
    do: DateTime.from_iso8601(value <> "Z") |> elem(1)

  defp decode_value(value, %{"type" => "TIME"}), do: Time.from_iso8601!(value)

  @epoch_seconds :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  defp decode_value(value, %{"type" => "TIMESTAMP"}) do
    unix_time = String.to_float(value) |> floor()

    NaiveDateTime.from_gregorian_seconds(unix_time + @epoch_seconds)
  end

  defp decode_value(value, _), do: value

  defp encode_value(%DateTime{} = datetime) do
    string =
      datetime
      |> DateTime.truncate(:second)
      |> to_string()

    value = String.slice(string, 0..(String.length(string) - 2))

    {value, "DATETIME"}
  end

  defp encode_value(%Date{} = date), do: {to_string(date), "DATE"}
  defp encode_value(%Time{} = time), do: {to_string(time), "TIME"}
  defp encode_value(%NaiveDateTime{} = timestamp), do: {to_string(timestamp), "TIMESTAMP"}

  defp encode_value(value) when is_boolean(value), do: {value, "BOOL"}
  defp encode_value(value) when is_float(value), do: {value, "FLOAT"}
  defp encode_value(value) when is_integer(value), do: {value, "INTEGER"}
  defp encode_value(value), do: {value, "STRING"}
end
