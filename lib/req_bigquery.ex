defmodule ReqBigQuery do
  @moduledoc """
  `Req` plugin for [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/rest).

  ReqBigQuery makes it easy to make BigQuery queries. It uses `Goth` for authentication.
  Query results are decoded into the `ReqBigQuery.Result` struct.
  The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.
  """

  alias Req.Request
  alias ReqBigQuery.Result

  @allowed_options ~w(goth default_dataset_id project_id bigquery max_results use_legacy_sql timeout_ms)a
  @base_url "https://bigquery.googleapis.com/bigquery/v2"
  @max_results 10_000
  @use_legacy_sql false
  @timeout_ms 10_000

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

    * `:max_results` - Optional. Number of rows to be returned by BigQuery in each request (paging).
      The rows Stream can make multiple requests if `num_rows` returned is grather than `:max_results`.
      Defaults to 10000.

    * `:use_legacy_sql` - Optional. Specifies whether to use BigQuery's legacy SQL dialect for this query.
      If set to false, the query will use BigQuery's GoogleSQL: https://cloud.google.com/bigquery/sql-reference/
      The default value is false.

    * `:timeout_ms` - Optional. How long to wait for the query to complete, in milliseconds, before the request times out and returns.
      Note: The call is not guaranteed to wait for the specified timeout. If the query takes longer to run than the timeout value,
      the call returns without any results and with the 'jobComplete' flag set to false. You can call GetQueryResults() to wait for the query to complete and read the results.
      The default value is 10000 milliseconds (10 seconds).

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
      iex> res = Req.post!(req, bigquery: query).body
      iex> res
      %ReqBigQuery.Result{
        columns: ["title", "views"],
        job_id: "job_JDDZKquJWkY7x0LlDcmZ4nMQqshb",
        num_rows: 10,
        rows: %Stream{}
      }
      iex> Enum.to_list(res.rows)
      [
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
      iex> res = Req.post!(req, bigquery: {query, ["Linkin_Park"]}).body
      %ReqBigQuery.Result{
        columns: ["year", "views"],
        job_id: "job_GXiJvALNsTAoAOJ39Eg3Mw94XMUQ",
        num_rows: 7,
        rows: %Stream{}
      }
      iex> Enum.to_list(res.rows)
      [[2017, 2895889], [2016, 1173359], [2018, 1133770], [2020, 906538], [2015, 860899], [2019, 790747], [2021, 481600]]

  """
  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    checked_options =
      options
      |> Keyword.put_new(:base_url, @base_url)
      |> Keyword.put_new(:max_results, @max_results)
      |> Keyword.put_new(:use_legacy_sql, @use_legacy_sql)
      |> Keyword.put_new(:timeout_ms, @timeout_ms)

    request
    |> Request.prepend_request_steps(bigquery_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(checked_options)
  end

  defp run(%Request{options: options} = request) do
    if query = options[:bigquery] do
      goth = options[:goth] || raise ":goth is missing"
      project_id = options[:project_id] || raise ":project_id is missing"
      base_url = options[:base_url]
      token = Goth.fetch!(goth).token
      uri = URI.parse("#{base_url}/projects/#{project_id}/queries")

      json =
        query
        |> build_request_body(options[:default_dataset_id])
        |> Map.put(:maxResults, options[:max_results])
        |> Map.put(:useLegacySql, options[:use_legacy_sql])
        |> Map.put(:timeoutMs, options[:timeout_ms])

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
    %{query: query}
  end

  defp build_request_body(query, dataset) when is_binary(query) do
    %{defaultDataset: %{datasetId: dataset}, query: query}
  end

  defp decode({request, %{status: 200} = response}) do
    {request, update_in(response.body, &decode_body(&1, request.options))}
  end

  defp decode(any), do: any

  defp decode_body(
         %{
           "jobReference" => %{"jobId" => job_id},
           "kind" => "bigquery#queryResponse",
           "rows" => _rows,
           "schema" => %{"fields" => fields},
           "totalRows" => num_rows
         } = initial_response,
         request_options
       ) do
    %Result{
      job_id: job_id,
      num_rows: String.to_integer(num_rows),
      rows: initial_response |> rows_stream(request_options) |> decode_rows(fields),
      columns: decode_columns(fields)
    }
  end

  defp decode_body(
         %{
           "jobReference" => %{"jobId" => job_id},
           "kind" => "bigquery#queryResponse",
           "schema" => %{"fields" => fields},
           "totalRows" => num_rows
         },
         _request_options
       ) do
    %Result{
      job_id: job_id,
      num_rows: String.to_integer(num_rows),
      rows: [],
      columns: decode_columns(fields)
    }
  end

  defp rows_stream(initial_response, request_options) do
    Stream.unfold({:initial, initial_response}, fn
      {:initial, %{"rows" => rows} = initial_body} ->
        {rows, initial_body}

      %{
        "pageToken" => page_token,
        "jobReference" => %{"jobId" => job_id, "projectId" => project_id}
      } ->
        resp = page_request(request_options, project_id, job_id, page_token)
        {resp.body["rows"], resp.body}

      _end ->
        # last iteration didn't have pageToken
        nil
    end)
    |> Stream.flat_map(& &1)
  end

  defp page_request(options, project_id, job_id, page_token) do
    uri =
      URI.parse(
        "#{@base_url}/projects/#{project_id}/queries/#{job_id}?maxResults=#{options[:max_results]}&pageToken=#{page_token}"
      )

    token = Goth.fetch!(options[:goth]).token

    Req.new(url: uri)
    |> Request.merge_options(auth: {:bearer, token})
    |> Req.get!()
  end

  defp decode_rows(rows, fields) do
    Stream.map(rows, fn %{"f" => columns} ->
      Enum.with_index(columns, fn %{"v" => value}, index ->
        field = Enum.at(fields, index)
        decode_value(value, field)
      end)
    end)
  end

  defp decode_columns(fields) do
    Enum.map(fields, & &1["name"])
  end

  @decimal_types ~w(NUMERIC BIGNUMERIC)

  defp decode_value(nil, _), do: nil
  defp decode_value(%{"v" => value}, field), do: decode_value(value, field)

  @invalid_float_values ["-Infinity", "Infinity", "NaN"]

  defp decode_value(value, %{"type" => "FLOAT"}) when value in @invalid_float_values do
    raise "float value #{inspect(value)} is not supported"
  end

  defp decode_value(values, %{"mode" => "REPEATED"} = field) do
    Enum.map(values, &decode_value(&1, Map.delete(field, "mode")))
  end

  defp decode_value(value, %{"type" => "FLOAT"}), do: String.to_float(value)
  defp decode_value(value, %{"type" => "INTEGER"}), do: String.to_integer(value)

  defp decode_value(value, %{"type" => type}) when type in @decimal_types,
    do: Decimal.new(value)

  defp decode_value("true", %{"type" => "BOOLEAN"}), do: true
  defp decode_value("false", %{"type" => "BOOLEAN"}), do: false

  defp decode_value(value, %{"fields" => fields, "type" => "RECORD"}) do
    decode_record(value, fields)
  end

  defp decode_value(value, %{"type" => "DATE"}), do: Date.from_iso8601!(value)

  defp decode_value(value, %{"type" => "DATETIME"}),
    do: NaiveDateTime.from_iso8601!(value)

  defp decode_value(value, %{"type" => "TIME"}), do: Time.from_iso8601!(value)

  defp decode_value(value, %{"type" => "TIMESTAMP"}) do
    float = String.to_float(value)
    DateTime.from_unix!(round(float * 1_000_000), :microsecond)
  end

  defp decode_value(value, _), do: value

  defp decode_record(values, fields) when is_list(values) do
    Enum.map(values, &decode_record(&1, fields))
  end

  defp decode_record(%{"f" => columns}, fields) do
    for {%{"v" => value}, index} <- Enum.with_index(columns), into: %{} do
      field = Enum.at(fields, index)

      {field["name"], decode_value(value, field)}
    end
  end

  defp encode_value(%DateTime{time_zone: "Etc/UTC"} = datetime) do
    naive_datetime = DateTime.to_naive(datetime)
    {to_string(naive_datetime), "TIMESTAMP"}
  end

  defp encode_value(%Date{} = date), do: {to_string(date), "DATE"}
  defp encode_value(%Time{} = time), do: {to_string(time), "TIME"}
  defp encode_value(%NaiveDateTime{} = timestamp), do: {to_string(timestamp), "DATETIME"}
  defp encode_value(%Decimal{} = decimal), do: {to_string(decimal), "BIGNUMERIC"}

  defp encode_value(value) when is_boolean(value), do: {value, "BOOL"}
  defp encode_value(value) when is_float(value), do: {value, "FLOAT"}
  defp encode_value(value) when is_integer(value), do: {value, "INTEGER"}
  defp encode_value(value), do: {value, "STRING"}
end
