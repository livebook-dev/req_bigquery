defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  test "returns the Google BigQuery's response", %{test: goth} do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    query = """
    SELECT title, SUM(views) AS views
      FROM `bigquery-public-data.wikipedia.table_bands`
     WHERE EXTRACT(YEAR FROM datehour) <= 2021
     GROUP BY title
     ORDER BY views DESC
     LIMIT 10
    """

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth)
      |> Req.post!(bigquery: query)

    assert response.status == 200

    result = response.body

    assert result.columns == ["title", "views"]
    assert result.num_rows == 10
    rows = result.rows |> Enum.to_list()

    assert rows == [
             ["The_Beatles", 13_758_950],
             ["Queen_(band)", 12_019_563],
             ["Pink_Floyd", 9_522_503],
             ["AC/DC", 8_972_364],
             ["Led_Zeppelin", 8_294_994],
             ["Linkin_Park", 8_242_802],
             ["The_Rolling_Stones", 7_825_952],
             ["Red_Hot_Chili_Peppers", 7_302_904],
             ["Fleetwood_Mac", 7_199_563],
             ["Twenty_One_Pilots", 6_970_692]
           ]
  end

  test "requests new pages and brings all the rows when max_results (paging) < response num_rows",
       %{test: goth} do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])
    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    query = """
    SELECT *
      FROM `bigquery-public-data.wikipedia.table_bands`
      ORDER BY datehour asc
      LIMIT 10
    """

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth, max_results: 2)
      |> Req.post!(bigquery: query)

    result = response.body

    assert %Stream{} = result.rows

    assert Enum.take(result.rows, 4) == [
             [~U[2015-05-01 01:00:00.000000Z], "Butter_08", 1],
             [~U[2015-05-01 01:00:00.000000Z], "The_Pipkins", 1],
             [~U[2015-05-01 01:00:00.000000Z], "Project_One", 1],
             [~U[2015-05-01 01:00:00.000000Z], "Gatibu", 1]
           ]
  end

  test "returns the Google BigQuery's response without rows", %{test: goth} do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    query = """
    SELECT title, SUM(views) AS views
      FROM `bigquery-public-data.wikipedia.table_bands`
     WHERE EXTRACT(YEAR FROM datehour) > EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
     GROUP BY title
     ORDER BY views DESC
    """

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth)
      |> Req.post!(bigquery: query)

    assert response.status == 200

    result = response.body

    assert result.columns == ["title", "views"]
    assert result.num_rows == 0
    assert result.rows == []
  end

  test "returns the Google BigQuery's response with parameterized query", %{test: goth} do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    query = """
    SELECT EXTRACT(YEAR FROM datehour) AS year, SUM(views) AS views
      FROM `bigquery-public-data.wikipedia.table_bands`
     WHERE EXTRACT(YEAR FROM datehour) <= 2021
       AND title = ?
     GROUP BY year
     ORDER BY views DESC
    """

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth)
      |> Req.post!(bigquery: {query, ["Linkin_Park"]})

    assert response.status == 200

    result = response.body

    assert result.columns == ["year", "views"]
    assert result.num_rows == 7

    assert Enum.to_list(result.rows) == [
             [2017, 2_895_889],
             [2016, 1_173_359],
             [2018, 1_133_770],
             [2020, 906_538],
             [2015, 860_899],
             [2019, 790_747],
             [2021, 481_600]
           ]
  end

  test "returns the Google BigQuery's response with more than one parameterized query", %{
    test: goth
  } do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    query = """
    SELECT en_description
      FROM `bigquery-public-data.wikipedia.wikidata`
     WHERE id = ?
       AND numeric_id = ?
    """

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth)
      |> Req.post!(bigquery: {query, ["Q89", 89]})

    assert response.status == 200

    result = response.body

    assert result.columns == ["en_description"]
    assert result.num_rows == 1

    rows = Enum.to_list(result.rows)
    assert rows == [["fruit of the apple tree"]]
  end

  test "encodes and decodes types received from Google BigQuery's response", %{
    test: goth
  } do
    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    project_id = System.get_env("PROJECT_ID", credentials["project_id"])

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    req = Req.new() |> ReqBigQuery.attach(project_id: project_id, goth: goth)

    value = Decimal.new("1.1")

    assert run_decoding_query(req, value) == [[value]]

    decimal = Decimal.new("1.10")

    assert run_decoding_query(req, decimal) == [[value]]

    value = Decimal.new("-1.1")

    assert run_decoding_query(req, value) == [[value]]

    value = "req"

    assert run_decoding_query(req, value) == [[value]]

    value = 1

    assert run_decoding_query(req, value) == [[value]]

    value = 1.1

    assert run_decoding_query(req, value) == [[value]]

    value = -1.1

    assert run_decoding_query(req, value) == [[value]]

    value = true

    assert run_decoding_query(req, value) == [[value]]

    value = String.to_float("1.175494351E-38")

    assert run_decoding_query(req, value) == [[value]]

    value = String.to_float("3.402823466E+38")

    assert run_decoding_query(req, value) == [[value]]

    value = Date.utc_today()

    assert run_decoding_query(req, value) == [[value]]

    value = Time.utc_now()

    assert run_decoding_query(req, value) == [[value]]

    value = NaiveDateTime.utc_now()

    assert run_decoding_query(req, value) == [[value]]

    value = DateTime.utc_now()

    assert run_decoding_query(req, value) |> Enum.to_list() == [[value]]

    value = %{"id" => 1}

    assert run_custom_query(req, "SELECT STRUCT(1 AS id)") == [[value]]

    value = %{"ids" => [10, 20]}

    assert run_custom_query(req, "SELECT STRUCT([10,20] AS ids)") == [[value]]

    assert_raise RuntimeError, "float value \"-Infinity\" is not supported", fn ->
      run_custom_query(req, "SELECT CAST('-inf' AS FLOAT64)")
    end

    assert_raise RuntimeError, "float value \"Infinity\" is not supported", fn ->
      run_custom_query(req, "SELECT CAST('+inf' AS FLOAT64)")
    end

    assert_raise RuntimeError, "float value \"NaN\" is not supported", fn ->
      run_custom_query(req, "SELECT CAST('NaN' AS FLOAT64)")
    end
  end

  defp run_decoding_query(req, input) do
    result = Req.post!(req, bigquery: {"SELECT ?", [input]}).body
    Enum.to_list(result.rows)
  end

  defp run_custom_query(req, query) do
    result = Req.post!(req, bigquery: query).body
    Enum.to_list(result.rows)
  end
end
