defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  test "returns the Google BigQuery's response", %{test: goth} do
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

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

    assert result.rows == [
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

  test "returns the Google BigQuery's response without rows", %{test: goth} do
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

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
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

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

    assert result.rows == [
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
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

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

    assert result.rows == [["fruit of the apple tree"]]
  end

  test "encodes and decodes types received from Google BigQuery's response", %{
    test: goth
  } do
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    req = Req.new() |> ReqBigQuery.attach(project_id: project_id, goth: goth)

    value = Decimal.new("1.1")
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    decimal = Decimal.new("1.10")
    assert Req.post!(req, bigquery: {"SELECT ?", [decimal]}).body.rows == [[value]]

    value = Decimal.new("-1.1")
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = "req"
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 1
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 1.1
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = -1.1
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = true
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = String.to_float("1.175494351E-38")
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = String.to_float("3.402823466E+38")
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = Date.utc_today()
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = Time.utc_now()
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = NaiveDateTime.utc_now()
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = DateTime.utc_now()
    assert Req.post!(req, bigquery: {"SELECT ?", [value]}).body.rows == [[value]]

    value = %{"id" => 1}
    assert Req.post!(req, bigquery: "SELECT STRUCT(1 AS id)").body.rows == [[value]]

    value = %{"ids" => [10, 20]}
    assert Req.post!(req, bigquery: "SELECT STRUCT([10,20] AS ids)").body.rows == [[value]]

    assert_raise RuntimeError, "float value \"-Infinity\" is not supported", fn ->
      Req.post!(req, bigquery: "SELECT CAST('-inf' AS FLOAT64)")
    end

    assert_raise RuntimeError, "float value \"Infinity\" is not supported", fn ->
      Req.post!(req, bigquery: "SELECT CAST('+inf' AS FLOAT64)")
    end

    assert_raise RuntimeError, "float value \"NaN\" is not supported", fn ->
      Req.post!(req, bigquery: "SELECT CAST('NaN' AS FLOAT64)")
    end
  end
end
