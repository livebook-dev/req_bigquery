defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  test "returns the Google BigQuery's API", %{test: goth} do
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    source = {:service_account, credentials, []}
    start_supervised!({Goth, name: goth, source: source, http_client: &Req.request/1})

    response =
      Req.new()
      |> ReqBigQuery.attach(project_id: project_id, goth: goth)
      |> Req.post!(
        bigquery: "SELECT id, text FROM [bigquery-public-data:hacker_news.full] LIMIT 2"
      )

    assert response.status == 200
    result = response.body
    assert %ReqBigQuery.Result{} = result
    assert result.num_rows == 2
    assert result.columns == ["id", "text"]
    assert [[x1, y1], [x2, y2]] = result.rows
    assert is_integer(x1)
    assert is_integer(x2)
    assert is_binary(y1)
    assert is_binary(y2)
  end
end
