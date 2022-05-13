defmodule IntegrationTest do
  use ExUnit.Case
  @moduletag :integration

  test "it works" do
    project_id = System.fetch_env!("PROJECT_ID")

    credentials =
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
      |> File.read!()
      |> Jason.decode!()

    source = {:service_account, credentials, []}
    assert {:ok, _} = Goth.start_link(name: MyGoth, source: source, http_client: &Req.request/1)

    assert %Req.Response{body: %ReqBigQuery.Result{} = result} =
             Req.new()
             |> ReqBigQuery.attach(project_id: project_id, dataset: "livebook", goth: MyGoth)
             |> Req.post!(bigquery: "SELECT sepal_length, sepal_width FROM iris LIMIT 2")

    assert result.num_rows == 2
    assert result.columns == ["sepal_length", "sepal_width"]
    assert [[x1, y1], [x2, y2]] = result.rows
    assert is_float(x1)
    assert is_float(x2)
    assert is_float(y1)
    assert is_float(y2)
  end
end
