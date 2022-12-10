defmodule ReqBigQueryTest do
  use ExUnit.Case, async: true

  test "executes a query string", ctx do
    fake_goth = fn request ->
      data = %{access_token: "dummy", expires_in: 3599, token_type: "Bearer"}
      {request, Req.Response.json(data)}
    end

    start_supervised!(
      {Goth,
       name: ctx.test,
       source: {:service_account, goth_credentials(), []},
       http_client: {&Req.request/1, adapter: fake_goth}}
    )

    fake_bigquery = fn request ->
      assert Jason.decode!(request.body) == %{
               "defaultDataset" => %{"datasetId" => "my_awesome_dataset"},
               "query" => "select * from iris",
               "maxResults" => 10000
             }

      assert URI.to_string(request.url) ==
               "https://bigquery.googleapis.com/bigquery/v2/projects/my_awesome_project_id/queries"

      assert Req.Request.get_header(request, "content-type") == ["application/json"]
      assert Req.Request.get_header(request, "authorization") == ["Bearer dummy"]

      data = %{
        jobReference: %{jobId: "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX"},
        kind: "bigquery#queryResponse",
        rows: [
          %{f: [%{v: "1"}, %{v: "Ale"}]},
          %{f: [%{v: "2"}, %{v: "Wojtek"}]}
        ],
        schema: %{
          fields: [
            %{mode: "NULLABLE", name: "id", type: "INTEGER"},
            %{mode: "NULLABLE", name: "name", type: "STRING"}
          ]
        },
        totalRows: "2"
      }

      {request, Req.Response.json(data)}
    end

    opts = [
      goth: ctx.test,
      project_id: "my_awesome_project_id",
      default_dataset_id: "my_awesome_dataset"
    ]

    assert response =
             Req.new(adapter: fake_bigquery)
             |> ReqBigQuery.attach(opts)
             |> Req.post!(bigquery: "select * from iris")

    assert response.status == 200

    assert %ReqBigQuery.Result{
             columns: ["id", "name"],
             job_id: "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX",
             num_rows: 2,
             rows: %Stream{}
           } = response.body

    assert Enum.to_list(response.body.rows) == [[1, "Ale"], [2, "Wojtek"]]
  end

  test "executes a parameterized query", ctx do
    fake_goth = fn request ->
      data = %{access_token: "dummy", expires_in: 3599, token_type: "Bearer"}
      {request, Req.Response.json(data)}
    end

    start_supervised!(
      {Goth,
       name: ctx.test,
       source: {:service_account, goth_credentials(), []},
       http_client: {&Req.request/1, adapter: fake_goth}}
    )

    fake_bigquery = fn request ->
      assert Jason.decode!(request.body) == %{
               "defaultDataset" => %{"datasetId" => "my_awesome_dataset"},
               "query" => "select * from iris where sepal_width = ?",
               "parameterMode" => "POSITIONAL",
               "queryParameters" => [
                 %{
                   "parameterType" => %{"type" => "FLOAT"},
                   "parameterValue" => %{"value" => 10.4}
                 }
               ],
               "useLegacySql" => false,
               "maxResults" => 10000
             }

      assert URI.to_string(request.url) ==
               "https://bigquery.googleapis.com/bigquery/v2/projects/my_awesome_project_id/queries"

      assert Req.Request.get_header(request, "content-type") == ["application/json"]
      assert Req.Request.get_header(request, "authorization") == ["Bearer dummy"]

      data = %{
        jobReference: %{jobId: "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX"},
        kind: "bigquery#queryResponse",
        rows: [
          %{f: [%{v: "1"}, %{v: "Ale"}]},
          %{f: [%{v: "2"}, %{v: "Wojtek"}]}
        ],
        schema: %{
          fields: [
            %{mode: "NULLABLE", name: "id", type: "INTEGER"},
            %{mode: "NULLABLE", name: "name", type: "STRING"}
          ]
        },
        totalRows: "2"
      }

      {request, Req.Response.json(data)}
    end

    opts = [
      goth: ctx.test,
      project_id: "my_awesome_project_id",
      default_dataset_id: "my_awesome_dataset"
    ]

    assert response =
             Req.new(adapter: fake_bigquery)
             |> ReqBigQuery.attach(opts)
             |> Req.post!(bigquery: {"select * from iris where sepal_width = ?", [10.4]})

    assert response.status == 200

    assert %ReqBigQuery.Result{
             columns: ["id", "name"],
             job_id: "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX",
             num_rows: 2,
             rows: %Stream{}
           } = response.body

    assert Enum.to_list(response.body.rows) == [[1, "Ale"], [2, "Wojtek"]]
  end

  defp goth_credentials do
    private_key = :public_key.generate_key({:rsa, 2048, 65_537})

    encoded_private_key =
      :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, private_key)])

    %{
      "private_key" => encoded_private_key,
      "client_email" => "alice@example.com",
      "token_uri" => "/"
    }
  end
end
