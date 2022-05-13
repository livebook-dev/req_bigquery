defmodule ReqBigQueryTest do
  use ExUnit.Case, async: true
  use Plug.Test

  test "it works", ctx do
    fake_goth = fn conn ->
      data = %{access_token: "dummy", expires_in: 3599, token_type: "Bearer"}

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode_to_iodata!(data))
    end

    start_supervised!(
      {Goth,
       name: ctx.test,
       source: {:service_account, goth_credentials(), []},
       http_client: {&Req.request/1, plug: fake_goth}}
    )

    fake_bigquery = fn conn ->
      assert {:ok, body, conn} = read_body(conn)

      assert Jason.decode!(body) == %{
               "defaultDataset" => %{"datasetId" => "my_awesome_dataset"},
               "query" => "select * from iris"
             }

      assert request_url(conn) ==
               "https://bigquery.googleapis.com/bigquery/v2/projects/my_awesome_project_id/queries"

      assert get_req_header(conn, "content-type") == ["application/json"]
      assert get_req_header(conn, "authorization") == ["Bearer dummy"]
      assert conn.method == "POST"

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

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode_to_iodata!(data))
    end

    opts = [goth: ctx.test, project_id: "my_awesome_project_id", dataset: "my_awesome_dataset"]

    assert response =
             Req.new(plug: fake_bigquery)
             |> ReqBigQuery.attach(opts)
             |> Req.post!(bigquery: "select * from iris")

    assert response.status == 200

    assert response.body == %{
             "jobReference" => %{"jobId" => "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX"},
             "kind" => "bigquery#queryResponse",
             "rows" => [
               %{"f" => [%{"v" => "1"}, %{"v" => "Ale"}]},
               %{"f" => [%{"v" => "2"}, %{"v" => "Wojtek"}]}
             ],
             "schema" => %{
               "fields" => [
                 %{"mode" => "NULLABLE", "name" => "id", "type" => "INTEGER"},
                 %{"mode" => "NULLABLE", "name" => "name", "type" => "STRING"}
               ]
             },
             "totalRows" => "2"
           }
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
