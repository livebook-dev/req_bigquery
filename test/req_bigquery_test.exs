defmodule ReqBigQueryTest do
  use ExUnit.Case, async: true
  import Plug.Conn

  test "it works", ctx do
    start_supervised!(Goth, goth_opts(ctx))

    req =
      Req.new(plug: &test_plug/1)
      |> ReqBigQuery.attach(
        goth: ctx.test,
        project_id: "my_awesome_project_id",
        dataset: "my_awesome_dataset"
      )

    assert %Req.Response{status: 200, body: body} = Req.post!(req, bigquery: "select * from iris")

    assert Jason.decode!(body) == %{
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

  defp test_plug(conn) do
    assert {:ok, body, conn} = read_body(conn)

    assert body ==
             ~s|{"defaultDataset":{"datasetId":"my_awesome_dataset"},"query":"select * from iris"}|

    assert get_req_header(conn, "authorization") == ["Bearer dummy"]
    assert conn.host == "bigquery.googleapis.com"
    assert conn.method == "POST"
    assert conn.request_path == "/bigquery/v2/projects/my_awesome_project_id/queries"

    send_resp(conn, 200, build_response())
  end

  defp build_response do
    json = %{
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

    Jason.encode!(json)
  end

  defp random_private_key do
    private_key = :public_key.generate_key({:rsa, 2048, 65_537})
    {:ok, private_key}
    :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, private_key)])
  end

  defp goth_opts(ctx) do
    credentials = %{
      "private_key" => random_private_key(),
      "client_email" => "alice@example.com",
      "token_uri" => "/"
    }

    opts = [
      name: ctx.test,
      source: {:service_account, credentials, []},
      http_client: &request_goth/1
    ]

    [start: {Goth, :start_link, [opts]}]
  end

  defp request_goth(_opts) do
    body = ~s|{"access_token":"dummy","expires_in":3599,"token_type":"Bearer"}|
    {:ok, %{status: 200, body: body}}
  end
end
