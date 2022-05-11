defmodule ReqBigQueryTest do
  use ExUnit.Case, async: true

  setup do
    goth_bypass = Bypass.open()
    bq_bypass = Bypass.open()

    {:ok,
     goth_bypass: goth_bypass,
     bq_bypass: bq_bypass,
     goth_url: "http://localhost:#{goth_bypass.port}",
     bq_url: "http://localhost:#{bq_bypass.port}"}
  end

  test "request api", ctx do
    apply_goth_bypass(ctx.goth_bypass)
    apply_bigquery_bypass(ctx.bq_bypass)

    req = Req.new(base_url: ctx.bq_url)
    assert {:ok, _} = Goth.start_link(goth_opts(ctx))
    Goth.fetch!(ctx.test)

    req =
      ReqBigQuery.attach(req,
        goth: ctx.test,
        project_id: "foo",
        dataset: "bar"
      )

    assert %Req.Response{status: 200} = Req.post!(req, bigquery: "select * from table")
  end

  test "attach/2", ctx do
    apply_goth_bypass(ctx.goth_bypass)

    req = Req.new(base_url: ctx.bq_url)
    assert {:ok, _} = Goth.start_link(goth_opts(ctx))
    Goth.fetch!(ctx.test)

    req =
      ReqBigQuery.attach(req,
        goth: ctx.test,
        project_id: "foo",
        dataset: "bar"
      )

    assert %{
             goth: ctx.test,
             base_url: ctx.bq_url,
             dataset: "bar",
             project_id: "foo"
           } == req.options
  end

  test "build request body", ctx do
    apply_goth_bypass(ctx.goth_bypass)

    assert {:ok, _} = Goth.start_link(goth_opts(ctx))
    Goth.fetch!(ctx.test)

    opts = [goth: ctx.test, project_id: "foo", dataset: "bar", bigquery: "select * from table"]

    req =
      Req.new(base_url: ctx.bq_url)
      |> ReqBigQuery.attach(opts)
      |> Req.Request.prepare()

    path = "/projects/#{opts[:project_id]}/queries"

    assert %URI{path: ^path} = req.url

    assert ~s|{"defaultDataset":{"datasetId":"#{opts[:dataset]}"},"query":"#{opts[:bigquery]}"}| ==
             req.body
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

    [
      name: ctx.test,
      source: {:service_account, credentials, url: ctx.goth_url},
      http_client: &Req.request/1
    ]
  end

  defp apply_bigquery_bypass(bypass) do
    Bypass.expect(bypass, fn conn ->
      body = %{
        "jobReference" => %{
          "job_id" => "a big job id"
        },
        "kind" => "bigquery#queryRequest",
        "rows" => [
          %{"f" => [%{"v" => "foo"}]}
        ],
        "schema" => %{
          "fields" => [
            %{"mode" => "NULLABLE", "type" => "STRING", "name" => "bar"}
          ]
        },
        "numRows" => "1"
      }

      Plug.Conn.resp(conn, 200, Jason.encode!(body))
    end)
  end

  defp apply_goth_bypass(bypass) do
    Bypass.expect(bypass, fn conn ->
      body = ~s|{"access_token":"dummy","expires_in":3599,"token_type":"Bearer"}|
      Plug.Conn.resp(conn, 200, body)
    end)
  end
end
