# ReqBigQuery

[Req](https://github.com/wojtekmach/req) plugin for [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/rest).

ReqBigQuery handles a custom API to send queries to be executed by Google BigQuery and decodes into
a fancy result struct, which can be used by other libraries as they want.

## Usage

```elixir
Mix.install([
	{:goth, github: "peburrows/goth"},
  {:req, github: "wojtekmach/req"},
  {:req_bigquery, github: "livebook-dev/req_bigquery"}
])

# We use Goth to handle Google OAuth2 tokens
credentials = %{
	"type" => "service_account",
	"project_id" => "foo",
	"private_key_id" => "baz",
	...
}
source = {:source, credentials, []}
{:ok, _} = Goth.start_link(name: MyGoth, source: source, http_client: &Req.request/1)

req = Req.new() |> ReqBigQuery.attach(goth: MyGoth, project_id: "foo", dataset: "bar")
Req.post!(req, bigquery: "SELECT * FROM my_table LIMIT 2").body
=>
# %ReqBigQuery.Result{
#   columns: ["id", "name"],
#   job_id: "job_KuHEcplA2ICv8pSqb0QeOVNNpDaX",
#   num_rows: 2,
#   rows: [[1, "Ale"], [2, "Wojtek"]]
# }
```
