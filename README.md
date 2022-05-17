# ReqBigQuery

[Req](https://github.com/wojtekmach/req) plugin for [Google BigQuery](https://cloud.google.com/bigquery/docs/reference/rest).

ReqBigQuery makes it easy to make BigQuery queries. It uses [Goth](https://github.com/peburrows/goth)
for authentication. Query results are decoded into the `ReqBigQuery.Result` struct.
The struct which implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.

## Usage

```elixir
Mix.install([
  {:goth, github: "peburrows/goth"},
  {:req, github: "wojtekmach/req"},
  {:req_bigquery, github: "livebook-dev/req_bigquery"}
])

# We use Goth to authenticate to Google Cloud API.
# See: https://hexdocs.pm/goth/1.3.0-rc.4/Goth.Token.html#fetch/1-source for more information.
credentials = File.read!("credentials.json") |> Jason.decode!()
source = {:service_account, credentials, []}
{:ok, _} = Goth.start_link(name: MyGoth, source: source, http_client: &Req.request/1)

project_id = System.fetch_env!("PROJECT_ID")

query = """
SELECT title, SUM(views) AS views
  FROM `bigquery-public-data.wikipedia.table_bands`
 WHERE EXTRACT(YEAR FROM datehour) <= 2021
 GROUP BY title
 ORDER BY views DESC
 LIMIT 10
"""

req = Req.new() |> ReqBigQuery.attach(goth: MyGoth, project_id: project_id)
Req.post!(req, bigquery: query).body
#=>
# %ReqBigQuery.Result{
#   columns: ["title", "views"],
#   job_id: "job_JDDZKquJWkY7x0LlDcmZ4nMQqshb",
#   num_rows: 10,
#   rows: [
#     ["The_Beatles", 13758950],
#     ["Queen_(band)", 12019563],
#     ["Pink_Floyd", 9522503],
#     ["AC/DC", 8972364],
#     ["Led_Zeppelin", 8294994],
#     ["Linkin_Park", 8242802],
#     ["The_Rolling_Stones", 7825952],
#     ["Red_Hot_Chili_Peppers", 7302904],
#     ["Fleetwood_Mac", 7199563],
#     ["Twenty_One_Pilots", 6970692]
#   ]
# }
```

## License

Copyright (C) 2022 Dashbit

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
