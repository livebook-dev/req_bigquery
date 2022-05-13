defmodule ReqBigQuery.Result do
  @moduledoc """
  Result struct returned from any successful query.

  Its fields are:

    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `job_id` - The ID of the Google BigQuery's executed job.
  """

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()] | binary()],
          num_rows: non_neg_integer(),
          job_id: binary()
        }

  defstruct [:job_id, num_rows: 0, rows: [], columns: []]
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: ReqBigQuery.Result do
    def init(%{columns: columns}) when columns in [nil, []] do
      {:rows, %{columns: []}, []}
    end

    def init(result) do
      {:rows, %{columns: result.columns}, result.rows}
    end
  end
end
