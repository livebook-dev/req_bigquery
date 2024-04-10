defmodule ReqBigQuery.Result do
  @moduledoc """
  Result struct returned from any successful query.

  Its fields are:

    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
    * `total_bytes_processed` - The total number of bytes processed for the query;
    * `job_id` - The ID of the Google BigQuery's executed job. Returns nil for dry runs.
  """

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()] | binary()],
          num_rows: non_neg_integer(),
          total_bytes_processed: non_neg_integer(),
          job_id: binary() | nil
        }

  defstruct [:job_id, :total_bytes_processed, num_rows: 0, rows: [], columns: []]
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: ReqBigQuery.Result do
    def init(result) do
      {:rows, %{columns: result.columns, count: result.num_rows}, result.rows}
    end
  end
end
