defmodule CasAgent do

  @moduledoc """
    Implement Compare and Set using an ETS table to solve read concurrency
    and an Agent to control write concurrency (the actual CAS).
  """

  @doc """
    Start the ETS table and the Agent before anything
  """
  def start do
    :ets.new(:CAS_table, [:public,:set,:named_table])
    Agent.start_link(fn -> %{} end, name: :CAS_agent)
    :ok
  end

  @doc """
    Returns %{v: value, ts: timestamp}, being `value` the value for the given
    key from the ETS table, and `timestamp` the last modification time.
  """
  def read(key) do

  end

  @doc """
    Tries a Compare and Set sequence, setting the value for the given key
    with the return value of the given function. The function is passed the
    previous value.

    It returns {:ok, %{v: value, ts: timestamp}} if it worked within the given
    number of retries. {:error, :failed_cas} otherwise.
  """
  def cas(key,fun, retries \\ 3) do

  end

  @doc """
    Updates the value for the given key using the value returned by the given
    function. The function will be passed the current value.
    The given `prev_ts` must match the current value of `ts` on table.
    `{:error, :wrong_ts}` is otherwise returned.
  """
  def write(key, prev_ts, fun) do

  end

  @doc """
    Get timestamp in seconds, microseconds, or nanoseconds
  """
  def ts(scale \\ :seconds) do
    {mega, sec, micro} = :os.timestamp
    t = mega * 1_000_000 + sec
    case scale do
      :seconds -> t
      :micro -> t * 1_000_000 + micro
      :nano -> (t * 1_000_000 + micro) * 1_000
    end
  end

end
