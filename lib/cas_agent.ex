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
    Agent.start_link(fn -> [] end, name: :CAS_agent)
    :ok
  end

  @doc """
    Returns %{v: value, ts: timestamp}, being `value` the value for the given
    key from the ETS table, and `timestamp` the last modification time.
  """
  def read(key) do
    [{_, data}] = :ets.lookup(:CAS_table, key)
    data
  end

  @doc """
    Tries a Compare and Set sequence, setting the value for the given key
    with the return value of the given function. The function is passed the
    previous value.

    It returns {:ok, %{v: value, ts: timestamp}} if it worked within the given
    number of retries. {:error, :failed_cas} otherwise.
  """
  def cas(key, fun, retries \\ 3) do
    %{v: value, ts: ts} = read(key)

    case write(key, ts, fun) do
      {:ok, res} -> {:ok, res}
      {:error, :wrong_ts} when retries > 0 -> cas(key, fun, retries - 1)
      _ -> {:error, :failed_cas}
    end
  end

  @doc """
    Updates the value for the given key using the value returned by the given
    function. The function will be passed the current value.
    It returrns {:ok, %{v: value, ts: timestamp}} if it worked.
    The given `prev_ts` must match the current value of `ts` on table at the
    moment of writing. `{:error, :wrong_ts}` is otherwise returned.
  """
  def write(key, prev_ts, fun) do
    Agent.get_and_update :CAS_agent, fn(_)->
      # use the Agent process to serialize operations

      %{v: value, ts: ts} = read(key)

      if prev_ts == ts do
        new_value = fun.(value)
        data = %{v: new_value, ts: new_ts}
        true = :ets.insert(:CAS_table, {key, data})
        {{:ok, data}, []}
      else
        {{:error, :wrong_ts}, []}
      end
    end
  end

  @doc """
    Get timestamp in seconds, microseconds, or nanoseconds
  """
  def new_ts(scale \\ :seconds) do
    {mega, sec, micro} = :os.timestamp
    t = mega * 1_000_000 + sec
    case scale do
      :seconds -> t
      :micro -> t * 1_000_000 + micro
      :nano -> (t * 1_000_000 + micro) * 1_000
    end
  end

end
