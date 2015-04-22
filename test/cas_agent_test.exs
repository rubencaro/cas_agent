alias CasAgent, as: C

defmodule CasAgentTest do
  use ExUnit.Case

  setup_all do
    C.start
  end

  setup do
    :ets.delete_all_objects :CAS_table
    Agent.update :CAS_agent, fn(_)-> %{} end
  end

  test "it reads" do
    true = :ets.insert :CAS_table, {:a, %{v: 1, ts: 0}}
    assert C.read(:a) == %{v: 1, ts: 0}
  end

  test "it writes given a right ts" do
    true = :ets.insert :CAS_table, {:a, %{v: 1, ts: 0}}
    assert {:ok, _} = C.write(:a, 0, fn(_)-> 43 end)
    assert %{v: 43, ts: ts} = C.read(:a)
    assert ts > 0
  end

  test "it does not write given a wrong ts" do
    true = :ets.insert :CAS_table, {:a, %{v: 1, ts: 0}}
    assert {:error, :wrong_ts} = C.write(:a, 18, fn(_)-> 43 end)
    assert %{v: 1, ts: 0} = C.read(:a)
  end

  test "it does cas" do
    true = :ets.insert :CAS_table, {:a, %{v: 1, ts: 0}}
    assert {:ok, _} = C.cas(:a, fn(_)-> 43 end)
    assert %{v: 43, ts: ts} = C.read(:a)
    assert ts > 0
  end

  test "it does cas sequentially" do
    true = :ets.insert :CAS_table, {:a, %{v: "", ts: 0}}
    for i <- 0..9 do
      {:ok, _} = C.cas(:a, fn(s)-> s <> "#{i}" end)
    end
    assert %{v: "0123456789"} = C.read(:a)
  end

  test "it does handle concurrency" do
    true = :ets.insert :CAS_table, {:a, %{v: 0, ts: 0}}

    wt = fn ->
      :timer.sleep(:random.uniform(500))
      {:ok, _} = C.cas(:a, &(&1 + 1))
    end

    rd = fn ->
      :timer.sleep(:random.uniform(500))
      %{v: v} = C.read(:a)
      assert v >= 0 && v <= 10
    end

    ws = for i <- 0..9, into:[], do: Task.async(wt)
    rs = for i <- 0..9, into:[], do: Task.async(rd)

    for t <- ws ++ rs, do: t.await

    %{v: v} = C.read(:a)
    assert v == 10
  end
end
