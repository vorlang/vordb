defmodule VorDB.CounterTest do
  use ExUnit.Case, async: true

  alias VorDB.Counter

  test "empty counter has value 0" do
    assert Counter.value(Counter.empty()) == 0
  end

  test "increment increases value" do
    c = Counter.empty()
    c = Counter.increment(c, :n1, 1)
    c = Counter.increment(c, :n1, 1)
    c = Counter.increment(c, :n1, 1)
    assert Counter.value(c) == 3
  end

  test "increment by custom amount" do
    c = Counter.increment(Counter.empty(), :n1, 5)
    assert Counter.value(c) == 5
  end

  test "decrement decreases value" do
    c = Counter.empty()
    c = Counter.increment(c, :n1, 5)
    c = Counter.decrement(c, :n1, 2)
    assert Counter.value(c) == 3
  end

  test "decrement below zero" do
    c = Counter.decrement(Counter.empty(), :n1, 3)
    assert Counter.value(c) == -3
  end

  test "increment and decrement from different nodes" do
    c = Counter.empty()
    c = Counter.increment(c, :node_a, 10)
    c = Counter.decrement(c, :node_b, 3)
    assert Counter.value(c) == 7
  end

  test "merge takes max of each node's count" do
    a = Counter.increment(Counter.empty(), :n1, 5)
    b = Counter.increment(Counter.empty(), :n1, 3) |> Counter.increment(:n2, 7)
    merged = Counter.merge(a, b)
    assert Counter.value(merged) == 12
  end

  test "merge handles disjoint nodes" do
    a = Counter.increment(Counter.empty(), :n1, 5)
    b = Counter.increment(Counter.empty(), :n2, 3)
    merged = Counter.merge(a, b)
    assert Counter.value(merged) == 8
  end

  test "merge handles both p and n" do
    a = Counter.increment(Counter.empty(), :n1, 10) |> Counter.decrement(:n1, 2)
    b = Counter.increment(Counter.empty(), :n1, 8) |> Counter.decrement(:n1, 5)
    merged = Counter.merge(a, b)
    # p: max(10, 8) = 10, n: max(2, 5) = 5
    assert Counter.value(merged) == 5
  end

  test "merge_stores merges per-key" do
    store_a = %{"c1" => Counter.increment(Counter.empty(), :n1, 10)}
    store_b = %{
      "c1" => Counter.increment(Counter.empty(), :n1, 5),
      "c2" => Counter.increment(Counter.empty(), :n2, 3)
    }

    merged = Counter.merge_stores(store_a, store_b)
    assert Counter.value(merged["c1"]) == 10
    assert Counter.value(merged["c2"]) == 3
  end
end
