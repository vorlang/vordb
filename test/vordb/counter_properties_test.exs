defmodule VorDB.CounterPropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VorDB.Counter

  defp counter_gen do
    gen all p_entries <- StreamData.list_of(
             StreamData.tuple({
               StreamData.member_of([:n1, :n2, :n3, :n4]),
               StreamData.positive_integer()
             }),
             max_length: 5
           ),
           n_entries <- StreamData.list_of(
             StreamData.tuple({
               StreamData.member_of([:n1, :n2, :n3, :n4]),
               StreamData.positive_integer()
             }),
             max_length: 5
           ) do
      %{p: Map.new(p_entries), n: Map.new(n_entries)}
    end
  end

  defp counter_store_gen do
    gen all pairs <- StreamData.list_of(
             StreamData.tuple({
               StreamData.string(:alphanumeric, min_length: 1, max_length: 5),
               counter_gen()
             }),
             max_length: 5
           ) do
      Map.new(pairs)
    end
  end

  property "PN-Counter merge is commutative" do
    check all a <- counter_gen(),
              b <- counter_gen(),
              max_runs: 50 do
      assert Counter.merge(a, b) == Counter.merge(b, a)
    end
  end

  property "PN-Counter merge is associative" do
    check all a <- counter_gen(),
              b <- counter_gen(),
              c <- counter_gen(),
              max_runs: 30 do
      assert Counter.merge(Counter.merge(a, b), c) ==
               Counter.merge(a, Counter.merge(b, c))
    end
  end

  property "PN-Counter merge is idempotent" do
    check all a <- counter_gen(),
              max_runs: 50 do
      assert Counter.merge(a, a) == a
    end
  end

  property "merge never decreases any node's individual counts" do
    check all a <- counter_gen(),
              b <- counter_gen(),
              max_runs: 50 do
      merged = Counter.merge(a, b)

      for {node, count} <- merged.p do
        assert count >= Map.get(a.p, node, 0)
        assert count >= Map.get(b.p, node, 0)
      end

      for {node, count} <- merged.n do
        assert count >= Map.get(a.n, node, 0)
        assert count >= Map.get(b.n, node, 0)
      end
    end
  end

  property "merge_stores is commutative" do
    check all a <- counter_store_gen(),
              b <- counter_store_gen(),
              max_runs: 50 do
      assert Counter.merge_stores(a, b) == Counter.merge_stores(b, a)
    end
  end
end
