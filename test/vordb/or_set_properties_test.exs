defmodule VorDB.OrSetPropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VorDB.OrSet

  defp tag_gen do
    gen all node_id <- StreamData.member_of([:n1, :n2, :n3]),
            ts <- StreamData.positive_integer(),
            counter <- StreamData.positive_integer() do
      OrSet.make_tag(node_id, ts, counter)
    end
  end

  defp or_set_gen do
    gen all ops <- StreamData.list_of(
             StreamData.tuple({
               StreamData.member_of([:add, :remove]),
               StreamData.string(:alphanumeric, min_length: 1, max_length: 5),
               tag_gen()
             }),
             max_length: 15
           ) do
      Enum.reduce(ops, OrSet.empty(), fn
        {:add, element, tag}, set -> OrSet.add_element(set, element, tag)
        {:remove, element, _tag}, set -> OrSet.remove_element(set, element)
      end)
    end
  end

  defp set_store_gen do
    gen all pairs <- StreamData.list_of(
             StreamData.tuple({
               StreamData.string(:alphanumeric, min_length: 1, max_length: 5),
               or_set_gen()
             }),
             min_length: 0,
             max_length: 5
           ) do
      Map.new(pairs)
    end
  end

  property "OR-Set merge is commutative" do
    check all a <- or_set_gen(),
              b <- or_set_gen(),
              max_runs: 50 do
      assert OrSet.merge(a, b) == OrSet.merge(b, a)
    end
  end

  property "OR-Set merge is associative" do
    check all a <- or_set_gen(),
              b <- or_set_gen(),
              c <- or_set_gen(),
              max_runs: 30 do
      assert OrSet.merge(OrSet.merge(a, b), c) == OrSet.merge(a, OrSet.merge(b, c))
    end
  end

  property "OR-Set merge is idempotent" do
    check all a <- or_set_gen(),
              max_runs: 50 do
      assert OrSet.merge(a, a) == a
    end
  end

  property "add followed by merge preserves element unless tombstoned by same tag" do
    check all base <- or_set_gen(),
              element <- StreamData.string(:alphanumeric, min_length: 1, max_length: 5),
              tag <- tag_gen(),
              other <- or_set_gen(),
              max_runs: 50 do
      with_add = OrSet.add_element(base, element, tag)
      merged = OrSet.merge(with_add, other)
      # Element is present UNLESS `other` has the exact same tag in tombstones
      other_tombstones = Map.get(other.tombstones, element, MapSet.new())

      if not MapSet.member?(other_tombstones, tag) do
        assert element in OrSet.read_elements(merged)
      end
    end
  end

  property "merge_stores is commutative" do
    check all a <- set_store_gen(),
              b <- set_store_gen(),
              max_runs: 50 do
      assert OrSet.merge_stores(a, b) == OrSet.merge_stores(b, a)
    end
  end
end
