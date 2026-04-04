defmodule VorDB.PropertiesTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias VorDB.TestHelpers

  @moduledoc """
  Property-based tests for LWW merge correctness.

  These exercise the actual Vor-compiled agent's sync handler to verify
  the mathematical properties of the LWW CRDT merge at runtime.
  """

  # -- Generators --

  defp entry_gen do
    gen all value <- StreamData.binary(min_length: 1, max_length: 20),
            timestamp <- StreamData.positive_integer(),
            node_id <- StreamData.member_of([:node_a, :node_b, :node_c, :node_d, :node_e]) do
      %{value: value, timestamp: timestamp, node_id: node_id}
    end
  end

  defp store_gen do
    gen all pairs <- StreamData.list_of(
             StreamData.tuple({
               StreamData.string(:alphanumeric, min_length: 1, max_length: 8),
               entry_gen()
             }),
             min_length: 0,
             max_length: 10
           ) do
      Map.new(pairs)
    end
  end

  # -- Helpers --

  # Merge two stores through the Vor agent's sync handler.
  # Returns the resulting store map.
  defp vor_merge(base_store, incoming_store) do
    {_storage_pid, dir} = TestHelpers.start_storage()
    pid = TestHelpers.start_kv_store(:prop_node)

    try do
      # Load base store
      if map_size(base_store) > 0 do
        GenServer.cast(pid, {:lww_sync, %{remote_lww_store: base_store}})
      end

      # Merge incoming
      GenServer.cast(pid, {:lww_sync, %{remote_lww_store: incoming_store}})

      # Read result (synchronizes cast processing)
      {:stores, %{lww: result}} = GenServer.call(pid, {:get_stores, %{}})
      result
    after
      GenServer.stop(pid, :normal, 5_000)
      TestHelpers.cleanup_dir(dir)
    end
  end

  # -- Properties --

  property "LWW merge is commutative" do
    check all store_a <- store_gen(),
              store_b <- store_gen(),
              max_runs: 50 do
      result_ab = vor_merge(store_a, store_b)
      result_ba = vor_merge(store_b, store_a)
      assert result_ab == result_ba
    end
  end

  property "LWW merge is associative" do
    check all store_a <- store_gen(),
              store_b <- store_gen(),
              store_c <- store_gen(),
              max_runs: 30 do
      # (a merge b) merge c
      ab = vor_merge(store_a, store_b)
      abc_left = vor_merge(ab, store_c)

      # a merge (b merge c)
      bc = vor_merge(store_b, store_c)
      abc_right = vor_merge(store_a, bc)

      assert abc_left == abc_right
    end
  end

  property "LWW merge is idempotent" do
    check all store_a <- store_gen(),
              max_runs: 50 do
      result = vor_merge(store_a, store_a)
      assert result == store_a
    end
  end

  property "merge never decreases timestamps" do
    check all store_a <- store_gen(),
              store_b <- store_gen(),
              max_runs: 50 do
      merged = vor_merge(store_a, store_b)

      for {key, entry} <- merged do
        if Map.has_key?(store_a, key) do
          assert entry.timestamp >= store_a[key].timestamp
        end

        if Map.has_key?(store_b, key) do
          assert entry.timestamp >= store_b[key].timestamp
        end
      end
    end
  end

  property "merge preserves all keys" do
    check all store_a <- store_gen(),
              store_b <- store_gen(),
              max_runs: 50 do
      merged = vor_merge(store_a, store_b)

      for key <- Map.keys(store_a) do
        assert Map.has_key?(merged, key), "key #{inspect(key)} from store_a missing in merged"
      end

      for key <- Map.keys(store_b) do
        assert Map.has_key?(merged, key), "key #{inspect(key)} from store_b missing in merged"
      end

      expected_keys = MapSet.union(MapSet.new(Map.keys(store_a)), MapSet.new(Map.keys(store_b)))
      assert MapSet.new(Map.keys(merged)) == expected_keys
    end
  end
end
