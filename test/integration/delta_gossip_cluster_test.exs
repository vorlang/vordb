defmodule VorDB.Integration.DeltaGossipClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VorDB.TestHelpers

  @moduledoc """
  Integration tests for delta gossip across simulated nodes.
  Uses manual gossip dispatch to verify delta behavior.
  """

  defp start_node(node_id) do
    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id], [])
    %{pid: pid, node_id: node_id}
  end

  defp stop_node(%{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
  end

  defp put_kv(node, key, value) do
    GenServer.call(node.pid, {:put, %{key: key, value: value}})
  end

  defp get_kv(node, key) do
    GenServer.call(node.pid, {:get, %{key: key}})
  end

  defp get_stores(node) do
    {:stores, stores} = GenServer.call(node.pid, {:get_stores, %{}})
    stores
  end

  defp delta_gossip_lww(from_node, to_node, keys) do
    {:lww_entries, %{entries: entries}} =
      GenServer.call(from_node.pid, {:get_lww_entries, %{keys: keys}})

    if map_size(entries) > 0 do
      GenServer.cast(to_node.pid, {:lww_sync, %{remote_lww_store: entries}})
      GenServer.call(to_node.pid, {:get_stores, %{}})
    end
  end

  defp full_gossip(from_node, to_node) do
    stores = get_stores(from_node)

    if map_size(stores.lww) > 0 do
      GenServer.cast(to_node.pid, {:lww_sync, %{remote_lww_store: stores.lww}})
    end

    if map_size(stores.sets) > 0 do
      GenServer.cast(to_node.pid, {:set_sync, %{remote_set_store: stores.sets}})
    end

    if map_size(stores.counters) > 0 do
      GenServer.cast(to_node.pid, {:counter_sync, %{remote_counter_store: stores.counters}})
    end

    GenServer.call(to_node.pid, {:get_stores, %{}})
  end

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    :ok
  end

  test "delta gossip — send only changed key" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    # Put 3 keys on n1, full gossip to n2
    put_kv(n1, "a", "1")
    put_kv(n1, "b", "2")
    put_kv(n1, "c", "3")
    full_gossip(n1, n2)

    # Put 1 new key on n1
    put_kv(n1, "d", "4")

    # Delta gossip — only send "d"
    delta_gossip_lww(n1, n2, ["d"])

    # n2 has all 4
    assert {:value, %{val: "1", found: :true}} = get_kv(n2, "a")
    assert {:value, %{val: "4", found: :true}} = get_kv(n2, "d")

    stop_node(n1)
    stop_node(n2)
  end

  test "full-state sync catches missed deltas" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "x", "hello")

    # Skip delta — go straight to full sync
    full_gossip(n1, n2)

    assert {:value, %{val: "hello", found: :true}} = get_kv(n2, "x")

    stop_node(n1)
    stop_node(n2)
  end

  test "multi-hop propagation — 3 nodes via delta re-gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)
    n3 = start_node(:node3)

    put_kv(n1, "x", "hello")

    # n1 → n2 (delta)
    delta_gossip_lww(n1, n2, ["x"])
    assert {:value, %{val: "hello", found: :true}} = get_kv(n2, "x")

    # n2 → n3 (n2 re-gossips what it received)
    delta_gossip_lww(n2, n3, ["x"])
    assert {:value, %{val: "hello", found: :true}} = get_kv(n3, "x")

    stop_node(n1)
    stop_node(n2)
    stop_node(n3)
  end

  test "mixed types in delta gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "k", "value")
    GenServer.call(n1.pid, {:set_add, %{key: "s", element: "alice"}})
    GenServer.call(n1.pid, {:counter_increment, %{key: "c", amount: 5}})

    # Full gossip carries all types
    full_gossip(n1, n2)

    assert {:value, %{val: "value", found: :true}} = get_kv(n2, "k")

    {:set_members, %{members: members}} =
      GenServer.call(n2.pid, {:set_members, %{key: "s"}})

    assert "alice" in members

    {:counter_value, %{val: 5}} =
      GenServer.call(n2.pid, {:counter_value, %{key: "c"}})

    stop_node(n1)
    stop_node(n2)
  end
end
