defmodule VorDB.Integration.CounterClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VorDB.TestHelpers

  defp start_node(node_id) do
    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id], [])
    %{pid: pid, node_id: node_id}
  end

  defp stop_node(%{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
  end

  defp counter_inc(node, key, amount \\ 1) do
    GenServer.call(node.pid, {:counter_increment, %{key: key, amount: amount}})
  end

  defp counter_dec(node, key, amount \\ 1) do
    GenServer.call(node.pid, {:counter_decrement, %{key: key, amount: amount}})
  end

  defp counter_val(node, key) do
    {:counter_value, %{val: val}} = GenServer.call(node.pid, {:counter_value, %{key: key}})
    val
  end

  defp get_counter_store(node) do
    {:stores, %{counters: store}} = GenServer.call(node.pid, {:get_stores, %{}})
    store
  end

  defp gossip_counters(from_node, to_node) do
    store = get_counter_store(from_node)
    GenServer.cast(to_node.pid, {:counter_sync, %{remote_counter_store: store}})
    GenServer.call(to_node.pid, {:get_stores, %{}})
  end

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    :ok
  end

  test "increment on node 1, value visible on node 2 after gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    for _ <- 1..5, do: counter_inc(n1, "hits")
    gossip_counters(n1, n2)

    assert counter_val(n2, "hits") == 5

    stop_node(n1)
    stop_node(n2)
  end

  test "increments on different nodes combine correctly" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    for _ <- 1..3, do: counter_inc(n1, "hits")
    for _ <- 1..4, do: counter_inc(n2, "hits")

    gossip_counters(n1, n2)
    gossip_counters(n2, n1)

    assert counter_val(n1, "hits") == 7
    assert counter_val(n2, "hits") == 7

    stop_node(n1)
    stop_node(n2)
  end

  test "decrements propagate via gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    counter_inc(n1, "hits", 10)
    gossip_counters(n1, n2)

    counter_dec(n2, "hits", 3)
    gossip_counters(n2, n1)

    assert counter_val(n1, "hits") == 7
    assert counter_val(n2, "hits") == 7

    stop_node(n1)
    stop_node(n2)
  end

  test "counter survives node restart and re-syncs" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    counter_inc(n1, "hits", 5)
    counter_inc(n2, "hits", 3)
    gossip_counters(n1, n2)
    gossip_counters(n2, n1)

    # Stop n2
    GenServer.stop(n2.pid, :normal)

    # More increments on n1
    counter_inc(n1, "hits", 2)

    # Restart n2
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :node2], [])
    n2_new = %{n2 | pid: pid2}

    gossip_counters(n1, n2_new)

    # n2 should have: n1=7 (5+2), n2=3 (from RocksDB) = 10
    assert counter_val(n2_new, "hits") == 10

    stop_node(n1)
    GenServer.stop(pid2)
  end
end
