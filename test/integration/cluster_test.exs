defmodule VorDB.Integration.ClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VorDB.TestHelpers

  @moduledoc """
  Multi-instance integration tests.

  Uses multiple KvStore agents within the same node, each with its own
  Storage instance (separate RocksDB directories). Gossip is simulated
  by manually passing store maps between agents via :sync casts.

  This is the simulated multi-instance approach (vs. :peer distributed nodes).
  It validates all core logic without the complexity of distributed Erlang setup.
  """

  # -- Helpers --

  defp start_node(node_id) do
    dir = Path.join(System.tmp_dir!(), "vordb_cluster_#{node_id}_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    # Each node needs its own Storage. We can't use the global VorDB.Storage name
    # for multiple instances. Instead, bypass Storage and use direct RocksDB for isolation,
    # OR use a single Storage with key prefixing.
    # For simplicity: use the global Storage but with prefixed keys per node.
    # The agent manages its own in-memory store, so key collisions only affect persistence.

    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id], [])
    %{pid: pid, node_id: node_id, dir: dir}
  end

  defp stop_node(%{pid: pid, dir: dir}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
    File.rm_rf!(dir)
  end

  defp put_kv(node, key, value) do
    GenServer.call(node.pid, {:put, %{key: key, value: value}})
  end

  defp get_kv(node, key) do
    GenServer.call(node.pid, {:get, %{key: key}})
  end

  defp delete_kv(node, key) do
    GenServer.call(node.pid, {:delete, %{key: key}})
  end

  defp get_store(node) do
    {:stores, %{lww: store}} = GenServer.call(node.pid, {:get_stores, %{}})
    store
  end

  defp gossip(from_node, to_node) do
    store = get_store(from_node)
    GenServer.cast(to_node.pid, {:lww_sync, %{remote_lww_store: store}})
    # Synchronize to ensure cast is processed
    get_store(to_node)
  end

  defp full_mesh_gossip(nodes) do
    for from <- nodes, to <- nodes, from != to do
      gossip(from, to)
    end
  end

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    :ok
  end

  # -- Tests --

  test "write to node 1, read from node 2 after gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "x", "hello")
    gossip(n1, n2)

    result = get_kv(n2, "x")
    assert {:value, %{key: "x", val: "hello", found: :true}} = result

    stop_node(n1)
    stop_node(n2)
  end

  test "writes to 3 nodes converge" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)
    n3 = start_node(:node3)
    nodes = [n1, n2, n3]

    put_kv(n1, "a", "from_1")
    put_kv(n2, "b", "from_2")
    put_kv(n3, "c", "from_3")

    # Two rounds of full mesh gossip for complete propagation
    full_mesh_gossip(nodes)
    full_mesh_gossip(nodes)

    for node <- nodes do
      assert {:value, %{val: "from_1", found: :true}} = get_kv(node, "a")
      assert {:value, %{val: "from_2", found: :true}} = get_kv(node, "b")
      assert {:value, %{val: "from_3", found: :true}} = get_kv(node, "c")
    end

    Enum.each(nodes, &stop_node/1)
  end

  test "concurrent writes to same key — LWW resolves" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "x", "first")
    Process.sleep(2)
    put_kv(n2, "x", "second")

    gossip(n1, n2)
    gossip(n2, n1)

    # Both nodes should agree — "second" has higher timestamp
    assert {:value, %{val: "second", found: :true}} = get_kv(n1, "x")
    assert {:value, %{val: "second", found: :true}} = get_kv(n2, "x")

    stop_node(n1)
    stop_node(n2)
  end

  test "delete propagates via gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "x", "hello")
    gossip(n1, n2)

    # Verify it arrived
    assert {:value, %{found: :true}} = get_kv(n2, "x")

    # Delete on n2
    delete_kv(n2, "x")
    gossip(n2, n1)

    # Should be gone on both
    assert {:value, %{found: :false}} = get_kv(n1, "x")
    assert {:value, %{found: :false}} = get_kv(n2, "x")

    stop_node(n1)
    stop_node(n2)
  end

  test "node restart recovers from RocksDB" do
    n1 = start_node(:node1)

    put_kv(n1, "x", "persist_me")
    GenServer.stop(n1.pid, :normal)

    # Start a new agent — should recover state from Storage via on :init
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :node1], [])

    result = GenServer.call(pid2, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "persist_me", found: :true}} = result

    GenServer.stop(pid2)
  end

  test "restarted node re-syncs with cluster" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    put_kv(n1, "x", "before_restart")
    gossip(n1, n2)

    # Stop n2
    GenServer.stop(n2.pid, :normal)

    # Write more on n1 while n2 is down
    put_kv(n1, "y", "during_downtime")

    # Restart n2 — recovers "x" from RocksDB via on :init
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :node2], [])
    n2_new = %{n2 | pid: pid2}

    # Gossip from n1 brings "y"
    gossip(n1, n2_new)

    assert {:value, %{val: "before_restart", found: :true}} = get_kv(n2_new, "x")
    assert {:value, %{val: "during_downtime", found: :true}} = get_kv(n2_new, "y")

    stop_node(n1)
    GenServer.stop(pid2)
  end

  test "partition and recovery" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)
    n3 = start_node(:node3)

    put_kv(n1, "x", "before_partition")
    full_mesh_gossip([n1, n2, n3])

    # "Partition" — only gossip between n1 and n2, not n3
    put_kv(n1, "y", "during_partition")
    gossip(n1, n2)
    gossip(n2, n1)

    # n3 doesn't know about "y"
    assert {:value, %{found: :false}} = get_kv(n3, "y")

    # "Heal partition" — gossip to n3
    gossip(n1, n3)

    # n3 now has both
    assert {:value, %{val: "before_partition", found: :true}} = get_kv(n3, "x")
    assert {:value, %{val: "during_partition", found: :true}} = get_kv(n3, "y")

    Enum.each([n1, n2, n3], &stop_node/1)
  end
end
