defmodule VorDB.Integration.SetClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VorDB.TestHelpers

  @moduledoc """
  Multi-instance integration tests for OR-Set gossip.
  Same simulated approach as cluster_test.exs — manual gossip via sync casts.
  """

  defp start_node(node_id) do
    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id, vnode_id: 0, sync_interval_ms: 600_000], [])
    %{pid: pid, node_id: node_id}
  end

  defp stop_node(%{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
  end

  defp set_add(node, key, element) do
    GenServer.call(node.pid, {:set_add, %{key: key, element: element}})
  end

  defp set_members(node, key) do
    GenServer.call(node.pid, {:set_members, %{key: key}})
  end

  defp set_remove(node, key, element) do
    GenServer.call(node.pid, {:set_remove, %{key: key, element: element}})
  end

  defp get_set_store(node) do
    {:stores, %{sets: store}} = GenServer.call(node.pid, {:get_stores, %{}})
    store
  end

  defp gossip_sets(from_node, to_node) do
    store = get_set_store(from_node)
    GenServer.cast(to_node.pid, {:set_sync, %{remote_set_store: store}})
    # Synchronize
    GenServer.call(to_node.pid, {:get_stores, %{}})
  end

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    :ok
  end

  test "set add on node 1, members visible on node 2 after gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    set_add(n1, "s1", "alice")
    gossip_sets(n1, n2)

    {:set_members, %{members: members}} = set_members(n2, "s1")
    assert "alice" in members

    stop_node(n1)
    stop_node(n2)
  end

  test "concurrent adds on different nodes merge correctly" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    set_add(n1, "s1", "alice")
    set_add(n2, "s1", "bob")

    gossip_sets(n1, n2)
    gossip_sets(n2, n1)

    {:set_members, %{members: m1}} = set_members(n1, "s1")
    {:set_members, %{members: m2}} = set_members(n2, "s1")

    assert Enum.sort(m1) == ["alice", "bob"]
    assert Enum.sort(m2) == ["alice", "bob"]

    stop_node(n1)
    stop_node(n2)
  end

  test "remove on node 1, element gone on node 2 after gossip" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    set_add(n1, "s1", "alice")
    set_add(n1, "s1", "bob")
    gossip_sets(n1, n2)

    set_remove(n1, "s1", "alice")
    gossip_sets(n1, n2)

    {:set_members, %{members: members}} = set_members(n2, "s1")
    assert members == ["bob"]

    stop_node(n1)
    stop_node(n2)
  end

  test "concurrent add and remove across nodes — add wins" do
    n1 = start_node(:node1)
    n2 = start_node(:node2)

    # Both nodes add "alice"
    set_add(n1, "s1", "alice")
    gossip_sets(n1, n2)

    # n1 removes alice, n2 concurrently adds alice again (new tag)
    set_remove(n1, "s1", "alice")
    set_add(n2, "s1", "alice")

    # Gossip both directions
    gossip_sets(n1, n2)
    gossip_sets(n2, n1)

    # alice should be present — n2's new add has a tag n1's remove didn't observe
    {:set_members, %{members: m1}} = set_members(n1, "s1")
    {:set_members, %{members: m2}} = set_members(n2, "s1")

    assert "alice" in m1
    assert "alice" in m2

    stop_node(n1)
    stop_node(n2)
  end
end
