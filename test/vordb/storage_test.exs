defmodule VorDB.StorageTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    %{dir: dir}
  end

  test "put_lww with vnode_id and get round-trip" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    assert :ok = VorDB.Storage.put_lww(5, "key1", entry)
    assert {:ok, ^entry} = VorDB.Storage.get("lww:05:key1")
  end

  test "get on missing key returns not_found" do
    assert :not_found = VorDB.Storage.get("lww:00:nonexistent")
  end

  test "get_all_lww returns only that vnode's keys with prefix stripped" do
    e1 = %{value: "a", timestamp: 1, node_id: :n1}
    e2 = %{value: "b", timestamp: 2, node_id: :n2}
    :ok = VorDB.Storage.put_lww(5, "key1", e1)
    :ok = VorDB.Storage.put_lww(5, "key2", e2)
    :ok = VorDB.Storage.put_lww(6, "key1", %{value: "other_vnode", timestamp: 3, node_id: :n3})

    all = VorDB.Storage.get_all_lww(5)
    assert map_size(all) == 2
    assert all["key1"] == e1
    assert all["key2"] == e2
  end

  test "get_all_sets returns only set entries for vnode" do
    set1 = %{entries: %{"alice" => MapSet.new([:tag1])}, tombstones: %{}}
    :ok = VorDB.Storage.put_set(3, "s1", set1)
    :ok = VorDB.Storage.put_lww(3, "s1", %{value: "lww", timestamp: 1, node_id: :n1})

    all = VorDB.Storage.get_all_sets(3)
    assert map_size(all) == 1
    assert all["s1"] == set1
  end

  test "put_all_lww batch writes with vnode prefix" do
    entries = %{
      "a" => %{value: "v1", timestamp: 1, node_id: :n1},
      "b" => %{value: "v2", timestamp: 2, node_id: :n2}
    }

    assert :ok = VorDB.Storage.put_all_lww(7, entries)
    assert {:ok, %{value: "v1"}} = VorDB.Storage.get("lww:07:a")

    all = VorDB.Storage.get_all_lww(7)
    assert map_size(all) == 2
  end

  test "put overwrites existing value" do
    e1 = %{value: "v1", timestamp: 1, node_id: :n1}
    e2 = %{value: "v2", timestamp: 2, node_id: :n1}
    :ok = VorDB.Storage.put_lww(0, "key1", e1)
    :ok = VorDB.Storage.put_lww(0, "key1", e2)
    assert {:ok, ^e2} = VorDB.Storage.get("lww:00:key1")
  end

  test "data survives process restart", %{dir: dir} do
    entry = %{value: "persist", timestamp: 1000, node_id: :node1}
    :ok = VorDB.Storage.put_lww(0, "key1", entry)

    GenServer.stop(VorDB.Storage, :normal)
    {:ok, _pid} = VorDB.Storage.start_link(data_dir: dir)

    assert {:ok, ^entry} = VorDB.Storage.get("lww:00:key1")
  end

  test "different vnodes share one RocksDB instance" do
    :ok = VorDB.Storage.put_lww(0, "x", %{value: "v0", timestamp: 1, node_id: :n1})
    :ok = VorDB.Storage.put_lww(1, "x", %{value: "v1", timestamp: 2, node_id: :n1})

    assert VorDB.Storage.get_all_lww(0)["x"].value == "v0"
    assert VorDB.Storage.get_all_lww(1)["x"].value == "v1"
  end
end
