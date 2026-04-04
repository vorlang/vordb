defmodule VorDB.StorageTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    %{dir: dir}
  end

  test "put_lww and get round-trip" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    assert :ok = VorDB.Storage.put_lww("key1", entry)
    assert {:ok, ^entry} = VorDB.Storage.get("lww:key1")
  end

  test "get on missing key returns not_found" do
    assert :not_found = VorDB.Storage.get("lww:nonexistent")
  end

  test "delete removes key" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    :ok = VorDB.Storage.put_lww("key1", entry)
    assert :ok = VorDB.Storage.delete("lww:key1")
    assert :not_found = VorDB.Storage.get("lww:key1")
  end

  test "get_all_lww returns only LWW entries with prefix stripped" do
    e1 = %{value: "a", timestamp: 1, node_id: :n1}
    e2 = %{value: "b", timestamp: 2, node_id: :n2}
    :ok = VorDB.Storage.put_lww("key1", e1)
    :ok = VorDB.Storage.put_lww("key2", e2)

    # Also put a set entry — should NOT appear in get_all_lww
    :ok = VorDB.Storage.put_set("key1", %{entries: %{}, tombstones: %{}})

    all = VorDB.Storage.get_all_lww()
    assert map_size(all) == 2
    assert all["key1"] == e1
    assert all["key2"] == e2
  end

  test "get_all_sets returns only set entries with prefix stripped" do
    set1 = %{entries: %{"alice" => MapSet.new([:tag1])}, tombstones: %{}}
    :ok = VorDB.Storage.put_set("s1", set1)

    # Also put an LWW entry — should NOT appear
    :ok = VorDB.Storage.put_lww("s1", %{value: "lww", timestamp: 1, node_id: :n1})

    all = VorDB.Storage.get_all_sets()
    assert map_size(all) == 1
    assert all["s1"] == set1
  end

  test "put_all_lww batch writes with prefix" do
    entries = %{
      "a" => %{value: "v1", timestamp: 1, node_id: :n1},
      "b" => %{value: "v2", timestamp: 2, node_id: :n2}
    }

    assert :ok = VorDB.Storage.put_all_lww(entries)
    assert {:ok, %{value: "v1"}} = VorDB.Storage.get("lww:a")
    assert {:ok, %{value: "v2"}} = VorDB.Storage.get("lww:b")

    all = VorDB.Storage.get_all_lww()
    assert map_size(all) == 2
  end

  test "put overwrites existing value" do
    e1 = %{value: "v1", timestamp: 1, node_id: :n1}
    e2 = %{value: "v2", timestamp: 2, node_id: :n1}
    :ok = VorDB.Storage.put_lww("key1", e1)
    :ok = VorDB.Storage.put_lww("key1", e2)
    assert {:ok, ^e2} = VorDB.Storage.get("lww:key1")
  end

  test "data survives process restart", %{dir: dir} do
    entry = %{value: "persist", timestamp: 1000, node_id: :node1}
    :ok = VorDB.Storage.put_lww("key1", entry)

    GenServer.stop(VorDB.Storage, :normal)
    {:ok, _pid} = VorDB.Storage.start_link(data_dir: dir)

    assert {:ok, ^entry} = VorDB.Storage.get("lww:key1")
  end
end
