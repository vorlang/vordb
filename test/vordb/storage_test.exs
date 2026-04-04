defmodule VorDB.StorageTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_pid, dir} = TestHelpers.start_storage()
    on_exit(fn -> TestHelpers.cleanup_dir(dir) end)
    %{dir: dir}
  end

  test "put and get round-trip" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    assert :ok = VorDB.Storage.put("key1", entry)
    assert {:ok, ^entry} = VorDB.Storage.get("key1")
  end

  test "get on missing key returns not_found" do
    assert :not_found = VorDB.Storage.get("nonexistent")
  end

  test "delete removes key" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    :ok = VorDB.Storage.put("key1", entry)
    assert :ok = VorDB.Storage.delete("key1")
    assert :not_found = VorDB.Storage.get("key1")
  end

  test "get_all returns all stored entries" do
    e1 = %{value: "a", timestamp: 1, node_id: :n1}
    e2 = %{value: "b", timestamp: 2, node_id: :n2}
    :ok = VorDB.Storage.put("key1", e1)
    :ok = VorDB.Storage.put("key2", e2)

    all = VorDB.Storage.get_all()
    assert map_size(all) == 2
    assert all["key1"] == e1
    assert all["key2"] == e2
  end

  test "put_all batch writes multiple entries" do
    entries = %{
      "a" => %{value: "v1", timestamp: 1, node_id: :n1},
      "b" => %{value: "v2", timestamp: 2, node_id: :n2},
      "c" => %{value: "v3", timestamp: 3, node_id: :n3}
    }

    assert :ok = VorDB.Storage.put_all(entries)
    assert {:ok, %{value: "v1"}} = VorDB.Storage.get("a")
    assert {:ok, %{value: "v2"}} = VorDB.Storage.get("b")
    assert {:ok, %{value: "v3"}} = VorDB.Storage.get("c")
  end

  test "put overwrites existing value" do
    e1 = %{value: "v1", timestamp: 1, node_id: :n1}
    e2 = %{value: "v2", timestamp: 2, node_id: :n1}
    :ok = VorDB.Storage.put("key1", e1)
    :ok = VorDB.Storage.put("key1", e2)
    assert {:ok, ^e2} = VorDB.Storage.get("key1")
  end

  test "data survives process restart", %{dir: dir} do
    entry = %{value: "persist", timestamp: 1000, node_id: :node1}
    :ok = VorDB.Storage.put("key1", entry)

    # Stop the Storage GenServer
    GenServer.stop(VorDB.Storage, :normal)

    # Restart with same directory
    {:ok, _pid} = VorDB.Storage.start_link(data_dir: dir)

    assert {:ok, ^entry} = VorDB.Storage.get("key1")
  end
end
