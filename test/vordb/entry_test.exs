defmodule VorDB.EntryTest do
  use ExUnit.Case, async: true

  alias VorDB.Entry

  test "new/3 builds correct map structure" do
    entry = Entry.new("hello", 1000, :node1)
    assert entry == %{value: "hello", timestamp: 1000, node_id: :node1}
  end

  test "tombstone/2 builds tombstone entry" do
    entry = Entry.tombstone(1000, :node1)
    assert entry.value == :__tombstone__
    assert entry.timestamp == 1000
    assert entry.node_id == :node1
  end

  test "lookup returns val and found: :true for existing key" do
    store = %{"key1" => %{value: "hello", timestamp: 1000, node_id: :node1}}
    assert %{val: "hello", found: :true} = Entry.lookup(store, "key1")
  end

  test "lookup returns found: :false for missing key" do
    assert %{val: :none, found: :false} = Entry.lookup(%{}, "missing")
  end

  test "lookup returns found: :false for tombstone" do
    store = %{"key1" => %{value: :__tombstone__, timestamp: 1000, node_id: :node1}}
    assert %{val: :none, found: :false} = Entry.lookup(store, "key1")
  end
end
