defmodule VorDB.SerializerTest do
  use ExUnit.Case, async: true

  alias VorDB.Serializer

  test "round-trip encode/decode for entry map" do
    entry = %{value: "hello", timestamp: 1000, node_id: :node1}
    assert Serializer.decode(Serializer.encode(entry)) == entry
  end

  test "round-trip for tombstone entry" do
    entry = %{value: :__tombstone__, timestamp: 2000, node_id: :node2}
    assert Serializer.decode(Serializer.encode(entry)) == entry
  end

  test "round-trip for empty map" do
    assert Serializer.decode(Serializer.encode(%{})) == %{}
  end

  test "round-trip for complex nested values" do
    entry = %{
      value: %{messages: ["hello", "world"], metadata: %{sender: :alice}},
      timestamp: 3000,
      node_id: :node3
    }

    assert Serializer.decode(Serializer.encode(entry)) == entry
  end
end
