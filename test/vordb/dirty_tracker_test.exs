defmodule VorDB.DirtyTrackerTest do
  use ExUnit.Case, async: false

  setup do
    case GenServer.whereis(VorDB.DirtyTracker) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    {:ok, pid} = VorDB.DirtyTracker.start_link(peers: [:node2, :node3])
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    :ok
  end

  test "mark_dirty adds key to all peer dirty sets" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    # Small sleep to let cast process
    Process.sleep(5)

    assert %{lww: lww2} = VorDB.DirtyTracker.take_deltas(:node2)
    assert "key1" in lww2
    assert %{lww: lww3} = VorDB.DirtyTracker.take_deltas(:node3)
    assert "key1" in lww3
  end

  test "take_deltas clears dirty set for that peer" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    Process.sleep(5)

    %{lww: first} = VorDB.DirtyTracker.take_deltas(:node2)
    assert "key1" in first

    %{lww: second} = VorDB.DirtyTracker.take_deltas(:node2)
    assert second == []
  end

  test "take_deltas doesn't clear other peers" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    Process.sleep(5)

    VorDB.DirtyTracker.take_deltas(:node2)

    %{lww: lww3} = VorDB.DirtyTracker.take_deltas(:node3)
    assert "key1" in lww3
  end

  test "mark_dirty_keys adds multiple keys" do
    VorDB.DirtyTracker.mark_dirty_keys(:lww, %{"a" => :v1, "b" => :v2})
    Process.sleep(5)

    %{lww: lww} = VorDB.DirtyTracker.take_deltas(:node2)
    assert Enum.sort(lww) == ["a", "b"]
  end

  test "different types tracked independently" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    VorDB.DirtyTracker.mark_dirty(:set, "key2")
    VorDB.DirtyTracker.mark_dirty(:counter, "key3")
    Process.sleep(5)

    deltas = VorDB.DirtyTracker.take_deltas(:node2)
    assert "key1" in deltas.lww
    assert "key2" in deltas.set
    assert "key3" in deltas.counter
  end

  test "duplicate marks are deduplicated" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    Process.sleep(5)

    %{lww: lww} = VorDB.DirtyTracker.take_deltas(:node2)
    assert lww == ["key1"]
  end

  test "add_peer initializes empty dirty sets" do
    VorDB.DirtyTracker.add_peer(:node4)
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    Process.sleep(5)

    %{lww: lww} = VorDB.DirtyTracker.take_deltas(:node4)
    assert "key1" in lww
  end

  test "remove_peer drops tracking" do
    VorDB.DirtyTracker.mark_dirty(:lww, "key1")
    VorDB.DirtyTracker.remove_peer(:node3)
    Process.sleep(5)

    result = VorDB.DirtyTracker.take_deltas(:node3)
    assert result == %{lww: [], set: [], counter: []}
  end

  test "take_deltas for unknown peer returns empty" do
    result = VorDB.DirtyTracker.take_deltas(:unknown_node)
    assert result == %{lww: [], set: [], counter: []}
  end
end
