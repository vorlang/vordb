defmodule VorDB.DirtyTrackerTest do
  use ExUnit.Case, async: false

  setup do
    case GenServer.whereis(VorDB.DirtyTracker) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    {:ok, pid} = VorDB.DirtyTracker.start_link(peers: [:node2, :node3], num_vnodes: 4)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    :ok
  end

  test "mark_dirty adds key to all peer dirty sets for that vnode" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    Process.sleep(5)

    {seq2, deltas2} = VorDB.DirtyTracker.take_deltas(0, :node2)
    {seq3, deltas3} = VorDB.DirtyTracker.take_deltas(0, :node3)

    assert "key1" in deltas2.lww
    assert seq2 > 0
    assert "key1" in deltas3.lww
    assert seq3 > 0
  end

  test "take_deltas moves keys to pending, clears dirty" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    Process.sleep(5)

    {_seq, first} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert "key1" in first.lww

    {_seq2, second} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert second.lww == []
  end

  test "take_deltas doesn't clear other peers" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    Process.sleep(5)

    VorDB.DirtyTracker.take_deltas(0, :node2)

    {_seq, deltas3} = VorDB.DirtyTracker.take_deltas(0, :node3)
    assert "key1" in deltas3.lww
  end

  test "confirm_ack clears pending for that sequence" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    Process.sleep(5)

    {seq, _deltas} = VorDB.DirtyTracker.take_deltas(0, :node2)
    VorDB.DirtyTracker.confirm_ack(0, :node2, seq)
    Process.sleep(5)

    # After ACK, taking again should be empty (no expired pending to re-add)
    {_seq2, deltas2} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert deltas2.lww == []
  end

  test "different vnodes have independent dirty sets" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key_a")
    VorDB.DirtyTracker.mark_dirty(1, :lww, "key_b")
    Process.sleep(5)

    {_seq, deltas0} = VorDB.DirtyTracker.take_deltas(0, :node2)
    {_seq, deltas1} = VorDB.DirtyTracker.take_deltas(1, :node2)

    assert deltas0.lww == ["key_a"]
    assert deltas1.lww == ["key_b"]
  end

  test "mark_dirty_keys adds multiple keys" do
    VorDB.DirtyTracker.mark_dirty_keys(0, :lww, %{"a" => :v1, "b" => :v2})
    Process.sleep(5)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert Enum.sort(deltas.lww) == ["a", "b"]
  end

  test "different types tracked independently" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    VorDB.DirtyTracker.mark_dirty(0, :set, "key2")
    VorDB.DirtyTracker.mark_dirty(0, :counter, "key3")
    Process.sleep(5)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert "key1" in deltas.lww
    assert "key2" in deltas.set
    assert "key3" in deltas.counter
  end

  test "duplicate marks are deduplicated" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    Process.sleep(5)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :node2)
    assert deltas.lww == ["key1"]
  end

  test "add_peer initializes tracking for all vnodes" do
    VorDB.DirtyTracker.add_peer(:node4)
    VorDB.DirtyTracker.mark_dirty(2, :lww, "key1")
    Process.sleep(5)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(2, :node4)
    assert "key1" in deltas.lww
  end

  test "remove_peer drops tracking" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "key1")
    VorDB.DirtyTracker.remove_peer(:node3)
    Process.sleep(5)

    {_seq, result} = VorDB.DirtyTracker.take_deltas(0, :node3)
    assert result == %{lww: [], set: [], counter: []}
  end

  test "take_deltas for unknown peer returns empty" do
    {seq, result} = VorDB.DirtyTracker.take_deltas(0, :unknown_node)
    assert seq == 0
    assert result == %{lww: [], set: [], counter: []}
  end

  test "sequence numbers increment per vnode+peer" do
    VorDB.DirtyTracker.mark_dirty(0, :lww, "a")
    Process.sleep(5)
    {seq1, _} = VorDB.DirtyTracker.take_deltas(0, :node2)

    VorDB.DirtyTracker.mark_dirty(0, :lww, "b")
    Process.sleep(5)
    {seq2, _} = VorDB.DirtyTracker.take_deltas(0, :node2)

    assert seq2 > seq1
  end
end
