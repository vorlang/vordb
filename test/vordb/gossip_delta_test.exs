defmodule VorDB.GossipDeltaTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()

    case GenServer.whereis(VorDB.DirtyTracker) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    {:ok, _dt} = VorDB.DirtyTracker.start_link(peers: [:peer1, :peer2], num_vnodes: 4)
    pid = TestHelpers.start_kv_store(:test_node, 0)

    on_exit(fn ->
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end

      TestHelpers.cleanup_dir(dir)
    end)

    %{pid: pid}
  end

  test "mutation marks key dirty for all peers on vnode 0", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    Process.sleep(10)

    {_seq, deltas1} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    {_seq, deltas2} = VorDB.DirtyTracker.take_deltas(0, :peer2)

    assert "x" in deltas1.lww
    assert "x" in deltas2.lww
  end

  test "multiple mutations accumulate in dirty set", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "1"}})
    GenServer.call(pid, {:put, %{key: "y", value: "2"}})
    GenServer.call(pid, {:put, %{key: "z", value: "3"}})
    Process.sleep(10)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert Enum.sort(deltas.lww) == ["x", "y", "z"]
  end

  test "dirty set cleared after take_deltas", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    Process.sleep(10)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert "x" in deltas.lww

    {_seq, deltas2} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert deltas2.lww == []
  end

  test "sync handler marks received keys dirty for propagation", %{pid: pid} do
    remote_store = %{
      "remote_key" => %{value: "remote", timestamp: 99999, node_id: :remote_node}
    }

    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: remote_store}})
    GenServer.call(pid, {:get_stores, %{}})
    Process.sleep(10)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert "remote_key" in deltas.lww
  end

  test "no dirty keys means empty deltas" do
    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert deltas.lww == []
    assert deltas.set == []
    assert deltas.counter == []
  end

  test "set mutation marks set type dirty", %{pid: pid} do
    GenServer.call(pid, {:set_add, %{key: "s1", element: "alice"}})
    Process.sleep(10)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert "s1" in deltas.set
  end

  test "counter mutation marks counter type dirty", %{pid: pid} do
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 1}})
    Process.sleep(10)

    {_seq, deltas} = VorDB.DirtyTracker.take_deltas(0, :peer1)
    assert "c1" in deltas.counter
  end

  test "get_*_entries returns specific entries by key", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "a", value: "1"}})
    GenServer.call(pid, {:put, %{key: "b", value: "2"}})
    GenServer.call(pid, {:put, %{key: "c", value: "3"}})

    {:lww_entries, %{entries: entries}} =
      GenServer.call(pid, {:get_lww_entries, %{keys: ["a", "c"]}})

    assert map_size(entries) == 2
    assert Map.has_key?(entries, "a")
    assert Map.has_key?(entries, "c")
    refute Map.has_key?(entries, "b")
  end
end
