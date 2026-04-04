defmodule VorDB.KvStoreTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    pid = TestHelpers.start_kv_store(:test_node)

    on_exit(fn ->
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end

      TestHelpers.cleanup_dir(dir)
    end)

    %{pid: pid, dir: dir}
  end

  test "put then get returns value", %{pid: pid} do
    {:ok, %{timestamp: _ts}} = GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "hello", found: :true}} = result
  end

  test "get missing key returns not found", %{pid: pid} do
    result = GenServer.call(pid, {:get, %{key: "nonexistent"}})
    assert {:value, %{key: "nonexistent", found: :false}} = result
  end

  test "delete then get returns not found", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    {:deleted, %{key: "x", timestamp: _ts}} = GenServer.call(pid, {:delete, %{key: "x"}})
    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", found: :false}} = result
  end

  test "put overwrites existing key", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "v1"}})
    GenServer.call(pid, {:put, %{key: "x", value: "v2"}})
    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "v2", found: :true}} = result
  end

  test "sync merges remote store — higher timestamp wins", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "local"}})
    Process.sleep(2)

    remote_store = %{
      "x" => %{value: "remote", timestamp: :erlang.system_time(:millisecond) + 1000, node_id: :remote_node}
    }

    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: remote_store}})
    # Call to synchronize — ensures cast is processed
    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "remote", found: :true}} = result
  end

  test "sync merges remote store — lower timestamp loses", %{pid: pid} do
    remote_store = %{
      "x" => %{value: "old", timestamp: 1, node_id: :remote_node}
    }

    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: remote_store}})
    # Ensure sync processed
    GenServer.call(pid, {:get_stores, %{}})

    GenServer.call(pid, {:put, %{key: "x", value: "new"}})
    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "new", found: :true}} = result
  end

  test "sync with equal timestamps uses node_id tiebreaker", %{pid: pid} do
    ts = :erlang.system_time(:millisecond)

    store_a = %{"x" => %{value: "from_a", timestamp: ts, node_id: :node_a}}
    store_z = %{"x" => %{value: "from_z", timestamp: ts, node_id: :node_z}}

    # Sync both — :node_z should win (lexicographically greater)
    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: store_a}})
    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: store_z}})

    result = GenServer.call(pid, {:get, %{key: "x"}})
    assert {:value, %{key: "x", val: "from_z", found: :true}} = result
  end

  test "sync adds new keys from remote", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "local"}})

    remote_store = %{
      "y" => %{value: "remote", timestamp: :erlang.system_time(:millisecond), node_id: :remote_node}
    }

    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: remote_store}})

    result_x = GenServer.call(pid, {:get, %{key: "x"}})
    result_y = GenServer.call(pid, {:get, %{key: "y"}})
    assert {:value, %{key: "x", found: :true}} = result_x
    assert {:value, %{key: "y", val: "remote", found: :true}} = result_y
  end

  test "sync persists merged state to storage", %{pid: pid} do
    remote_store = %{
      "y" => %{value: "remote", timestamp: 5000, node_id: :remote_node}
    }

    GenServer.cast(pid, {:lww_sync, %{remote_lww_store: remote_store}})
    # Synchronize — get_store forces the cast (and its put_all) to complete
    {:stores, %{lww: _}} = GenServer.call(pid, {:get_stores, %{}})

    # Verify persistence by stopping the agent and starting a new one
    # that loads from Storage via on :init
    GenServer.stop(pid, :normal, 5_000)
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :verify_node], [])

    result = GenServer.call(pid2, {:get, %{key: "y"}})
    assert {:value, %{key: "y", val: "remote", found: :true}} = result

    GenServer.stop(pid2)
  end

  test "state loads from storage on init (on :init handler)", %{dir: _dir} do
    # Write entries directly to storage
    entry = %{value: "persisted", timestamp: 9999, node_id: :old_node}
    VorDB.Storage.put_lww("preloaded", entry)

    # Start a new agent — should load via on :init
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :new_node], [])

    result = GenServer.call(pid2, {:get, %{key: "preloaded"}})
    assert {:value, %{key: "preloaded", val: "persisted", found: :true}} = result

    GenServer.stop(pid2)
  end
end
