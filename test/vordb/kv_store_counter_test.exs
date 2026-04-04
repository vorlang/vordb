defmodule VorDB.KvStoreCounterTest do
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

    %{pid: pid}
  end

  test "counter_increment then counter_value returns count", %{pid: pid} do
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 1}})
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 1}})
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 1}})

    {:counter_value, %{key: "c1", val: val}} =
      GenServer.call(pid, {:counter_value, %{key: "c1"}})

    assert val == 3
  end

  test "counter_decrement reduces value", %{pid: pid} do
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 5}})
    GenServer.call(pid, {:counter_decrement, %{key: "c1", amount: 2}})

    {:counter_value, %{val: val}} = GenServer.call(pid, {:counter_value, %{key: "c1"}})
    assert val == 3
  end

  test "counter_value on missing key returns not_found", %{pid: pid} do
    result = GenServer.call(pid, {:counter_value, %{key: "nonexistent"}})
    assert {:counter_not_found, %{key: "nonexistent"}} = result
  end

  test "increment with custom amount", %{pid: pid} do
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 10}})

    {:counter_value, %{val: val}} = GenServer.call(pid, {:counter_value, %{key: "c1"}})
    assert val == 10
  end

  test "counter operations don't interfere with LWW or set operations", %{pid: pid} do
    GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    GenServer.call(pid, {:set_add, %{key: "x", element: "alice"}})
    GenServer.call(pid, {:counter_increment, %{key: "x", amount: 42}})

    {:value, %{val: "hello", found: :true}} = GenServer.call(pid, {:get, %{key: "x"}})
    {:set_members, %{members: members}} = GenServer.call(pid, {:set_members, %{key: "x"}})
    {:counter_value, %{val: 42}} = GenServer.call(pid, {:counter_value, %{key: "x"}})

    assert "alice" in members
  end

  test "counter_sync merges remote counter store", %{pid: pid} do
    # Local: increment 5 times
    for _ <- 1..5 do
      GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 1}})
    end

    # Remote: node2 incremented 3 times
    remote_counter = VorDB.Counter.increment(VorDB.Counter.empty(), :node2, 3)
    remote_store = %{"c1" => remote_counter}
    GenServer.cast(pid, {:counter_sync, %{remote_counter_store: remote_store}})

    # Synchronize
    GenServer.call(pid, {:get_stores, %{}})

    {:counter_value, %{val: val}} = GenServer.call(pid, {:counter_value, %{key: "c1"}})
    assert val == 8
  end

  test "counter state persists across restart", %{pid: pid} do
    GenServer.call(pid, {:counter_increment, %{key: "c1", amount: 7}})

    GenServer.stop(pid, :normal, 5_000)
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :test_node], [])

    {:counter_value, %{val: val}} = GenServer.call(pid2, {:counter_value, %{key: "c1"}})
    assert val == 7

    GenServer.stop(pid2)
  end
end
