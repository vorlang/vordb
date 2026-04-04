defmodule VorDB.KvStoreSetTest do
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

  test "set_add then set_members returns element", %{pid: pid} do
    {:set_ok, %{}} = GenServer.call(pid, {:set_add, %{key: "s1", element: "alice"}})
    result = GenServer.call(pid, {:set_members, %{key: "s1"}})
    assert {:set_members, %{key: "s1", members: members}} = result
    assert "alice" in members
  end

  test "set_members on missing key returns not_found", %{pid: pid} do
    result = GenServer.call(pid, {:set_members, %{key: "nonexistent"}})
    assert {:set_not_found, %{key: "nonexistent"}} = result
  end

  test "set_remove then set_members excludes element", %{pid: pid} do
    GenServer.call(pid, {:set_add, %{key: "s1", element: "alice"}})
    GenServer.call(pid, {:set_add, %{key: "s1", element: "bob"}})
    GenServer.call(pid, {:set_remove, %{key: "s1", element: "alice"}})

    {:set_members, %{members: members}} = GenServer.call(pid, {:set_members, %{key: "s1"}})
    assert members == ["bob"]
  end

  test "set operations don't interfere with LWW operations", %{pid: pid} do
    # LWW put
    GenServer.call(pid, {:put, %{key: "x", value: "hello"}})
    # Set add with same key name
    GenServer.call(pid, {:set_add, %{key: "x", element: "alice"}})

    # LWW get should return "hello"
    {:value, %{key: "x", val: "hello", found: :true}} =
      GenServer.call(pid, {:get, %{key: "x"}})

    # Set get should return ["alice"]
    {:set_members, %{key: "x", members: members}} =
      GenServer.call(pid, {:set_members, %{key: "x"}})

    assert members == ["alice"]
  end

  test "set_sync merges remote set store", %{pid: pid} do
    GenServer.call(pid, {:set_add, %{key: "s1", element: "alice"}})

    # Build remote set_store with "bob" in s1
    remote_set = VorDB.OrSet.add_element(
      VorDB.OrSet.empty(),
      "bob",
      VorDB.OrSet.make_tag(:remote, 1000, 1)
    )

    remote_store = %{"s1" => remote_set}
    GenServer.cast(pid, {:set_sync, %{remote_set_store: remote_store}})

    # Synchronize
    GenServer.call(pid, {:get_stores, %{}})

    {:set_members, %{members: members}} = GenServer.call(pid, {:set_members, %{key: "s1"}})
    assert "alice" in members
    assert "bob" in members
  end

  test "set state persists across restart", %{pid: pid} do
    GenServer.call(pid, {:set_add, %{key: "s1", element: "alice"}})
    GenServer.call(pid, {:set_add, %{key: "s1", element: "bob"}})

    GenServer.stop(pid, :normal, 5_000)
    {:ok, pid2} = GenServer.start_link(Vor.Agent.KvStore, [node_id: :test_node], [])

    {:set_members, %{members: members}} = GenServer.call(pid2, {:set_members, %{key: "s1"}})
    assert "alice" in members
    assert "bob" in members

    GenServer.stop(pid2)
  end
end
