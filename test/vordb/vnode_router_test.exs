defmodule VorDB.VnodeRouterTest do
  use ExUnit.Case, async: false

  alias VorDB.TestHelpers

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    TestHelpers.start_vnode_stack(:test_node)

    on_exit(fn ->
      TestHelpers.stop_vnode_stack()
      TestHelpers.cleanup_dir(dir)
    end)

    :ok
  end

  test "vnode_for_key is deterministic" do
    v1 = VorDB.VnodeRouter.vnode_for_key("mykey")
    v2 = VorDB.VnodeRouter.vnode_for_key("mykey")
    assert v1 == v2
  end

  test "vnode_for_key distributes across range" do
    keys = for i <- 1..1000, do: "key_#{i}"
    vnodes = Enum.map(keys, &VorDB.VnodeRouter.vnode_for_key/1) |> Enum.uniq() |> Enum.sort()
    num_vnodes = Application.get_env(:vordb, :num_vnodes, 4)
    assert length(vnodes) == num_vnodes
  end

  test "call routes to correct vnode and returns result" do
    {:ok, %{timestamp: _}} = VorDB.VnodeRouter.call("x", {:put, %{key: "x", value: "hello"}})
    {:value, %{val: "hello", found: :true}} = VorDB.VnodeRouter.call("x", {:get, %{key: "x"}})
  end

  test "different keys can route to different vnodes" do
    # Find two keys that hash to different vnodes
    {k1, k2} =
      Enum.reduce_while(1..1000, nil, fn i, _acc ->
        a = "a#{i}"
        b = "b#{i}"

        if VorDB.VnodeRouter.vnode_for_key(a) != VorDB.VnodeRouter.vnode_for_key(b) do
          {:halt, {a, b}}
        else
          {:cont, nil}
        end
      end)

    VorDB.VnodeRouter.call(k1, {:put, %{key: k1, value: "v1"}})
    VorDB.VnodeRouter.call(k2, {:put, %{key: k2, value: "v2"}})

    # Both keys accessible
    {:value, %{val: "v1", found: :true}} = VorDB.VnodeRouter.call(k1, {:get, %{key: k1}})
    {:value, %{val: "v2", found: :true}} = VorDB.VnodeRouter.call(k2, {:get, %{key: k2}})

    # They're in different vnodes
    v1 = VorDB.VnodeRouter.vnode_for_key(k1)
    v2 = VorDB.VnodeRouter.vnode_for_key(k2)
    assert v1 != v2
  end

  test "keys are distributed across vnodes" do
    keys = for i <- 1..50, do: "key_#{i}"
    for k <- keys, do: VorDB.VnodeRouter.call(k, {:put, %{key: k, value: k}})

    # Check each vnode has some keys
    vnode_counts =
      for v <- VorDB.VnodeRouter.all_vnodes() do
        {:stores, %{lww: store}} = VorDB.VnodeRouter.call_vnode(v, {:get_stores, %{}})
        map_size(store)
      end

    assert Enum.sum(vnode_counts) == 50
    assert Enum.all?(vnode_counts, &(&1 > 0))
  end

  test "set and counter operations route correctly" do
    VorDB.VnodeRouter.call("s1", {:set_add, %{key: "s1", element: "alice"}})
    VorDB.VnodeRouter.call("c1", {:counter_increment, %{key: "c1", amount: 5}})

    {:set_members, %{members: members}} =
      VorDB.VnodeRouter.call("s1", {:set_members, %{key: "s1"}})

    {:counter_value, %{val: 5}} =
      VorDB.VnodeRouter.call("c1", {:counter_value, %{key: "c1"}})

    assert "alice" in members
  end
end
