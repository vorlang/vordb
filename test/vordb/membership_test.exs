defmodule VorDB.MembershipTest do
  use ExUnit.Case, async: false

  setup do
    # Ensure DirtyTracker is running (Membership calls it on join/leave)
    VorDB.TestHelpers.ensure_dirty_tracker()

    case GenServer.whereis(VorDB.Membership) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    {:ok, pid} = VorDB.Membership.start_link([])
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    :ok
  end

  test "initial membership is self only" do
    members = VorDB.Membership.members()
    assert members == [node()]
  end

  test "handle_member_join adds new member" do
    VorDB.Membership.handle_member_join(:new_node@host)
    Process.sleep(5)

    members = VorDB.Membership.members()
    assert :new_node@host in members
    assert node() in members
  end

  test "duplicate join is idempotent" do
    VorDB.Membership.handle_member_join(:new_node@host)
    VorDB.Membership.handle_member_join(:new_node@host)
    Process.sleep(5)

    members = VorDB.Membership.members()
    assert Enum.count(members, &(&1 == :new_node@host)) == 1
  end

  test "handle_member_leave removes member" do
    VorDB.Membership.handle_member_join(:leaving_node@host)
    Process.sleep(5)
    assert :leaving_node@host in VorDB.Membership.members()

    VorDB.Membership.handle_member_leave(:leaving_node@host)
    Process.sleep(5)
    refute :leaving_node@host in VorDB.Membership.members()
  end

  test "members returns sorted list" do
    VorDB.Membership.handle_member_join(:z_node@host)
    VorDB.Membership.handle_member_join(:a_node@host)
    Process.sleep(5)

    members = VorDB.Membership.members()
    assert members == Enum.sort(members)
  end
end
