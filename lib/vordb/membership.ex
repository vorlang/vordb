defmodule VorDB.Membership do
  @moduledoc """
  Dynamic cluster membership.

  Manages the list of active members. Supports join (via seed node)
  and leave. Membership changes propagate to DirtyTracker and Cluster.
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def members do
    GenServer.call(__MODULE__, :members)
  end

  def join(seed_node) do
    GenServer.call(__MODULE__, {:join, seed_node}, 30_000)
  end

  def leave do
    GenServer.call(__MODULE__, :leave)
  end

  @doc "Called by remote nodes to announce a new member."
  def handle_member_join(new_node) do
    GenServer.cast(__MODULE__, {:member_joined, new_node})
  end

  @doc "Called by remote nodes to announce departure."
  def handle_member_leave(leaving_node) do
    GenServer.cast(__MODULE__, {:member_left, leaving_node})
  end

  @impl true
  def init(_opts) do
    {:ok, %{members: [node()]}}
  end

  @impl true
  def handle_call(:members, _from, state) do
    {:reply, state.members, state}
  end

  def handle_call({:join, seed_node}, _from, state) do
    case Node.connect(seed_node) do
      true ->
        # Get current membership from seed
        remote_members =
          try do
            :erpc.call(seed_node, VorDB.Membership, :members, [], 10_000)
          catch
            _, _ -> [seed_node]
          end

        # Announce ourselves to all existing members
        for member <- remote_members, member != node() do
          :erpc.cast(member, VorDB.Membership, :handle_member_join, [node()])
        end

        # Build full member list
        all_members = Enum.uniq([node() | remote_members]) |> Enum.sort()

        # Update DirtyTracker with new peers
        for member <- all_members, member != node() do
          VorDB.DirtyTracker.add_peer(member)
        end

        # Trigger full state sync from seed
        spawn(fn ->
          Process.sleep(500)
          VorDB.Gossip.send_full_sync()
        end)

        Logger.info("Joined cluster via #{seed_node}. Members: #{inspect(all_members)}")
        {:reply, {:ok, all_members}, %{state | members: all_members}}

      false ->
        {:reply, {:error, :connect_failed}, state}

      :ignored ->
        {:reply, {:error, :not_distributed}, state}
    end
  end

  def handle_call(:leave, _from, state) do
    peers = state.members -- [node()]

    # Announce departure to all peers
    for peer <- peers do
      :erpc.cast(peer, VorDB.Membership, :handle_member_leave, [node()])
    end

    Logger.info("Leaving cluster. Notified #{length(peers)} peers.")
    {:reply, :ok, %{state | members: [node()]}}
  end

  @impl true
  def handle_cast({:member_joined, new_node}, state) do
    if new_node not in state.members do
      members = Enum.sort([new_node | state.members])
      VorDB.DirtyTracker.add_peer(new_node)
      Logger.info("Member joined: #{new_node}. Members: #{inspect(members)}")
      {:noreply, %{state | members: members}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:member_left, leaving_node}, state) do
    if leaving_node in state.members do
      members = List.delete(state.members, leaving_node)
      VorDB.DirtyTracker.remove_peer(leaving_node)
      Logger.info("Member left: #{leaving_node}. Members: #{inspect(members)}")
      {:noreply, %{state | members: members}}
    else
      {:noreply, state}
    end
  end
end
