defmodule VorDB.DirtyTracker do
  @moduledoc """
  Tracks which keys have changed since the last delta sync to each peer per vnode.
  Supports ACK-based confirmation: dirty entries cleared only on ACK, not on send.

  State per {vnode, peer}:
    - dirty: MapSet of dirty keys per type (accumulated since last confirmed)
    - pending: map of seq => {keys_by_type, sent_at} (sent but unACKed)
    - next_seq: next sequence number to assign
  """

  use GenServer

  @ack_timeout_ms 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def mark_dirty(vnode_index, type, key) do
    GenServer.cast(__MODULE__, {:mark_dirty, vnode_index, type, key})
  end

  def mark_dirty_keys(vnode_index, type, keys) when is_map(keys) do
    GenServer.cast(__MODULE__, {:mark_dirty_keys, vnode_index, type, Map.keys(keys)})
  end

  def mark_dirty_keys(vnode_index, type, keys) when is_list(keys) do
    GenServer.cast(__MODULE__, {:mark_dirty_keys, vnode_index, type, keys})
  end

  @doc "Take dirty keys for a vnode+peer, assign a sequence number. Returns {seq, deltas}."
  def take_deltas(vnode_index, peer) do
    GenServer.call(__MODULE__, {:take_deltas, vnode_index, peer})
  end

  @doc "Confirm a sequence was received by peer. Clears those entries from dirty."
  def confirm_ack(vnode_index, peer, seq) do
    GenServer.cast(__MODULE__, {:confirm_ack, vnode_index, peer, seq})
  end

  def add_peer(peer) do
    GenServer.cast(__MODULE__, {:add_peer, peer})
  end

  def remove_peer(peer) do
    GenServer.cast(__MODULE__, {:remove_peer, peer})
  end

  @impl true
  def init(opts) do
    peers = Keyword.get(opts, :peers, [])
    num_vnodes = Keyword.get(opts, :num_vnodes, Application.get_env(:vordb, :num_vnodes, 16))

    trackers =
      for vnode <- 0..(num_vnodes - 1),
          peer <- peers,
          into: %{} do
        {{vnode, peer}, empty_tracker()}
      end

    {:ok, %{trackers: trackers, peers: peers, num_vnodes: num_vnodes}}
  end

  @impl true
  def handle_cast({:mark_dirty, vnode_index, type, key}, state) do
    trackers =
      Enum.reduce(state.peers, state.trackers, fn peer, acc ->
        ck = {vnode_index, peer}

        case Map.get(acc, ck) do
          nil -> acc
          t -> Map.put(acc, ck, update_in_dirty(t, type, &MapSet.put(&1, key)))
        end
      end)

    {:noreply, %{state | trackers: trackers}}
  end

  def handle_cast({:mark_dirty_keys, _vnode_index, _type, []}, state), do: {:noreply, state}

  def handle_cast({:mark_dirty_keys, vnode_index, type, keys}, state) do
    key_set = MapSet.new(keys)

    trackers =
      Enum.reduce(state.peers, state.trackers, fn peer, acc ->
        ck = {vnode_index, peer}

        case Map.get(acc, ck) do
          nil -> acc
          t -> Map.put(acc, ck, update_in_dirty(t, type, &MapSet.union(&1, key_set)))
        end
      end)

    {:noreply, %{state | trackers: trackers}}
  end

  def handle_cast({:confirm_ack, vnode_index, peer, seq}, state) do
    ck = {vnode_index, peer}

    trackers =
      case Map.get(state.trackers, ck) do
        nil ->
          state.trackers

        t ->
          Map.put(state.trackers, ck, %{t | pending: Map.delete(t.pending, seq)})
      end

    {:noreply, %{state | trackers: trackers}}
  end

  def handle_cast({:add_peer, peer}, state) do
    peers = [peer | state.peers] |> Enum.uniq()

    new_entries =
      for vnode <- 0..(state.num_vnodes - 1), into: %{} do
        {{vnode, peer}, empty_tracker()}
      end

    trackers = Map.merge(new_entries, state.trackers)
    {:noreply, %{state | trackers: trackers, peers: peers}}
  end

  def handle_cast({:remove_peer, peer}, state) do
    peers = List.delete(state.peers, peer)

    trackers =
      state.trackers
      |> Enum.reject(fn {{_vnode, p}, _t} -> p == peer end)
      |> Map.new()

    {:noreply, %{state | trackers: trackers, peers: peers}}
  end

  @impl true
  def handle_call({:take_deltas, vnode_index, peer}, _from, state) do
    ck = {vnode_index, peer}

    case Map.get(state.trackers, ck) do
      nil ->
        {:reply, {0, %{lww: [], set: [], counter: []}}, state}

      t ->
        # Expire old pending sequences — return their keys to dirty
        now = :erlang.system_time(:millisecond)
        {t, expired_keys} = expire_pending(t, now)

        # Re-add expired keys to dirty
        t = Enum.reduce(expired_keys, t, fn {type, keys}, acc ->
          update_in_dirty(acc, type, &MapSet.union(&1, keys))
        end)

        # Take current dirty as this sequence's payload
        result = %{
          lww: MapSet.to_list(t.dirty.lww),
          set: MapSet.to_list(t.dirty.set),
          counter: MapSet.to_list(t.dirty.counter)
        }

        has_data = result.lww != [] or result.set != [] or result.counter != []

        if has_data do
          seq = t.next_seq
          pending_entry = {%{lww: t.dirty.lww, set: t.dirty.set, counter: t.dirty.counter}, now}

          new_t = %{t |
            dirty: %{lww: MapSet.new(), set: MapSet.new(), counter: MapSet.new()},
            pending: Map.put(t.pending, seq, pending_entry),
            next_seq: seq + 1
          }

          trackers = Map.put(state.trackers, ck, new_t)
          {:reply, {seq, result}, %{state | trackers: trackers}}
        else
          trackers = Map.put(state.trackers, ck, t)
          {:reply, {0, result}, %{state | trackers: trackers}}
        end
    end
  end

  ## Private

  defp empty_tracker do
    %{
      dirty: %{lww: MapSet.new(), set: MapSet.new(), counter: MapSet.new()},
      pending: %{},
      next_seq: 1
    }
  end

  defp update_in_dirty(tracker, type, fun) do
    %{tracker | dirty: Map.update!(tracker.dirty, type, fun)}
  end

  defp expire_pending(tracker, now) do
    {expired, remaining} =
      Map.split_with(tracker.pending, fn {_seq, {_keys, sent_at}} ->
        now - sent_at > @ack_timeout_ms
      end)

    expired_keys =
      Enum.flat_map(expired, fn {_seq, {keys_by_type, _sent_at}} ->
        Enum.map(keys_by_type, fn {type, key_set} -> {type, key_set} end)
      end)

    {%{tracker | pending: remaining}, expired_keys}
  end
end
