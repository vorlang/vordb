defmodule VorDB.DirtyTracker do
  @moduledoc """
  Tracks which keys have changed since the last delta sync to each peer.
  Maintains per-peer dirty sets: peer_node => %{lww: MapSet, set: MapSet, counter: MapSet}
  """

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def mark_dirty(type, key) do
    GenServer.cast(__MODULE__, {:mark_dirty, type, key})
  end

  def mark_dirty_keys(type, keys) when is_map(keys) do
    GenServer.cast(__MODULE__, {:mark_dirty_keys, type, Map.keys(keys)})
  end

  def mark_dirty_keys(type, keys) when is_list(keys) do
    GenServer.cast(__MODULE__, {:mark_dirty_keys, type, keys})
  end

  def take_deltas(peer) do
    GenServer.call(__MODULE__, {:take_deltas, peer})
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
    dirty = Map.new(peers, fn peer -> {peer, empty_sets()} end)
    {:ok, %{dirty: dirty}}
  end

  @impl true
  def handle_cast({:mark_dirty, type, key}, state) do
    dirty =
      Map.new(state.dirty, fn {peer, sets} ->
        {peer, Map.update!(sets, type, &MapSet.put(&1, key))}
      end)

    {:noreply, %{state | dirty: dirty}}
  end

  def handle_cast({:mark_dirty_keys, _type, []}, state), do: {:noreply, state}

  def handle_cast({:mark_dirty_keys, type, keys}, state) do
    key_set = MapSet.new(keys)

    dirty =
      Map.new(state.dirty, fn {peer, sets} ->
        {peer, Map.update!(sets, type, &MapSet.union(&1, key_set))}
      end)

    {:noreply, %{state | dirty: dirty}}
  end

  def handle_cast({:add_peer, peer}, state) do
    dirty = Map.put_new(state.dirty, peer, empty_sets())
    {:noreply, %{state | dirty: dirty}}
  end

  def handle_cast({:remove_peer, peer}, state) do
    {:noreply, %{state | dirty: Map.delete(state.dirty, peer)}}
  end

  @impl true
  def handle_call({:take_deltas, peer}, _from, state) do
    case Map.get(state.dirty, peer) do
      nil ->
        {:reply, %{lww: [], set: [], counter: []}, state}

      sets ->
        result = %{
          lww: MapSet.to_list(sets.lww),
          set: MapSet.to_list(sets.set),
          counter: MapSet.to_list(sets.counter)
        }

        dirty = Map.put(state.dirty, peer, empty_sets())
        {:reply, result, %{state | dirty: dirty}}
    end
  end

  defp empty_sets, do: %{lww: MapSet.new(), set: MapSet.new(), counter: MapSet.new()}
end
