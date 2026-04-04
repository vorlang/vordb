defmodule VorDB.Gossip do
  @moduledoc """
  Gossip protocol — delta and full-state sync.

  Delta gossip (frequent): sends only changed keys per peer via DirtyTracker.
  Full-state gossip (infrequent): sends entire state as fallback for missed deltas.
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    delta_interval = Keyword.get(opts, :sync_interval_ms, 1_000)
    full_interval = Keyword.get(opts, :full_sync_interval_ms, 30_000)

    schedule_delta(delta_interval)
    schedule_full_sync(full_interval)

    {:ok, %{delta_interval: delta_interval, full_interval: full_interval}}
  end

  @impl true
  def handle_info(:delta_gossip, %{delta_interval: interval} = state) do
    send_deltas()
    schedule_delta(interval)
    {:noreply, state}
  end

  def handle_info(:full_sync, %{full_interval: interval} = state) do
    send_full_sync()
    schedule_full_sync(interval)
    {:noreply, state}
  end

  ## Delta Sync

  defp send_deltas do
    peers = Node.list()

    for peer <- peers do
      deltas = VorDB.DirtyTracker.take_deltas(peer)
      send_type_deltas(peer, :lww, deltas.lww)
      send_type_deltas(peer, :set, deltas.set)
      send_type_deltas(peer, :counter, deltas.counter)
    end
  end

  defp send_type_deltas(_peer, _type, []), do: :ok

  defp send_type_deltas(peer, :lww, keys) do
    {:lww_entries, %{entries: entries}} =
      GenServer.call(Vor.Agent.KvStore, {:get_lww_entries, %{keys: keys}})

    if map_size(entries) > 0 do
      cast_to_peer(peer, {:lww_sync, %{remote_lww_store: entries}})
    end
  end

  defp send_type_deltas(peer, :set, keys) do
    {:set_entries, %{entries: entries}} =
      GenServer.call(Vor.Agent.KvStore, {:get_set_entries, %{keys: keys}})

    if map_size(entries) > 0 do
      cast_to_peer(peer, {:set_sync, %{remote_set_store: entries}})
    end
  end

  defp send_type_deltas(peer, :counter, keys) do
    {:counter_entries, %{entries: entries}} =
      GenServer.call(Vor.Agent.KvStore, {:get_counter_entries, %{keys: keys}})

    if map_size(entries) > 0 do
      cast_to_peer(peer, {:counter_sync, %{remote_counter_store: entries}})
    end
  end

  ## Full-State Sync

  defp send_full_sync do
    peers = Node.list()

    if peers != [] do
      {:stores, %{lww: lww, sets: sets, counters: counters}} =
        GenServer.call(Vor.Agent.KvStore, {:get_stores, %{}})

      for peer <- peers do
        if map_size(lww) > 0 do
          cast_to_peer(peer, {:lww_sync, %{remote_lww_store: lww}})
        end

        if map_size(sets) > 0 do
          cast_to_peer(peer, {:set_sync, %{remote_set_store: sets}})
        end

        if map_size(counters) > 0 do
          cast_to_peer(peer, {:counter_sync, %{remote_counter_store: counters}})
        end
      end

      total = map_size(lww) + map_size(sets) + map_size(counters)
      Logger.debug("Full sync sent to #{length(peers)} peers (#{total} keys)")
    end
  end

  ## Helpers

  defp cast_to_peer(peer, message) do
    :erpc.cast(peer, GenServer, :cast, [Vor.Agent.KvStore, message])
  end

  defp schedule_delta(interval) do
    Process.send_after(self(), :delta_gossip, interval)
  end

  defp schedule_full_sync(interval) do
    Process.send_after(self(), :full_sync, interval)
  end
end
