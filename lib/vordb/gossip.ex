defmodule VorDB.Gossip do
  @moduledoc """
  Gossip protocol — per-vnode delta gossip with ACK, full-state on demand.

  Delta gossip: each vnode's Vor `every` block calls send_vnode_deltas/2.
  Deltas include sequence numbers; peers ACK receipt. Unacked deltas are retried.
  Full-state sync: available on demand via send_full_sync/0 (no periodic timer).
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  ## Public API — called by Vor agent's every block via extern

  def send_vnode_deltas(_node_id, vnode_id) do
    peers = Node.list()

    for peer <- peers do
      {seq, deltas} = VorDB.DirtyTracker.take_deltas(vnode_id, peer)

      if seq > 0 do
        send_type_deltas(peer, vnode_id, :lww, deltas.lww)
        send_type_deltas(peer, vnode_id, :set, deltas.set)
        send_type_deltas(peer, vnode_id, :counter, deltas.counter)

        # Request ACK from peer
        :erpc.cast(peer, fn ->
          VorDB.VnodeRouter.cast_vnode(vnode_id,
            {:delta_ack, %{from_node: peer, vnode_id: vnode_id, seq: seq}})
        end)
      end
    end

    :ok
  end

  ## Public API — on-demand full sync (for node join, admin, recovery)

  def send_full_sync do
    peers = Node.list()

    if peers != [] do
      for vnode_index <- VorDB.VnodeRouter.all_vnodes() do
        case VorDB.VnodeRouter.call_vnode(vnode_index, {:get_stores, %{}}) do
          {:stores, %{lww: lww, sets: sets, counters: counters}} ->
            for peer <- peers do
              if map_size(lww) > 0,
                do: cast_to_peer_vnode(peer, vnode_index, {:lww_sync, %{remote_lww_store: lww}})

              if map_size(sets) > 0,
                do: cast_to_peer_vnode(peer, vnode_index, {:set_sync, %{remote_set_store: sets}})

              if map_size(counters) > 0,
                do:
                  cast_to_peer_vnode(
                    peer,
                    vnode_index,
                    {:counter_sync, %{remote_counter_store: counters}}
                  )
            end

          _ ->
            :ok
        end
      end

      Logger.info("Full sync sent to #{length(peers)} peers")
    end
  end

  ## GenServer — minimal, no periodic timers

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  ## Delta helpers

  defp send_type_deltas(_peer, _vnode, _type, []), do: :ok

  defp send_type_deltas(peer, vnode_index, :lww, keys) do
    case VorDB.VnodeRouter.call_vnode(vnode_index, {:get_lww_entries, %{keys: keys}}) do
      {:lww_entries, %{entries: entries}} when map_size(entries) > 0 ->
        cast_to_peer_vnode(peer, vnode_index, {:lww_sync, %{remote_lww_store: entries}})

      _ ->
        :ok
    end
  end

  defp send_type_deltas(peer, vnode_index, :set, keys) do
    case VorDB.VnodeRouter.call_vnode(vnode_index, {:get_set_entries, %{keys: keys}}) do
      {:set_entries, %{entries: entries}} when map_size(entries) > 0 ->
        cast_to_peer_vnode(peer, vnode_index, {:set_sync, %{remote_set_store: entries}})

      _ ->
        :ok
    end
  end

  defp send_type_deltas(peer, vnode_index, :counter, keys) do
    case VorDB.VnodeRouter.call_vnode(vnode_index, {:get_counter_entries, %{keys: keys}}) do
      {:counter_entries, %{entries: entries}} when map_size(entries) > 0 ->
        cast_to_peer_vnode(peer, vnode_index, {:counter_sync, %{remote_counter_store: entries}})

      _ ->
        :ok
    end
  end

  defp cast_to_peer_vnode(peer, vnode_index, message) do
    :erpc.cast(peer, VorDB.VnodeRouter, :cast_vnode, [vnode_index, message])
  end
end
