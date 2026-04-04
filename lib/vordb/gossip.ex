defmodule VorDB.Gossip do
  @moduledoc """
  Gossip dispatcher — periodic full-state broadcast to peers via Erlang distribution.

  Talks to the Vor-compiled Vor.Agent.KvStore gen_server to get state, then broadcasts
  to peer nodes. Remote peers receive state via GenServer.cast to their Vor.Agent.KvStore.

  Phase 0: full-state gossip (entire store map sent each interval).
  Phase 1: delta-state gossip (only changed keys since last sync per peer).
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :sync_interval_ms, 1_000)
    schedule_gossip(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:gossip, %{interval: interval} = state) do
    broadcast_sync()
    schedule_gossip(interval)
    {:noreply, state}
  end

  defp broadcast_sync do
    peers = Node.list()

    if peers != [] do
      {:store, %{data: store}} = GenServer.call(Vor.Agent.KvStore, {:get_store, %{}})

      for peer <- peers do
        :erpc.cast(peer, GenServer, :cast, [Vor.Agent.KvStore, {:sync, %{remote_store: store}}])
      end

      Logger.debug("Gossip sent to #{length(peers)} peers (#{map_size(store)} keys)")
    end
  end

  defp schedule_gossip(interval) do
    Process.send_after(self(), :gossip, interval)
  end
end
