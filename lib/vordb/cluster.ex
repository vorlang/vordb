defmodule VorDB.Cluster do
  @moduledoc """
  Cluster membership — static topology for Phase 0.

  Connects to configured peers on startup and monitors connections.
  Peer list is static from config; dynamic membership is Phase 3.
  """

  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def peers do
    Application.get_env(:vordb, :peers, [])
  end

  def connected_peers do
    Node.list()
  end

  @impl true
  def init(_opts) do
    :net_kernel.monitor_nodes(true)
    send(self(), :connect_peers)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:connect_peers, state) do
    for peer <- peers() do
      case Node.connect(peer) do
        true ->
          Logger.info("Connected to peer #{peer}")

        false ->
          Logger.warning("Failed to connect to peer #{peer}")

        :ignored ->
          :ok
      end
    end

    {:noreply, state}
  end

  def handle_info({:nodeup, node}, state) do
    Logger.info("Node joined: #{node}")
    {:noreply, state}
  end

  def handle_info({:nodedown, node}, state) do
    Logger.warning("Node left: #{node}")
    {:noreply, state}
  end
end
