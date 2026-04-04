defmodule VorDB.TestHelpers do
  @moduledoc "Shared test setup helpers."

  @doc "Start a fresh Storage GenServer with a unique temp directory. Returns {pid, dir}."
  def start_storage(_context \\ %{}) do
    dir = Path.join(System.tmp_dir!(), "vordb_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    # Stop any existing Storage process
    case GenServer.whereis(VorDB.Storage) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal, 5_000)
    end

    {:ok, pid} = VorDB.Storage.start_link(data_dir: dir)
    {pid, dir}
  end

  @doc "Start a KvStore Vor agent (not name-registered). Returns pid."
  def start_kv_store(node_id \\ :test_node) do
    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id], [])
    pid
  end

  @doc "Start a name-registered KvStore (for HTTP/Gossip tests that use the registered name)."
  def start_kv_store_registered(node_id \\ :test_node) do
    {:ok, pid} =
      GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id], name: Vor.Agent.KvStore)
    pid
  end

  @doc "Clean up a temp directory."
  def cleanup_dir(dir) do
    File.rm_rf!(dir)
  end

  @doc "Build an LWW entry map."
  def make_entry(value, timestamp, node_id) do
    %{value: value, timestamp: timestamp, node_id: node_id}
  end
end
