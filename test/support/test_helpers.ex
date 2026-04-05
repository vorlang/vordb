defmodule VorDB.TestHelpers do
  @moduledoc "Shared test setup helpers."

  def start_storage(_context \\ %{}) do
    dir = Path.join(System.tmp_dir!(), "vordb_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    stop_if_alive(VorDB.Storage)
    {:ok, pid} = VorDB.Storage.start_link(data_dir: dir)
    {pid, dir}
  end

  def ensure_dirty_tracker do
    case GenServer.whereis(VorDB.DirtyTracker) do
      nil ->
        {:ok, _pid} = VorDB.DirtyTracker.start_link(peers: [], num_vnodes: 4)
        :ok

      _pid ->
        :ok
    end
  end

  @doc "Start a single KvStore Vor agent (not name-registered). vnode_id defaults to 0."
  def start_kv_store(node_id \\ :test_node, vnode_id \\ 0) do
    ensure_dirty_tracker()
    {:ok, pid} = GenServer.start_link(Vor.Agent.KvStore, [node_id: node_id, vnode_id: vnode_id, sync_interval_ms: 600_000], [])
    pid
  end

  @doc "Start the full vnode stack: Registry + VnodeSupervisor. For HTTP and integration tests."
  def start_vnode_stack(node_id \\ :test_node, num_vnodes \\ 4) do
    ensure_dirty_tracker()

    # Start Registry if not running
    case Registry.start_link(keys: :unique, name: VorDB.VnodeRegistry) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, sup_pid} =
      VorDB.VnodeSupervisor.start_link(node_id: node_id, num_vnodes: num_vnodes)

    sup_pid
  end

  @doc "Stop vnode stack."
  def stop_vnode_stack do
    stop_if_alive(VorDB.VnodeSupervisor)
  end

  def cleanup_dir(dir) do
    File.rm_rf!(dir)
  end

  def make_entry(value, timestamp, node_id) do
    %{value: value, timestamp: timestamp, node_id: node_id}
  end

  defp stop_if_alive(name) do
    case GenServer.whereis(name) do
      nil -> :ok
      pid ->
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
    end
  end
end
