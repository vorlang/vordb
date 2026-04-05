defmodule VorDB.Storage do
  @moduledoc """
  RocksDB storage layer for VorDB.

  One RocksDB instance per node. Keys are prefixed by type and vnode index:
  "lww:05:mykey", "set:12:mykey", "counter:00:mykey".
  """

  use GenServer

  require Logger

  ## Public API — vnode-aware, type-specific

  def put_lww(vnode_id, key, value) when is_integer(vnode_id) and is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "lww:#{pad(vnode_id)}:#{key}", value})
  end

  def put_set(vnode_id, key, value) when is_integer(vnode_id) and is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "set:#{pad(vnode_id)}:#{key}", value})
  end

  def put_counter(vnode_id, key, value) when is_integer(vnode_id) and is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "counter:#{pad(vnode_id)}:#{key}", value})
  end

  def get_all_lww(vnode_id) when is_integer(vnode_id) do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "lww:#{pad(vnode_id)}:"})
  end

  def get_all_sets(vnode_id) when is_integer(vnode_id) do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "set:#{pad(vnode_id)}:"})
  end

  def get_all_counters(vnode_id) when is_integer(vnode_id) do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "counter:#{pad(vnode_id)}:"})
  end

  def put_all_lww(vnode_id, entries) when is_integer(vnode_id) and is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "lww:#{pad(vnode_id)}:", entries})
  end

  def put_all_sets(vnode_id, entries) when is_integer(vnode_id) and is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "set:#{pad(vnode_id)}:", entries})
  end

  def put_all_counters(vnode_id, entries) when is_integer(vnode_id) and is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "counter:#{pad(vnode_id)}:", entries})
  end

  ## Public API — generic (used by tests and migration)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(key) when is_binary(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  def delete(key) when is_binary(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  def get_all do
    GenServer.call(__MODULE__, :get_all)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    File.mkdir_p!(data_dir)

    db_opts = [
      {:create_if_missing, true},
      {:write_buffer_size, 64 * 1024 * 1024},
      {:max_open_files, 1000}
    ]

    case :rocksdb.open(String.to_charlist(data_dir), db_opts) do
      {:ok, db} ->
        Logger.info("VorDB.Storage opened at #{data_dir}")
        {:ok, %{db: db, data_dir: data_dir}}

      {:error, reason} ->
        {:stop, {:rocksdb_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, %{db: db} = state) do
    encoded = VorDB.Serializer.encode(value)

    case :rocksdb.put(db, key, encoded, []) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, key}, _from, %{db: db} = state) do
    case :rocksdb.get(db, key, []) do
      {:ok, binary} -> {:reply, {:ok, VorDB.Serializer.decode(binary)}, state}
      :not_found -> {:reply, :not_found, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, key}, _from, %{db: db} = state) do
    case :rocksdb.delete(db, key, []) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:get_all, _from, %{db: db} = state) do
    {:ok, iter} = :rocksdb.iterator(db, [])
    entries = iterate_all(iter, :rocksdb.iterator_move(iter, :first), %{})
    :rocksdb.iterator_close(iter)
    {:reply, entries, state}
  end

  def handle_call({:get_all_by_prefix, prefix}, _from, %{db: db} = state) do
    {:ok, iter} = :rocksdb.iterator(db, [])
    entries = iterate_by_prefix(iter, :rocksdb.iterator_move(iter, :first), prefix, %{})
    :rocksdb.iterator_close(iter)
    {:reply, entries, state}
  end

  def handle_call({:put_all_prefixed, prefix, entries}, _from, %{db: db} = state) do
    batch =
      Enum.map(entries, fn {key, value} ->
        {:put, "#{prefix}#{key}", VorDB.Serializer.encode(value)}
      end)

    case :rocksdb.write(db, batch, []) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def terminate(_reason, %{db: db}) do
    :rocksdb.close(db)
  end

  ## Private

  defp iterate_all(iter, {:ok, key, value}, acc) do
    entry = VorDB.Serializer.decode(value)
    acc = Map.put(acc, key, entry)
    iterate_all(iter, :rocksdb.iterator_move(iter, :next), acc)
  end

  defp iterate_all(_iter, {:error, :invalid_iterator}, acc), do: acc

  defp iterate_by_prefix(iter, {:ok, key, value}, prefix, acc) do
    prefix_len = byte_size(prefix)

    acc =
      case key do
        <<^prefix::binary-size(prefix_len), rest::binary>> ->
          entry = VorDB.Serializer.decode(value)
          Map.put(acc, rest, entry)

        _ ->
          acc
      end

    iterate_by_prefix(iter, :rocksdb.iterator_move(iter, :next), prefix, acc)
  end

  defp iterate_by_prefix(_iter, {:error, :invalid_iterator}, _prefix, acc), do: acc

  defp pad(vnode_id), do: String.pad_leading(Integer.to_string(vnode_id), 2, "0")
end
