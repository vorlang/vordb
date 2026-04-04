defmodule VorDB.Storage do
  @moduledoc """
  RocksDB storage layer for VorDB.

  GenServer that owns the RocksDB handle. Keys are prefix-namespaced by
  CRDT type: "lww:key" for LWW-Register, "set:key" for OR-Set.
  """

  use GenServer

  require Logger

  ## Public API — type-specific

  def put_lww(key, value) when is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "lww:#{key}", value})
  end

  def put_set(key, value) when is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "set:#{key}", value})
  end

  def get_all_lww do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "lww:"})
  end

  def get_all_sets do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "set:"})
  end

  def put_all_lww(entries) when is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "lww:", entries})
  end

  def put_all_sets(entries) when is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "set:", entries})
  end

  def put_counter(key, value) when is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, "counter:#{key}", value})
  end

  def get_all_counters do
    GenServer.call(__MODULE__, {:get_all_by_prefix, "counter:"})
  end

  def put_all_counters(entries) when is_map(entries) do
    GenServer.call(__MODULE__, {:put_all_prefixed, "counter:", entries})
  end

  ## Public API — generic (used by tests)

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
end
