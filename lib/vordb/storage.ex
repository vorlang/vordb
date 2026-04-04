defmodule VorDB.Storage do
  @moduledoc """
  RocksDB storage layer for VorDB.

  GenServer that owns the RocksDB handle. All reads/writes go through this
  process to ensure proper handle lifecycle.
  """

  use GenServer

  require Logger

  ## Public API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put(key, value) when is_binary(key) and is_map(value) do
    GenServer.call(__MODULE__, {:put, key, value})
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

  def put_all(entries) when is_map(entries) do
    GenServer.call(__MODULE__, {:put_all, entries})
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

  def handle_call({:put_all, entries}, _from, %{db: db} = state) do
    batch =
      Enum.map(entries, fn {key, value} ->
        {:put, key, VorDB.Serializer.encode(value)}
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
end
