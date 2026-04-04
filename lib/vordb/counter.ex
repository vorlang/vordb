defmodule VorDB.Counter do
  @moduledoc """
  PN-Counter CRDT implementation (subsumes G-Counter).

  A pair of G-Counters: `p` for increments, `n` for decrements.
  Value = sum(p) - sum(n). Merge takes max of each node's count per side.
  Called as extern from the Vor KvStore agent.
  """

  def empty do
    %{p: %{}, n: %{}}
  end

  def increment(counter, node_id, amount) when amount > 0 do
    current = Map.get(counter.p, node_id, 0)
    %{counter | p: Map.put(counter.p, node_id, current + amount)}
  end

  def decrement(counter, node_id, amount) when amount > 0 do
    current = Map.get(counter.n, node_id, 0)
    %{counter | n: Map.put(counter.n, node_id, current + amount)}
  end

  def value(counter) do
    p_sum = counter.p |> Map.values() |> Enum.sum()
    n_sum = counter.n |> Map.values() |> Enum.sum()
    p_sum - n_sum
  end

  def merge(local, remote) do
    %{
      p: merge_gcounters(local.p, remote.p),
      n: merge_gcounters(local.n, remote.n)
    }
  end

  def merge_stores(local_store, remote_store) do
    Map.merge(local_store, remote_store, fn _key, local_counter, remote_counter ->
      merge(local_counter, remote_counter)
    end)
  end

  @doc "Get counter for key, or empty if not present."
  def get_or_empty(store, key) do
    Map.get(store, key, empty())
  end

  @doc "Check if key exists in store. Returns :true or :false."
  def has_key(store, key) do
    if Map.has_key?(store, key), do: :true, else: :false
  end

  defp merge_gcounters(a, b) do
    Map.merge(a, b, fn _node_id, count_a, count_b ->
      max(count_a, count_b)
    end)
  end
end
