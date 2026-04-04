defmodule VorDB.OrSet do
  @moduledoc """
  OR-Set (Observed-Remove Set) CRDT implementation.

  Pure functions, no side effects. Called as extern from the Vor KvStore agent.

  Each element is tracked with unique tags. A remove only affects tags it has
  observed. Concurrent add and remove → add wins (new tag not yet observed).
  Merge is set union on both entries and tombstones.
  """

  def empty do
    %{entries: %{}, tombstones: %{}}
  end

  def make_tag(node_id, timestamp, counter) do
    %{node_id: node_id, timestamp: timestamp, counter: counter}
  end

  def add_element(set_state, element, tag) do
    entries = set_state.entries
    existing_tags = Map.get(entries, element, MapSet.new())
    new_tags = MapSet.put(existing_tags, tag)
    %{set_state | entries: Map.put(entries, element, new_tags)}
  end

  def remove_element(set_state, element) do
    entries = set_state.entries
    tombstones = set_state.tombstones

    case Map.get(entries, element) do
      nil ->
        set_state

      tags ->
        existing_tombstones = Map.get(tombstones, element, MapSet.new())
        new_tombstones = MapSet.union(existing_tombstones, tags)
        %{set_state | tombstones: Map.put(tombstones, element, new_tombstones)}
    end
  end

  def read_elements(set_state) do
    entries = set_state.entries
    tombstones = set_state.tombstones

    entries
    |> Enum.filter(fn {element, tags} ->
      dead_tags = Map.get(tombstones, element, MapSet.new())
      live_tags = MapSet.difference(tags, dead_tags)
      MapSet.size(live_tags) > 0
    end)
    |> Enum.map(fn {element, _tags} -> element end)
    |> Enum.sort()
  end

  def merge(local, remote) do
    merged_entries = merge_tag_maps(local.entries, remote.entries)
    merged_tombstones = merge_tag_maps(local.tombstones, remote.tombstones)
    %{entries: merged_entries, tombstones: merged_tombstones}
  end

  def merge_stores(local_store, remote_store) do
    Map.merge(local_store, remote_store, fn _key, local_set, remote_set ->
      merge(local_set, remote_set)
    end)
  end

  @doc "Get set for key, or empty if not present. Used by Vor agent to avoid nil."
  def get_or_empty(store, key) do
    Map.get(store, key, empty())
  end

  @doc "Check if key exists in store. Returns :true or :false."
  def has_key(store, key) do
    if Map.has_key?(store, key), do: :true, else: :false
  end

  defp merge_tag_maps(a, b) do
    Map.merge(a, b, fn _element, tags_a, tags_b ->
      MapSet.union(tags_a, tags_b)
    end)
  end
end
