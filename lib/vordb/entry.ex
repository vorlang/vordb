defmodule VorDB.Entry do
  @moduledoc """
  LWW-Register entry construction and lookup.

  Called as extern from the Vor KvStore agent. Handles map construction
  (GAP-005: map literal syntax not yet supported) and lookup logic
  (avoids nested if/else with variable binding issues in Vor).
  """

  @tombstone :__tombstone__

  def new(value, timestamp, node_id) do
    %{value: value, timestamp: timestamp, node_id: node_id}
  end

  def tombstone(timestamp, node_id) do
    %{value: @tombstone, timestamp: timestamp, node_id: node_id}
  end

  @doc "Look up a key in the store. Returns %{val: value, found: :true/:false}."
  def lookup(store, key) do
    case Map.get(store, key) do
      nil ->
        %{val: :none, found: :false}

      %{value: @tombstone} ->
        %{val: :none, found: :false}

      %{value: value} ->
        %{val: value, found: :true}
    end
  end

  def tombstone_value, do: @tombstone
end
