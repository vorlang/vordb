defmodule VorDB.Serializer do
  @moduledoc "Term serialization for RocksDB values. Phase 0 uses :erlang.term_to_binary."

  def encode(term) do
    :erlang.term_to_binary(term)
  end

  def decode(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary)
  end
end
