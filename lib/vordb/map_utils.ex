defmodule VorDB.MapUtils do
  @moduledoc "Map utility functions called as Vor extern."

  def select_keys(source, keys) when is_map(source) and is_list(keys) do
    Map.take(source, keys)
  end
end
