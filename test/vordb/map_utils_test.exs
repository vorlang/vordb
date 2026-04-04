defmodule VorDB.MapUtilsTest do
  use ExUnit.Case, async: true

  alias VorDB.MapUtils

  test "select_keys returns subset of map" do
    assert MapUtils.select_keys(%{"a" => 1, "b" => 2, "c" => 3}, ["a", "c"]) ==
             %{"a" => 1, "c" => 3}
  end

  test "select_keys with missing keys returns only present ones" do
    assert MapUtils.select_keys(%{"a" => 1}, ["a", "b"]) == %{"a" => 1}
  end

  test "select_keys with empty key list returns empty map" do
    assert MapUtils.select_keys(%{"a" => 1}, []) == %{}
  end

  test "select_keys with empty source returns empty map" do
    assert MapUtils.select_keys(%{}, ["a"]) == %{}
  end
end
