defmodule VorDB.OrSetTest do
  use ExUnit.Case, async: true

  alias VorDB.OrSet

  test "empty set has no members" do
    assert OrSet.read_elements(OrSet.empty()) == []
  end

  test "add element makes it present" do
    set = OrSet.empty()
    tag = OrSet.make_tag(:n1, 1000, 1)
    set = OrSet.add_element(set, "alice", tag)
    assert OrSet.read_elements(set) == ["alice"]
  end

  test "add multiple elements" do
    set = OrSet.empty()
    set = OrSet.add_element(set, "charlie", OrSet.make_tag(:n1, 1, 1))
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 2, 2))
    set = OrSet.add_element(set, "bob", OrSet.make_tag(:n1, 3, 3))
    assert OrSet.read_elements(set) == ["alice", "bob", "charlie"]
  end

  test "add duplicate element is idempotent in read" do
    set = OrSet.empty()
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 1, 1))
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 2, 2))
    assert OrSet.read_elements(set) == ["alice"]
  end

  test "remove element makes it absent" do
    set = OrSet.empty()
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 1, 1))
    set = OrSet.remove_element(set, "alice")
    assert OrSet.read_elements(set) == []
  end

  test "remove then re-add — add wins" do
    set = OrSet.empty()
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 1, 1))
    set = OrSet.remove_element(set, "alice")
    set = OrSet.add_element(set, "alice", OrSet.make_tag(:n1, 2, 2))
    assert OrSet.read_elements(set) == ["alice"]
  end

  test "remove nonexistent element is no-op" do
    set = OrSet.empty()
    set = OrSet.remove_element(set, "alice")
    assert OrSet.read_elements(set) == []
  end

  test "concurrent add and remove — add wins" do
    # Set B: add alice with tag1, then remove (tombstones tag1)
    set_b = OrSet.empty()
    tag1 = OrSet.make_tag(:n1, 1, 1)
    set_b = OrSet.add_element(set_b, "alice", tag1)
    set_b = OrSet.remove_element(set_b, "alice")

    # Set C: add alice with tag2 (concurrent, different tag)
    set_c = OrSet.empty()
    tag2 = OrSet.make_tag(:n2, 1, 1)
    set_c = OrSet.add_element(set_c, "alice", tag2)

    # Merge: alice survives — tag2 not in B's tombstones
    merged = OrSet.merge(set_b, set_c)
    assert OrSet.read_elements(merged) == ["alice"]
  end

  test "merge combines elements from both sets" do
    set_a = OrSet.add_element(OrSet.empty(), "alice", OrSet.make_tag(:n1, 1, 1))
    set_b = OrSet.add_element(OrSet.empty(), "bob", OrSet.make_tag(:n2, 1, 1))
    merged = OrSet.merge(set_a, set_b)
    assert OrSet.read_elements(merged) == ["alice", "bob"]
  end

  test "merge_stores merges per-key" do
    store_a = %{"s1" => OrSet.add_element(OrSet.empty(), "alice", OrSet.make_tag(:n1, 1, 1))}
    store_b = %{
      "s1" => OrSet.add_element(OrSet.empty(), "bob", OrSet.make_tag(:n2, 1, 1)),
      "s2" => OrSet.add_element(OrSet.empty(), "charlie", OrSet.make_tag(:n2, 2, 2))
    }

    merged = OrSet.merge_stores(store_a, store_b)
    assert OrSet.read_elements(merged["s1"]) == ["alice", "bob"]
    assert OrSet.read_elements(merged["s2"]) == ["charlie"]
  end
end
