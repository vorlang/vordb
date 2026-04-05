import gleeunit/should
import vordb/or_set
import gleam/dict

pub fn empty_set_has_no_members_test() {
  or_set.empty() |> or_set.read_elements() |> should.equal([])
}

pub fn add_element_makes_it_present_test() {
  let tag = or_set.make_tag("n1", 1000, 1)
  or_set.empty()
  |> or_set.add_element("alice", tag)
  |> or_set.read_elements()
  |> should.equal(["alice"])
}

pub fn add_multiple_elements_test() {
  or_set.empty()
  |> or_set.add_element("charlie", or_set.make_tag("n1", 1, 1))
  |> or_set.add_element("alice", or_set.make_tag("n1", 2, 2))
  |> or_set.add_element("bob", or_set.make_tag("n1", 3, 3))
  |> or_set.read_elements()
  |> should.equal(["alice", "bob", "charlie"])
}

pub fn add_duplicate_is_idempotent_in_read_test() {
  or_set.empty()
  |> or_set.add_element("alice", or_set.make_tag("n1", 1, 1))
  |> or_set.add_element("alice", or_set.make_tag("n1", 2, 2))
  |> or_set.read_elements()
  |> should.equal(["alice"])
}

pub fn remove_makes_absent_test() {
  or_set.empty()
  |> or_set.add_element("alice", or_set.make_tag("n1", 1, 1))
  |> or_set.remove_element("alice")
  |> or_set.read_elements()
  |> should.equal([])
}

pub fn remove_then_readd_add_wins_test() {
  or_set.empty()
  |> or_set.add_element("alice", or_set.make_tag("n1", 1, 1))
  |> or_set.remove_element("alice")
  |> or_set.add_element("alice", or_set.make_tag("n1", 2, 2))
  |> or_set.read_elements()
  |> should.equal(["alice"])
}

pub fn remove_nonexistent_is_noop_test() {
  or_set.empty()
  |> or_set.remove_element("alice")
  |> or_set.read_elements()
  |> should.equal([])
}

pub fn concurrent_add_and_remove_add_wins_test() {
  let tag1 = or_set.make_tag("n1", 1, 1)
  let tag2 = or_set.make_tag("n2", 1, 1)

  // Set B: add with tag1, then remove (tombstones tag1)
  let set_b =
    or_set.empty()
    |> or_set.add_element("alice", tag1)
    |> or_set.remove_element("alice")

  // Set C: add with tag2 (concurrent, different tag)
  let set_c = or_set.empty() |> or_set.add_element("alice", tag2)

  // Merge: alice survives (tag2 not in B's tombstones)
  or_set.merge(set_b, set_c)
  |> or_set.read_elements()
  |> should.equal(["alice"])
}

pub fn merge_combines_elements_test() {
  let set_a = or_set.empty() |> or_set.add_element("alice", or_set.make_tag("n1", 1, 1))
  let set_b = or_set.empty() |> or_set.add_element("bob", or_set.make_tag("n2", 1, 1))
  or_set.merge(set_a, set_b)
  |> or_set.read_elements()
  |> should.equal(["alice", "bob"])
}

pub fn merge_stores_per_key_test() {
  let store_a = dict.from_list([
    #("s1", or_set.empty() |> or_set.add_element("alice", or_set.make_tag("n1", 1, 1))),
  ])
  let store_b = dict.from_list([
    #("s1", or_set.empty() |> or_set.add_element("bob", or_set.make_tag("n2", 1, 1))),
    #("s2", or_set.empty() |> or_set.add_element("charlie", or_set.make_tag("n2", 2, 2))),
  ])
  let merged = or_set.merge_stores(store_a, store_b)

  case dict.get(merged, "s1") {
    Ok(s) -> or_set.read_elements(s) |> should.equal(["alice", "bob"])
    Error(_) -> should.fail()
  }
  case dict.get(merged, "s2") {
    Ok(s) -> or_set.read_elements(s) |> should.equal(["charlie"])
    Error(_) -> should.fail()
  }
}
