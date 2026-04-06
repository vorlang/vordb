import gleam/dict
import gleeunit/should
import vordb/or_set
import vordb/types.{type Orswot}

pub fn empty_set_has_no_members_test() {
  or_set.empty() |> or_set.members() |> should.equal([])
}

pub fn add_element_makes_it_present_test() {
  or_set.empty()
  |> or_set.add("alice", "n1")
  |> or_set.members()
  |> should.equal(["alice"])
}

pub fn add_multiple_elements_test() {
  or_set.empty()
  |> or_set.add("charlie", "n1")
  |> or_set.add("alice", "n1")
  |> or_set.add("bob", "n1")
  |> or_set.members()
  |> should.equal(["alice", "bob", "charlie"])
}

pub fn add_duplicate_is_idempotent_in_read_test() {
  or_set.empty()
  |> or_set.add("alice", "n1")
  |> or_set.add("alice", "n1")
  |> or_set.members()
  |> should.equal(["alice"])
}

pub fn remove_makes_absent_test() {
  or_set.empty()
  |> or_set.add("alice", "n1")
  |> or_set.remove("alice")
  |> or_set.members()
  |> should.equal([])
}

pub fn remove_then_readd_add_wins_test() {
  or_set.empty()
  |> or_set.add("alice", "n1")
  |> or_set.remove("alice")
  |> or_set.add("alice", "n1")
  |> or_set.members()
  |> should.equal(["alice"])
}

pub fn remove_nonexistent_is_noop_test() {
  or_set.empty()
  |> or_set.remove("alice")
  |> or_set.members()
  |> should.equal([])
}

pub fn concurrent_add_and_remove_add_wins_test() {
  // Node B: add alice, then remove
  let set_b =
    or_set.empty()
    |> or_set.add("alice", "n1")
    |> or_set.remove("alice")

  // Node C: concurrent add of alice (different node)
  let set_c = or_set.empty() |> or_set.add("alice", "n2")

  // Merge: alice survives — C's dot not dominated by B's clock
  or_set.merge(set_b, set_c)
  |> or_set.members()
  |> should.equal(["alice"])
}

pub fn observed_remove_wins_test() {
  // Node A adds alice
  let set_a = or_set.empty() |> or_set.add("alice", "n1")

  // Node B receives A's state via merge, then removes alice
  let set_b =
    or_set.merge(or_set.empty(), set_a)
    |> or_set.remove("alice")

  // Merge A and B: B observed A's add and removed it → alice gone
  or_set.merge(set_a, set_b)
  |> or_set.members()
  |> should.equal([])
}

pub fn merge_combines_elements_test() {
  let set_a = or_set.empty() |> or_set.add("alice", "n1")
  let set_b = or_set.empty() |> or_set.add("bob", "n2")
  or_set.merge(set_a, set_b)
  |> or_set.members()
  |> should.equal(["alice", "bob"])
}

pub fn merge_stores_per_key_test() {
  let store_a = dict.from_list([
    #("s1", or_set.empty() |> or_set.add("alice", "n1")),
  ])
  let store_b = dict.from_list([
    #("s1", or_set.empty() |> or_set.add("bob", "n2")),
    #("s2", or_set.empty() |> or_set.add("charlie", "n2")),
  ])
  let merged = or_set.merge_stores(store_a, store_b)

  case dict.get(merged, "s1") {
    Ok(s) -> or_set.members(s) |> should.equal(["alice", "bob"])
    Error(_) -> should.fail()
  }
  case dict.get(merged, "s2") {
    Ok(s) -> or_set.members(s) |> should.equal(["charlie"])
    Error(_) -> should.fail()
  }
}

pub fn bounded_size_after_churn_test() {
  // 100 add/remove cycles — ORSWOT stays small
  let final = add_remove_loop(or_set.empty(), 0, 100)
  or_set.members(final) |> should.equal([])
  // Clock should have exactly one entry
  should.equal(dict.size(or_set.clock(final)), 1)
}

fn add_remove_loop(set: Orswot, i: Int, max: Int) -> Orswot {
  case i >= max {
    True -> set
    False -> {
      let key = "msg_" <> int_to_string(i)
      let updated = set |> or_set.add(key, "n1") |> or_set.remove(key)
      add_remove_loop(updated, i + 1, max)
    }
  }
}

@external(erlang, "erlang", "integer_to_binary")
fn int_to_string(n: Int) -> String
