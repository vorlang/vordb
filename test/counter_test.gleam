import gleeunit/should
import vordb/counter
import gleam/dict

pub fn empty_counter_has_value_zero_test() {
  counter.empty() |> counter.value() |> should.equal(0)
}

pub fn increment_increases_value_test() {
  counter.empty()
  |> counter.increment("n1", 1)
  |> counter.increment("n1", 1)
  |> counter.increment("n1", 1)
  |> counter.value()
  |> should.equal(3)
}

pub fn increment_by_custom_amount_test() {
  counter.empty()
  |> counter.increment("n1", 5)
  |> counter.value()
  |> should.equal(5)
}

pub fn decrement_decreases_value_test() {
  counter.empty()
  |> counter.increment("n1", 5)
  |> counter.decrement("n1", 2)
  |> counter.value()
  |> should.equal(3)
}

pub fn decrement_below_zero_test() {
  counter.empty()
  |> counter.decrement("n1", 3)
  |> counter.value()
  |> should.equal(-3)
}

pub fn different_nodes_test() {
  counter.empty()
  |> counter.increment("node_a", 10)
  |> counter.decrement("node_b", 3)
  |> counter.value()
  |> should.equal(7)
}

pub fn merge_takes_max_test() {
  let a = counter.empty() |> counter.increment("n1", 5)
  let b = counter.empty() |> counter.increment("n1", 3) |> counter.increment("n2", 7)
  counter.merge(a, b) |> counter.value() |> should.equal(12)
}

pub fn merge_disjoint_nodes_test() {
  let a = counter.empty() |> counter.increment("n1", 5)
  let b = counter.empty() |> counter.increment("n2", 3)
  counter.merge(a, b) |> counter.value() |> should.equal(8)
}

pub fn merge_both_p_and_n_test() {
  let a = counter.empty() |> counter.increment("n1", 10) |> counter.decrement("n1", 2)
  let b = counter.empty() |> counter.increment("n1", 8) |> counter.decrement("n1", 5)
  // p: max(10,8)=10, n: max(2,5)=5
  counter.merge(a, b) |> counter.value() |> should.equal(5)
}

pub fn merge_stores_per_key_test() {
  let store_a = dict.from_list([
    #("c1", counter.empty() |> counter.increment("n1", 10)),
  ])
  let store_b = dict.from_list([
    #("c1", counter.empty() |> counter.increment("n1", 5)),
    #("c2", counter.empty() |> counter.increment("n2", 3)),
  ])
  let merged = counter.merge_stores(store_a, store_b)

  case dict.get(merged, "c1") {
    Ok(c) -> counter.value(c) |> should.equal(10)
    Error(_) -> should.fail()
  }
  case dict.get(merged, "c2") {
    Ok(c) -> counter.value(c) |> should.equal(3)
    Error(_) -> should.fail()
  }
}
