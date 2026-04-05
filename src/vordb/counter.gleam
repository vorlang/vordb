/// PN-Counter CRDT implementation (subsumes G-Counter).
/// Pure functions — all property-testable.
import gleam/dict
import gleam/int
import vordb/types.{type PnCounter, PnCounter}

/// Create an empty PN-Counter.
pub fn empty() -> PnCounter {
  PnCounter(p: dict.new(), n: dict.new())
}

/// Increment the counter for a given node.
pub fn increment(counter: PnCounter, node_id: String, amount: Int) -> PnCounter {
  let current = case dict.get(counter.p, node_id) {
    Ok(v) -> v
    Error(_) -> 0
  }
  PnCounter(..counter, p: dict.insert(counter.p, node_id, current + amount))
}

/// Decrement the counter for a given node.
pub fn decrement(counter: PnCounter, node_id: String, amount: Int) -> PnCounter {
  let current = case dict.get(counter.n, node_id) {
    Ok(v) -> v
    Error(_) -> 0
  }
  PnCounter(..counter, n: dict.insert(counter.n, node_id, current + amount))
}

/// Get the current value (sum of P - sum of N).
pub fn value(counter: PnCounter) -> Int {
  let p_sum = counter.p |> dict.values() |> int.sum()
  let n_sum = counter.n |> dict.values() |> int.sum()
  p_sum - n_sum
}

/// Merge two PN-Counters (max per node per side).
pub fn merge(local: PnCounter, remote: PnCounter) -> PnCounter {
  PnCounter(
    p: merge_gcounters(local.p, remote.p),
    n: merge_gcounters(local.n, remote.n),
  )
}

/// Merge two counter stores (per-key merge).
pub fn merge_stores(
  local: dict.Dict(String, PnCounter),
  remote: dict.Dict(String, PnCounter),
) -> dict.Dict(String, PnCounter) {
  dict.combine(local, remote, fn(l, r) { merge(l, r) })
}

/// Get counter for key, or empty if not present.
pub fn get_or_empty(
  store: dict.Dict(String, PnCounter),
  key: String,
) -> PnCounter {
  case dict.get(store, key) {
    Ok(c) -> c
    Error(_) -> empty()
  }
}

/// Check if key exists in store.
pub fn has_key(store: dict.Dict(String, PnCounter), key: String) -> Bool {
  dict.has_key(store, key)
}

fn merge_gcounters(
  a: dict.Dict(String, Int),
  b: dict.Dict(String, Int),
) -> dict.Dict(String, Int) {
  dict.combine(a, b, fn(ca, cb) { int.max(ca, cb) })
}
