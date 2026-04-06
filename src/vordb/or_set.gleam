/// ORSWOT (OR-Set Without Tombstones) — bounded-size set CRDT.
/// Replaces the old tag-based OR-Set. Same public API, no tombstone growth.
import gleam/dict
import gleam/int
import gleam/list
import gleam/result
import gleam/set
import gleam/string
import vordb/types.{type Orswot, Orswot}

/// Create an empty ORSWOT.
pub fn empty() -> Orswot {
  Orswot(entries: dict.new(), clock: dict.new())
}

/// Add an element. Increments this node's counter in the clock.
pub fn add(orswot: Orswot, element: String, node_id: String) -> Orswot {
  let new_counter = case dict.get(orswot.clock, node_id) {
    Ok(c) -> c + 1
    Error(_) -> 1
  }
  let new_clock = dict.insert(orswot.clock, node_id, new_counter)
  let new_dots = dict.from_list([#(node_id, new_counter)])
  let new_entries = dict.insert(orswot.entries, element, new_dots)
  Orswot(entries: new_entries, clock: new_clock)
}

/// Remove an element. No tombstone — the clock records what was observed.
pub fn remove(orswot: Orswot, element: String) -> Orswot {
  Orswot(..orswot, entries: dict.delete(orswot.entries, element))
}

/// Get all present elements (sorted).
pub fn members(orswot: Orswot) -> List(String) {
  orswot.entries
  |> dict.keys()
  |> list.sort(by: string.compare)
}

/// Number of present elements.
pub fn size(orswot: Orswot) -> Int {
  dict.size(orswot.entries)
}

/// Check if an element is present.
pub fn contains(orswot: Orswot, element: String) -> Bool {
  dict.has_key(orswot.entries, element)
}

/// Get the clock (for debugging).
pub fn clock(orswot: Orswot) -> dict.Dict(String, Int) {
  orswot.clock
}

/// Merge two ORSWOTs.
/// An element survives if it has dots NOT dominated by the other side's clock.
pub fn merge(a: Orswot, b: Orswot) -> Orswot {
  let merged_clock = dict.combine(a.clock, b.clock, fn(va, vb) {
    int.max(va, vb)
  })

  // Collect all elements from both sides
  let all_elements =
    set.union(
      set.from_list(dict.keys(a.entries)),
      set.from_list(dict.keys(b.entries)),
    )

  let merged_entries =
    all_elements
    |> set.to_list()
    |> list.filter_map(fn(element) {
      let a_dots = dict.get(a.entries, element) |> result.unwrap(dict.new())
      let b_dots = dict.get(b.entries, element) |> result.unwrap(dict.new())

      // Keep dots from A that are either:
      // - Not dominated by B's clock (concurrent add)
      // - Also present in B (both sides have it)
      let surviving_a = keep_surviving(a_dots, b.clock, b_dots)
      let surviving_b = keep_surviving(b_dots, a.clock, a_dots)

      // Merge surviving dots (max per node)
      let merged_dots =
        dict.combine(surviving_a, surviving_b, fn(ca, cb) {
          int.max(ca, cb)
        })

      case dict.size(merged_dots) > 0 {
        True -> Ok(#(element, merged_dots))
        False -> Error(Nil)
      }
    })
    |> dict.from_list()

  Orswot(entries: merged_entries, clock: merged_clock)
}

/// Merge two ORSWOT stores (per-key merge).
pub fn merge_stores(
  local: dict.Dict(String, Orswot),
  remote: dict.Dict(String, Orswot),
) -> dict.Dict(String, Orswot) {
  dict.combine(local, remote, fn(l, r) { merge(l, r) })
}

/// Get set for key, or empty if not present.
pub fn get_or_empty(
  store: dict.Dict(String, Orswot),
  key: String,
) -> Orswot {
  case dict.get(store, key) {
    Ok(s) -> s
    Error(_) -> empty()
  }
}

/// Check if key exists in store.
pub fn has_key(store: dict.Dict(String, Orswot), key: String) -> Bool {
  dict.has_key(store, key)
}

// --- Backward compatibility aliases (called by Vor agent via extern gleam) ---

/// Add element (Vor extern compatible — takes state, element, node_id).
pub fn add_element(state: Orswot, element: String, node_id: String) -> Orswot {
  add(state, element, node_id)
}

/// Remove element (Vor extern compatible).
pub fn remove_element(state: Orswot, element: String) -> Orswot {
  remove(state, element)
}

/// Read elements (Vor extern compatible).
pub fn read_elements(state: Orswot) -> List(String) {
  members(state)
}

// --- Private ---

/// A dot survives if:
/// - It's not dominated by the other clock (counter > other_clock[node]), OR
/// - The same element exists on the other side (both have it — keep it)
fn keep_surviving(
  dots: dict.Dict(String, Int),
  other_clock: dict.Dict(String, Int),
  other_dots: dict.Dict(String, Int),
) -> dict.Dict(String, Int) {
  dots
  |> dict.to_list()
  |> list.filter(fn(pair) {
    let #(node_id, counter) = pair
    let not_dominated = case dict.get(other_clock, node_id) {
      Ok(clock_counter) -> counter > clock_counter
      Error(_) -> True
    }
    let in_other = dict.has_key(other_dots, node_id)
    not_dominated || in_other
  })
  |> dict.from_list()
}
