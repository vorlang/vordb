/// OR-Set (Observed-Remove Set) CRDT implementation.
/// Pure functions — all property-testable.
import gleam/dict
import gleam/list
import gleam/set
import gleam/string
import vordb/types.{type OrSetState, type UniqueTag, OrSetState, UniqueTag}

/// Create an empty OR-Set.
pub fn empty() -> OrSetState {
  OrSetState(entries: dict.new(), tombstones: dict.new())
}

/// Create a unique tag for add operations.
pub fn make_tag(node_id: String, timestamp: Int, counter: Int) -> UniqueTag {
  UniqueTag(node_id: node_id, timestamp: timestamp, counter: counter)
}

/// Add an element to the set with a unique tag.
pub fn add_element(
  state: OrSetState,
  element: String,
  tag: UniqueTag,
) -> OrSetState {
  let existing = case dict.get(state.entries, element) {
    Ok(tags) -> tags
    Error(_) -> set.new()
  }
  let new_tags = set.insert(existing, tag)
  OrSetState(..state, entries: dict.insert(state.entries, element, new_tags))
}

/// Remove an element — moves all current tags to tombstones.
pub fn remove_element(state: OrSetState, element: String) -> OrSetState {
  case dict.get(state.entries, element) {
    Error(_) -> state
    Ok(tags) -> {
      let existing_tombs = case dict.get(state.tombstones, element) {
        Ok(t) -> t
        Error(_) -> set.new()
      }
      let new_tombs = set.union(existing_tombs, tags)
      OrSetState(
        ..state,
        tombstones: dict.insert(state.tombstones, element, new_tombs),
      )
    }
  }
}

/// Read the set of present elements (entries minus tombstones).
pub fn read_elements(state: OrSetState) -> List(String) {
  state.entries
  |> dict.to_list()
  |> list.filter(fn(pair) {
    let #(element, tags) = pair
    let dead_tags = case dict.get(state.tombstones, element) {
      Ok(t) -> t
      Error(_) -> set.new()
    }
    let live_tags = set.difference(tags, dead_tags)
    set.size(live_tags) > 0
  })
  |> list.map(fn(pair) { pair.0 })
  |> list.sort(by: string.compare)
}

/// Merge two OR-Sets (set union on entries and tombstones).
pub fn merge(local: OrSetState, remote: OrSetState) -> OrSetState {
  OrSetState(
    entries: merge_tag_maps(local.entries, remote.entries),
    tombstones: merge_tag_maps(local.tombstones, remote.tombstones),
  )
}

/// Merge two OR-Set stores (per-key merge).
pub fn merge_stores(
  local: dict.Dict(String, OrSetState),
  remote: dict.Dict(String, OrSetState),
) -> dict.Dict(String, OrSetState) {
  dict.combine(local, remote, fn(l, r) { merge(l, r) })
}

/// Get set for key, or empty if not present.
pub fn get_or_empty(
  store: dict.Dict(String, OrSetState),
  key: String,
) -> OrSetState {
  case dict.get(store, key) {
    Ok(s) -> s
    Error(_) -> empty()
  }
}

/// Check if key exists in store.
pub fn has_key(store: dict.Dict(String, OrSetState), key: String) -> Bool {
  dict.has_key(store, key)
}

fn merge_tag_maps(
  a: dict.Dict(String, set.Set(UniqueTag)),
  b: dict.Dict(String, set.Set(UniqueTag)),
) -> dict.Dict(String, set.Set(UniqueTag)) {
  dict.combine(a, b, fn(tags_a, tags_b) { set.union(tags_a, tags_b) })
}
