/// Core type definitions for VorDB CRDT types.
import gleam/dict.{type Dict}
import gleam/set.{type Set}

/// LWW entry — the merge unit for key-value data
pub type LwwEntry {
  LwwEntry(value: String, timestamp: Int, node_id: String)
  Tombstone(timestamp: Int, node_id: String)
}

/// Unique tag for OR-Set operations
pub type UniqueTag {
  UniqueTag(node_id: String, timestamp: Int, counter: Int)
}

/// OR-Set state for a single key
pub type OrSetState {
  OrSetState(
    entries: Dict(String, Set(UniqueTag)),
    tombstones: Dict(String, Set(UniqueTag)),
  )
}

/// PN-Counter state for a single key
pub type PnCounter {
  PnCounter(
    p: Dict(String, Int),
    n: Dict(String, Int),
  )
}

/// Lookup result for LWW get operations
pub type LookupResult {
  Found(value: String)
  NotFound
}

/// Delta tracking types
pub type DirtyKeys {
  DirtyKeys(lww: Set(String), set_keys: Set(String), counter: Set(String))
}
