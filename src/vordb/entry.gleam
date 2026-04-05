/// LWW-Register entry construction and lookup.
/// Called as extern from the Vor KvStore agent.
import gleam/dict
import vordb/types.{type LookupResult, Found, LwwEntry, NotFound, Tombstone}

const tombstone_marker = "__tombstone__"

/// Create a new LWW entry.
pub fn new(value: String, timestamp: Int, node_id: String) -> types.LwwEntry {
  LwwEntry(value: value, timestamp: timestamp, node_id: node_id)
}

/// Create a tombstone entry for deletion.
pub fn tombstone(timestamp: Int, node_id: String) -> types.LwwEntry {
  Tombstone(timestamp: timestamp, node_id: node_id)
}

/// Look up a key in the LWW store. Returns Found or NotFound.
pub fn lookup(
  store: dict.Dict(String, types.LwwEntry),
  key: String,
) -> LookupResult {
  case dict.get(store, key) {
    Ok(LwwEntry(value: v, ..)) -> Found(value: v)
    Ok(Tombstone(..)) -> NotFound
    Error(_) -> NotFound
  }
}

/// Get the tombstone marker string (for Vor extern compatibility).
pub fn tombstone_value() -> String {
  tombstone_marker
}
