/// Core type definitions for VorDB CRDT types.
import gleam/dict.{type Dict}
import gleam/set.{type Set}

/// LWW entry — the merge unit for key-value data
pub type LwwEntry {
  LwwEntry(value: String, timestamp: Int, node_id: String)
  Tombstone(timestamp: Int, node_id: String)
}

/// ORSWOT — OR-Set Without Tombstones
/// Bounded size: O(elements × nodes) instead of O(total_operations)
pub type Orswot {
  Orswot(
    entries: Dict(String, Dict(String, Int)),
    // element → {node_id → counter} ("dots")
    clock: Dict(String, Int),
    // node_id → max counter seen ("causal context")
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

/// CRDT type tag — determines which agent handler and merge function a bucket uses.
pub type CrdtType {
  CrdtLww
  CrdtOrswot
  CrdtPnCounter
}

/// Bucket — a named collection with per-bucket CRDT type, TTL, and replication.
pub type Bucket {
  Bucket(
    name: String,
    crdt_type: CrdtType,
    ttl_seconds: Int,       // 0 = no expiration
    replication_n: Int,     // 0 = use cluster default
    created_at: Int,
  )
}

/// Bucket operation errors.
pub type BucketError {
  BucketAlreadyExists
  BucketNotFound
  InvalidBucketConfig(reason: String)
  CrdtTypeMismatch(expected: CrdtType, got: String)
}
