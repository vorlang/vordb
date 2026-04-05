/// RocksDB storage — typed Gleam wrapper around vordb_ffi storage functions.
import gleam/dynamic.{type Dynamic}

/// Start the storage gen_server (managed by FFI).
@external(erlang, "vordb_ffi", "storage_start")
pub fn start(data_dir: String) -> Result(Dynamic, Dynamic)

/// Stop the storage gen_server.
@external(erlang, "vordb_ffi", "storage_stop")
pub fn stop() -> Dynamic

/// Put an LWW entry.
@external(erlang, "vordb_ffi", "storage_put_lww")
pub fn put_lww(vnode_id: Int, key: String, value: Dynamic) -> Dynamic

/// Get all LWW entries for a vnode.
@external(erlang, "vordb_ffi", "storage_get_all_lww")
pub fn get_all_lww(vnode_id: Int) -> Dynamic

/// Batch put LWW entries.
@external(erlang, "vordb_ffi", "storage_put_all_lww")
pub fn put_all_lww(vnode_id: Int, entries: Dynamic) -> Dynamic

/// Put an OR-Set entry.
@external(erlang, "vordb_ffi", "storage_put_set")
pub fn put_set(vnode_id: Int, key: String, value: Dynamic) -> Dynamic

/// Get all OR-Set entries for a vnode.
@external(erlang, "vordb_ffi", "storage_get_all_sets")
pub fn get_all_sets(vnode_id: Int) -> Dynamic

/// Batch put OR-Set entries.
@external(erlang, "vordb_ffi", "storage_put_all_sets")
pub fn put_all_sets(vnode_id: Int, entries: Dynamic) -> Dynamic

/// Put a counter entry.
@external(erlang, "vordb_ffi", "storage_put_counter")
pub fn put_counter(vnode_id: Int, key: String, value: Dynamic) -> Dynamic

/// Get all counter entries for a vnode.
@external(erlang, "vordb_ffi", "storage_get_all_counters")
pub fn get_all_counters(vnode_id: Int) -> Dynamic

/// Batch put counter entries.
@external(erlang, "vordb_ffi", "storage_put_all_counters")
pub fn put_all_counters(vnode_id: Int, entries: Dynamic) -> Dynamic
