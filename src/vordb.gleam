/// VorDB — A CRDT-based distributed key-value store.
///
/// Public API. Routes operations to the correct vnode transparently.
import gleam/dynamic.{type Dynamic}
import vordb/vnode_router

/// Construct Vor agent messages via FFI (Erlang tuples with maps).
@external(erlang, "vordb_ffi", "make_put_msg")
fn make_put_msg(key: String, value: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_get_msg")
fn make_get_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_delete_msg")
fn make_delete_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_add_msg")
fn make_set_add_msg(key: String, element: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_remove_msg")
fn make_set_remove_msg(key: String, element: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_members_msg")
fn make_set_members_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_increment_msg")
fn make_counter_increment_msg(key: String, amount: Int) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_decrement_msg")
fn make_counter_decrement_msg(key: String, amount: Int) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_value_msg")
fn make_counter_value_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_get_stores_msg")
fn make_get_stores_msg() -> Dynamic

/// Put a key-value pair (LWW-Register).
pub fn put(key: String, value: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_put_msg(key, value))
}

/// Get a value by key (LWW-Register).
pub fn get(key: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_get_msg(key))
}

/// Delete a key (LWW-Register).
pub fn delete(key: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_delete_msg(key))
}

/// Add element to set (OR-Set).
pub fn set_add(key: String, element: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_set_add_msg(key, element))
}

/// Remove element from set (OR-Set).
pub fn set_remove(key: String, element: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_set_remove_msg(key, element))
}

/// Get set members (OR-Set).
pub fn set_members(key: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_set_members_msg(key))
}

/// Increment counter (PN-Counter).
pub fn counter_increment(key: String, amount: Int, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_counter_increment_msg(key, amount))
}

/// Decrement counter (PN-Counter).
pub fn counter_decrement(key: String, amount: Int, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_counter_decrement_msg(key, amount))
}

/// Get counter value (PN-Counter).
pub fn counter_value(key: String, num_vnodes: Int) -> Result(Dynamic, String) {
  vnode_router.call(key, num_vnodes, make_counter_value_msg(key))
}
