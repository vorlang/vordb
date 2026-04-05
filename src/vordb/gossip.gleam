/// Gossip dispatch — typed wrapper around FFI gossip functions.
import gleam/dynamic.{type Dynamic}

/// Trigger full-state sync to all peers (on-demand).
@external(erlang, "vordb_ffi", "gossip_send_full_sync")
pub fn send_full_sync() -> Dynamic

/// Send vnode deltas (called by Vor agent's every block via FFI).
@external(erlang, "vordb_ffi", "gossip_send_vnode_deltas")
pub fn send_vnode_deltas(node_id: Dynamic, vnode_id: Int) -> Dynamic
