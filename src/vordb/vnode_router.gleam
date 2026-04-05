/// Routes key operations to the correct vnode via ring-based partitioning.
/// In Phase 2, identifies local vs remote partitions.
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}

/// Route a key operation to the correct local vnode.
/// Returns error if partition is not local (Phase 2: no forwarding yet).
pub fn call(key: String, num_vnodes: Int, message: Dynamic) -> Result(Dynamic, String) {
  let partition = ring_key_partition(key)
  let is_local = ring_is_local(partition)
  case is_local {
    True -> call_vnode(partition, message)
    False -> Error("partition_not_local")
  }
}

/// Call a specific vnode by partition index.
pub fn call_vnode(partition: Int, message: Dynamic) -> Result(Dynamic, String) {
  case registry_lookup_vnode(partition) {
    Ok(pid) -> Ok(call_agent(pid, message))
    Error(_) -> Error("vnode_not_found")
  }
}

/// Cast to a specific vnode by partition index.
pub fn cast_vnode(partition: Int, message: Dynamic) -> Result(Nil, String) {
  case registry_lookup_vnode(partition) {
    Ok(pid) -> {
      cast_agent(pid, message)
      Ok(Nil)
    }
    Error(_) -> Error("vnode_not_found")
  }
}

/// Get all partitions owned by this node.
pub fn all_vnodes() -> List(Int) {
  ring_my_partitions()
}

/// Get the partition for a key.
pub fn vnode_for_key(key: String, num_vnodes: Int) -> Int {
  ring_key_partition(key)
}

// FFI

@external(erlang, "vordb_ring_manager", "key_partition")
fn ring_key_partition(key: String) -> Int

@external(erlang, "vordb_ring_manager", "is_local")
fn ring_is_local(partition: Int) -> Bool

@external(erlang, "vordb_ring_manager", "my_partitions")
fn ring_my_partitions() -> List(Int)

@external(erlang, "vordb_ffi", "registry_lookup_vnode")
fn registry_lookup_vnode(vnode_id: Int) -> Result(Pid, Dynamic)

@external(erlang, "vordb_ffi", "call_agent")
fn call_agent(pid: Pid, message: Dynamic) -> Dynamic

@external(erlang, "vordb_ffi", "cast_agent")
fn cast_agent(pid: Pid, message: Dynamic) -> Dynamic
