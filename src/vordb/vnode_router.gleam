/// Routes key operations to the correct vnode via consistent hashing.
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}

@external(erlang, "vordb_ffi", "phash2")
fn phash2(key: String, range: Int) -> Int

@external(erlang, "vordb_ffi", "registry_lookup_vnode")
fn registry_lookup_vnode(vnode_id: Int) -> Result(Pid, Dynamic)

@external(erlang, "vordb_ffi", "call_agent")
fn call_agent(pid: Pid, message: Dynamic) -> Dynamic

@external(erlang, "vordb_ffi", "cast_agent")
fn cast_agent(pid: Pid, message: Dynamic) -> Dynamic

/// Get the vnode index for a key.
pub fn vnode_for_key(key: String, num_vnodes: Int) -> Int {
  phash2(key, num_vnodes)
}

/// Call a specific vnode by index.
pub fn call_vnode(vnode_index: Int, message: Dynamic) -> Result(Dynamic, String) {
  case registry_lookup_vnode(vnode_index) {
    Ok(pid) -> Ok(call_agent(pid, message))
    Error(_) -> Error("vnode_not_found")
  }
}

/// Cast to a specific vnode by index.
pub fn cast_vnode(vnode_index: Int, message: Dynamic) -> Result(Nil, String) {
  case registry_lookup_vnode(vnode_index) {
    Ok(pid) -> {
      cast_agent(pid, message)
      Ok(Nil)
    }
    Error(_) -> Error("vnode_not_found")
  }
}

/// Route a call to the correct vnode for a key.
pub fn call(key: String, num_vnodes: Int, message: Dynamic) -> Result(Dynamic, String) {
  let vnode = vnode_for_key(key, num_vnodes)
  call_vnode(vnode, message)
}
