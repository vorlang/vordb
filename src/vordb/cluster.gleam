/// Cluster connectivity and peer discovery.
import gleam/dynamic.{type Dynamic}

@external(erlang, "vordb_ffi", "node_self")
pub fn self_node() -> Dynamic

@external(erlang, "vordb_ffi", "node_list")
pub fn connected_peers() -> List(Dynamic)

@external(erlang, "vordb_ffi", "node_connect")
pub fn connect(node: String) -> Result(Nil, String)
