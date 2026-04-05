/// Consistent hashing ring — maps keys to partitions and partitions to nodes.
/// Pure functions, no side effects. The ring is a value computed from membership.
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/order
import gleam/string

/// The ring: maps partition indices to owning nodes.
pub type Ring {
  Ring(
    size: Int,
    n_val: Int,
    partitions: Dict(Int, String),
    nodes: List(String),
    version: Int,
  )
}

/// A partition that needs to move between nodes on ring change.
pub type PartitionTransfer {
  PartitionTransfer(partition: Int, from_node: String, to_node: String)
}

/// Create a new ring from a sorted list of node names.
/// Ring size must be > 0. Nodes must be non-empty.
pub fn new(ring_size: Int, n_val: Int, nodes: List(String)) -> Ring {
  let sorted_nodes = list.sort(nodes, by: fn(a, b) {
    string.compare(a, b)
  })
  let num_nodes = list.length(sorted_nodes)
  let partitions = case num_nodes {
    0 -> dict.new()
    _ ->
      range(0, ring_size - 1)
      |> list.fold(dict.new(), fn(acc, p) {
        let node_index = p % num_nodes
        let assert Ok(node) = list_at(sorted_nodes, node_index)
        dict.insert(acc, p, node)
      })
  }
  Ring(
    size: ring_size,
    n_val: n_val,
    partitions: partitions,
    nodes: sorted_nodes,
    version: 1,
  )
}

/// Which partition does this key belong to?
pub fn key_to_partition(ring: Ring, key: String) -> Int {
  phash2(key, ring.size)
}

/// Which node is the primary owner of this partition?
pub fn partition_owner(ring: Ring, partition: Int) -> Result(String, Nil) {
  dict.get(ring.partitions, partition)
}

/// The preference list: up to N distinct nodes responsible for this partition.
/// First node is primary, rest are replicas walking clockwise.
pub fn preference_list(ring: Ring, partition: Int) -> List(String) {
  let num_nodes = list.length(ring.nodes)
  let target_n = int.min(ring.n_val, num_nodes)
  case target_n {
    0 -> []
    _ -> collect_distinct_nodes(ring, partition, target_n, num_nodes, [])
  }
}

/// The preference list for a key (convenience).
pub fn key_nodes(ring: Ring, key: String) -> List(String) {
  preference_list(ring, key_to_partition(ring, key))
}

/// Which partitions does this node own as primary?
pub fn node_partitions(ring: Ring, node: String) -> List(Int) {
  ring.partitions
  |> dict.to_list()
  |> list.filter(fn(pair) { pair.1 == node })
  |> list.map(fn(pair) { pair.0 })
  |> list.sort(by: int.compare)
}

/// All partitions this node is responsible for (primary + replica).
pub fn node_all_partitions(ring: Ring, node: String) -> List(Int) {
  range(0, ring.size - 1)
  |> list.filter(fn(p) {
    let pref = preference_list(ring, p)
    list.contains(pref, node)
  })
  |> list.sort(by: int.compare)
}

/// Create a new ring with a node added.
pub fn add_node(ring: Ring, node: String) -> Ring {
  case list.contains(ring.nodes, node) {
    True -> ring
    False -> {
      let new_nodes = [node, ..ring.nodes]
      let new_ring = new(ring.size, ring.n_val, new_nodes)
      Ring(..new_ring, version: ring.version + 1)
    }
  }
}

/// Create a new ring with a node removed.
pub fn remove_node(ring: Ring, node: String) -> Ring {
  case list.contains(ring.nodes, node) {
    False -> ring
    True -> {
      let new_nodes = list.filter(ring.nodes, fn(n) { n != node })
      let new_ring = new(ring.size, ring.n_val, new_nodes)
      Ring(..new_ring, version: ring.version + 1)
    }
  }
}

/// Compute which partitions changed primary ownership between two rings.
pub fn diff(old_ring: Ring, new_ring: Ring) -> List(PartitionTransfer) {
  let max_size = int.max(old_ring.size, new_ring.size)
  range(0, max_size - 1)
  |> list.filter_map(fn(p) {
    let old_owner = dict.get(old_ring.partitions, p)
    let new_owner = dict.get(new_ring.partitions, p)
    case old_owner, new_owner {
      Ok(from), Ok(to) if from != to ->
        Ok(PartitionTransfer(partition: p, from_node: from, to_node: to))
      _, _ -> Error(Nil)
    }
  })
}

/// Ring version number.
pub fn version(ring: Ring) -> Int {
  ring.version
}

/// Serialize ring to binary (Erlang term_to_binary).
pub fn to_binary(ring: Ring) -> BitArray {
  do_to_binary(ring)
}

/// Deserialize ring from binary.
pub fn from_binary(data: BitArray) -> Result(Ring, Nil) {
  do_from_binary(data)
}

// --- FFI ---

@external(erlang, "erlang", "phash2")
fn phash2(key: String, range: Int) -> Int

@external(erlang, "erlang", "term_to_binary")
fn do_to_binary(term: Ring) -> BitArray

@external(erlang, "vordb_ffi", "ring_from_binary")
fn do_from_binary(data: BitArray) -> Result(Ring, Nil)

// --- Private helpers ---

fn collect_distinct_nodes(
  ring: Ring,
  start_partition: Int,
  target: Int,
  num_nodes: Int,
  acc: List(String),
) -> List(String) {
  case list.length(acc) >= target {
    True -> list.reverse(acc)
    False -> {
      let offset = list.length(acc)
      let p = { start_partition + offset } % ring.size
      case dict.get(ring.partitions, p) {
        Ok(node) -> {
          case list.contains(acc, node) {
            True -> {
              // Skip duplicate, try next partition
              let next_p = { start_partition + offset + 1 } % ring.size
              collect_from_offset(ring, start_partition, offset + 1, target, num_nodes, acc)
            }
            False ->
              collect_distinct_nodes(ring, start_partition, target, num_nodes, [node, ..acc])
          }
        }
        Error(_) -> list.reverse(acc)
      }
    }
  }
}

fn collect_from_offset(
  ring: Ring,
  start: Int,
  offset: Int,
  target: Int,
  num_nodes: Int,
  acc: List(String),
) -> List(String) {
  case list.length(acc) >= target || offset >= ring.size {
    True -> list.reverse(acc)
    False -> {
      let p = { start + offset } % ring.size
      case dict.get(ring.partitions, p) {
        Ok(node) -> {
          case list.contains(acc, node) {
            True -> collect_from_offset(ring, start, offset + 1, target, num_nodes, acc)
            False ->
              collect_from_offset(ring, start, offset + 1, target, num_nodes, [node, ..acc])
          }
        }
        Error(_) -> list.reverse(acc)
      }
    }
  }
}

fn range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..range(from + 1, to)]
  }
}

fn list_at(lst: List(a), index: Int) -> Result(a, Nil) {
  case lst, index {
    [head, ..], 0 -> Ok(head)
    [_, ..tail], n if n > 0 -> list_at(tail, n - 1)
    _, _ -> Error(Nil)
  }
}
