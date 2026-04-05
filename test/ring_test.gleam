import gleam/dict
import gleam/int
import gleam/list
import gleeunit/should
import vordb/ring.{type Ring, PartitionTransfer, Ring}

fn make_ring(size: Int, n: Int, nodes: List(String)) -> Ring {
  ring.new(size, n, nodes)
}

// --- Construction ---

pub fn new_ring_has_correct_size_and_nodes_test() {
  let r = make_ring(256, 3, ["node1", "node2", "node3"])
  should.equal(r.size, 256)
  should.equal(r.n_val, 3)
  should.equal(list.length(r.nodes), 3)
}

pub fn ring_distributes_partitions_evenly_test() {
  let r = make_ring(256, 3, ["node1", "node2", "node3", "node4"])
  // Each of 4 nodes should own exactly 64 partitions
  should.equal(list.length(ring.node_partitions(r, "node1")), 64)
  should.equal(list.length(ring.node_partitions(r, "node2")), 64)
  should.equal(list.length(ring.node_partitions(r, "node3")), 64)
  should.equal(list.length(ring.node_partitions(r, "node4")), 64)
  // All partitions assigned
  should.equal(dict.size(r.partitions), 256)
}

pub fn ring_single_node_owns_all_test() {
  let r = make_ring(256, 3, ["only_node"])
  should.equal(list.length(ring.node_partitions(r, "only_node")), 256)
}

// --- Key to partition ---

pub fn key_to_partition_is_deterministic_test() {
  let r = make_ring(256, 3, ["a", "b", "c"])
  let p1 = ring.key_to_partition(r, "mykey")
  let p2 = ring.key_to_partition(r, "mykey")
  should.equal(p1, p2)
}

pub fn key_to_partition_independent_of_membership_test() {
  let r1 = make_ring(256, 3, ["a", "b", "c"])
  let r2 = make_ring(256, 3, ["a", "b", "c", "d", "e"])
  // Same key maps to same partition regardless of nodes
  should.equal(
    ring.key_to_partition(r1, "testkey"),
    ring.key_to_partition(r2, "testkey"),
  )
}

// --- Preference list ---

pub fn preference_list_has_n_entries_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let pref = ring.preference_list(r, 0)
  should.equal(list.length(pref), 3)
}

pub fn preference_list_contains_distinct_nodes_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let pref = ring.preference_list(r, 0)
  should.equal(list.length(pref), list.length(list.unique(pref)))
}

pub fn preference_list_primary_is_owner_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let assert Ok(owner) = ring.partition_owner(r, 10)
  let pref = ring.preference_list(r, 10)
  let assert [first, ..] = pref
  should.equal(first, owner)
}

pub fn preference_list_wraps_around_test() {
  let r = make_ring(8, 3, ["a", "b", "c", "d"])
  // Partition 7 (last) should wrap to partitions 0, 1, etc. for replicas
  let pref = ring.preference_list(r, 7)
  should.equal(list.length(pref), 3)
}

pub fn preference_list_full_replication_test() {
  // N == num_nodes → every preference list has all nodes
  let r = make_ring(256, 3, ["a", "b", "c"])
  let pref = ring.preference_list(r, 0)
  should.equal(list.length(pref), 3)
}

pub fn preference_list_n_greater_than_nodes_caps_test() {
  let r = make_ring(256, 3, ["a", "b"])
  let pref = ring.preference_list(r, 0)
  // Only 2 nodes available, can't have 3 replicas
  should.equal(list.length(pref), 2)
}

// --- Node queries ---

pub fn node_partitions_returns_correct_primaries_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let parts = ring.node_partitions(r, "n1")
  should.equal(list.length(parts), 64)
  // All returned partitions should have n1 as owner
  list.each(parts, fn(p) {
    let assert Ok(owner) = ring.partition_owner(r, p)
    should.equal(owner, "n1")
  })
}

pub fn node_all_partitions_includes_replicas_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let primary = ring.node_partitions(r, "n1")
  let all = ring.node_all_partitions(r, "n1")
  should.be_true(list.length(all) > list.length(primary))
}

pub fn node_all_partitions_with_n1_equals_primaries_test() {
  let r = make_ring(256, 1, ["n1", "n2", "n3", "n4"])
  let primary = ring.node_partitions(r, "n1")
  let all = ring.node_all_partitions(r, "n1")
  should.equal(list.length(primary), list.length(all))
}

// --- Add node ---

pub fn add_node_creates_ring_with_more_nodes_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r, "n4")
  should.equal(list.length(r2.nodes), 4)
  should.equal(dict.size(r2.partitions), 256)
}

pub fn add_node_redistributes_partitions_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r, "n4")
  should.equal(list.length(ring.node_partitions(r2, "n4")), 64)
}

pub fn add_node_increments_version_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r, "n4")
  should.equal(r2.version, r.version + 1)
}

pub fn add_node_idempotent_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r, "n1")
  should.equal(r2.version, r.version)
}

// --- Remove node ---

pub fn remove_node_creates_ring_without_node_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let r2 = ring.remove_node(r, "n4")
  should.equal(list.length(r2.nodes), 3)
  should.equal(list.length(ring.node_partitions(r2, "n4")), 0)
}

pub fn remove_node_redistributes_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let r2 = ring.remove_node(r, "n4")
  // Remaining 3 nodes split 256 partitions: ~85 each
  should.be_true(list.length(ring.node_partitions(r2, "n1")) > 64)
}

pub fn remove_node_increments_version_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let r2 = ring.remove_node(r, "n4")
  should.equal(r2.version, r.version + 1)
}

// --- Diff ---

pub fn diff_identical_rings_is_empty_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  should.equal(ring.diff(r, r), [])
}

pub fn diff_after_add_node_shows_transfers_test() {
  let r1 = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r1, "n4")
  let transfers = ring.diff(r1, r2)
  // Modulo reassignment moves many partitions, not just to n4
  should.be_true(list.length(transfers) > 0)
  // All transfers have different from/to
  list.each(transfers, fn(t) { should.not_equal(t.from_node, t.to_node) })
}

pub fn diff_after_remove_shows_transfers_test() {
  let r1 = make_ring(256, 3, ["n1", "n2", "n3", "n4"])
  let r2 = ring.remove_node(r1, "n4")
  let transfers = ring.diff(r1, r2)
  // Modulo reassignment moves many partitions
  should.be_true(list.length(transfers) > 0)
  // All transfers have different from/to
  list.each(transfers, fn(t) { should.not_equal(t.from_node, t.to_node) })
}

pub fn diff_transfers_have_different_from_to_test() {
  let r1 = make_ring(256, 3, ["n1", "n2", "n3"])
  let r2 = ring.add_node(r1, "n4")
  let transfers = ring.diff(r1, r2)
  list.each(transfers, fn(t) { should.not_equal(t.from_node, t.to_node) })
}

// --- Serialization ---

pub fn round_trip_serialization_test() {
  let r = make_ring(256, 3, ["n1", "n2", "n3"])
  let binary = ring.to_binary(r)
  let assert Ok(r2) = ring.from_binary(binary)
  should.equal(r2.size, r.size)
  should.equal(r2.n_val, r.n_val)
  should.equal(r2.nodes, r.nodes)
  should.equal(r2.version, r.version)
}
