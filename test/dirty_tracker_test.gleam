import gleam/dynamic.{type Dynamic}
import gleeunit/should

@external(erlang, "vordb_dirty_tracker", "start_link")
fn start_link(opts: Dynamic) -> Result(Dynamic, Dynamic)

@external(erlang, "vordb_dirty_tracker", "stop")
fn stop() -> Dynamic

@external(erlang, "vordb_dirty_tracker", "mark_dirty")
fn mark_dirty(vnode: Int, type_name: Dynamic, key: String) -> Dynamic

@external(erlang, "vordb_dirty_tracker", "take_deltas")
fn take_deltas(vnode: Int, peer: Dynamic) -> #(Int, Dynamic)

@external(erlang, "vordb_dirty_tracker", "confirm_ack")
fn confirm_ack(vnode: Int, peer: Dynamic, seq: Int) -> Dynamic

@external(erlang, "vordb_dirty_tracker", "add_peer")
fn add_peer(peer: Dynamic) -> Dynamic

@external(erlang, "vordb_dirty_tracker", "remove_peer")
fn remove_peer(peer: Dynamic) -> Dynamic

@external(erlang, "erlang", "binary_to_atom")
fn atom(s: String) -> Dynamic

@external(erlang, "timer", "sleep")
fn sleep(ms: Int) -> Dynamic

@external(erlang, "maps", "get")
fn maps_get(key: Dynamic, map: Dynamic) -> Dynamic

@external(erlang, "erlang", "length")
fn list_length(l: Dynamic) -> Int

@external(erlang, "vordb_dirty_tracker_test_helper", "whereis_dt")
fn whereis_dt() -> Bool

@external(erlang, "vordb_gleam_helpers", "dt_start_opts")
fn dt_start_opts(peers: List(Dynamic), num_vnodes: Int) -> Dynamic

fn setup() {
  case whereis_dt() {
    True -> { let _ = stop() Nil }
    False -> Nil
  }
  let opts = dt_start_opts([atom("node2"), atom("node3")], 4)
  let assert Ok(_) = start_link(opts)
}

fn teardown() {
  case whereis_dt() {
    True -> { let _ = stop() Nil }
    False -> Nil
  }
}

fn get_lww(deltas: Dynamic) -> Dynamic {
  maps_get(atom("lww"), deltas)
}

pub fn mark_dirty_adds_to_peers_test() {
  setup()
  mark_dirty(0, atom("lww"), "key1")
  let _ = sleep(5)
  let #(seq, deltas) = take_deltas(0, atom("node2"))
  should.be_true(seq > 0)
  list_length(get_lww(deltas)) |> should.equal(1)
  teardown()
}

pub fn take_deltas_clears_for_that_peer_test() {
  setup()
  mark_dirty(0, atom("lww"), "key1")
  let _ = sleep(5)
  let #(_, _) = take_deltas(0, atom("node2"))
  let #(seq2, deltas2) = take_deltas(0, atom("node2"))
  should.equal(seq2, 0)
  list_length(get_lww(deltas2)) |> should.equal(0)
  teardown()
}

pub fn different_vnodes_independent_test() {
  setup()
  mark_dirty(0, atom("lww"), "key_a")
  mark_dirty(1, atom("lww"), "key_b")
  let _ = sleep(5)
  let #(_, d0) = take_deltas(0, atom("node2"))
  let #(_, d1) = take_deltas(1, atom("node2"))
  list_length(get_lww(d0)) |> should.equal(1)
  list_length(get_lww(d1)) |> should.equal(1)
  teardown()
}

pub fn confirm_ack_clears_pending_test() {
  setup()
  mark_dirty(0, atom("lww"), "key1")
  let _ = sleep(5)
  let #(seq, _) = take_deltas(0, atom("node2"))
  confirm_ack(0, atom("node2"), seq)
  let _ = sleep(5)
  let #(seq2, d2) = take_deltas(0, atom("node2"))
  should.equal(seq2, 0)
  list_length(get_lww(d2)) |> should.equal(0)
  teardown()
}

pub fn add_peer_works_test() {
  setup()
  add_peer(atom("node4"))
  mark_dirty(2, atom("lww"), "key1")
  let _ = sleep(5)
  let #(_, d) = take_deltas(2, atom("node4"))
  list_length(get_lww(d)) |> should.equal(1)
  teardown()
}

pub fn remove_peer_works_test() {
  setup()
  mark_dirty(0, atom("lww"), "key1")
  remove_peer(atom("node3"))
  let _ = sleep(5)
  let #(seq, d) = take_deltas(0, atom("node3"))
  should.equal(seq, 0)
  list_length(get_lww(d)) |> should.equal(0)
  teardown()
}

pub fn sequence_numbers_increment_test() {
  setup()
  mark_dirty(0, atom("lww"), "a")
  let _ = sleep(5)
  let #(seq1, _) = take_deltas(0, atom("node2"))
  mark_dirty(0, atom("lww"), "b")
  let _ = sleep(5)
  let #(seq2, _) = take_deltas(0, atom("node2"))
  should.be_true(seq2 > seq1)
  teardown()
}
