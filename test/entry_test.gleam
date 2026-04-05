import gleeunit/should
import vordb/entry
import vordb/types
import gleam/dict

pub fn new_builds_correct_structure_test() {
  let e = entry.new("hello", 1000, "node1")
  should.equal(e, types.LwwEntry(value: "hello", timestamp: 1000, node_id: "node1"))
}

pub fn tombstone_builds_tombstone_entry_test() {
  let e = entry.tombstone(1000, "node1")
  should.equal(e, types.Tombstone(timestamp: 1000, node_id: "node1"))
}

pub fn lookup_returns_found_for_existing_key_test() {
  let store = dict.from_list([#("key1", types.LwwEntry("hello", 1000, "node1"))])
  entry.lookup(store, "key1")
  |> should.equal(types.Found(value: "hello"))
}

pub fn lookup_returns_not_found_for_missing_key_test() {
  let store = dict.new()
  entry.lookup(store, "missing")
  |> should.equal(types.NotFound)
}

pub fn lookup_returns_not_found_for_tombstone_test() {
  let store = dict.from_list([#("key1", types.Tombstone(1000, "node1"))])
  entry.lookup(store, "key1")
  |> should.equal(types.NotFound)
}
