import gleeunit/should
import vordb/map_utils
import gleam/dict

pub fn select_keys_returns_subset_test() {
  let source = dict.from_list([#("a", 1), #("b", 2), #("c", 3)])
  map_utils.select_keys(source, ["a", "c"])
  |> should.equal(dict.from_list([#("a", 1), #("c", 3)]))
}

pub fn select_keys_missing_returns_present_only_test() {
  let source = dict.from_list([#("a", 1)])
  map_utils.select_keys(source, ["a", "b"])
  |> should.equal(dict.from_list([#("a", 1)]))
}

pub fn select_keys_empty_list_test() {
  let source = dict.from_list([#("a", 1)])
  map_utils.select_keys(source, [])
  |> should.equal(dict.new())
}

pub fn select_keys_empty_source_test() {
  map_utils.select_keys(dict.new(), ["a"])
  |> should.equal(dict.new())
}
