import gleeunit/should

/// Run all Storage tests (implemented in Erlang for RocksDB FFI compatibility).
@external(erlang, "vordb_integration_tests", "run_storage_tests")
fn run_storage_tests() -> #(Int, Int)

pub fn storage_tests_test() {
  let #(passed, failed) = run_storage_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 6)
}
