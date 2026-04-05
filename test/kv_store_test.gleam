import gleeunit/should

/// Run all KvStore agent tests (implemented in Erlang for FFI compatibility).
@external(erlang, "vordb_integration_tests", "run_kv_store_tests")
fn run_kv_store_tests() -> #(Int, Int)

pub fn kv_store_agent_tests_test() {
  let #(passed, failed) = run_kv_store_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 9)
}
