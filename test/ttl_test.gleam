import gleeunit/should

/// Run TTL tests (key expiration, TTL reset, sweep).
@external(erlang, "vordb_ttl_tests", "run_all")
fn run_ttl_tests() -> #(Int, Int)

pub fn ttl_tests_test() {
  let #(passed, failed) = run_ttl_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 8)
}
