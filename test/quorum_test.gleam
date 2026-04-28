import gleeunit/should

/// Run quorum read/write tests.
@external(erlang, "vordb_quorum_tests", "run_all")
fn run_quorum_tests() -> #(Int, Int)

pub fn quorum_tests_test() {
  let #(passed, failed) = run_quorum_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 8)
}
