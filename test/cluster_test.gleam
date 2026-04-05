import gleeunit/should

/// Run integration cluster tests (multi-instance gossip, convergence, recovery).
@external(erlang, "vordb_cluster_tests", "run_all")
fn run_cluster_tests() -> #(Int, Int)

pub fn cluster_integration_tests_test() {
  let #(passed, failed) = run_cluster_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 11)
}
