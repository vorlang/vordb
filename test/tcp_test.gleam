import gleeunit/should

@external(erlang, "vordb_tcp_tests", "run_all")
fn run_tcp_tests() -> #(Int, Int)

pub fn tcp_protocol_tests_test() {
  let #(passed, failed) = run_tcp_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 8)
}
