import gleeunit/should

/// Run all HTTP endpoint tests (starts full stack with mist, makes real HTTP requests).
@external(erlang, "vordb_http_tests", "run_all")
fn run_http_tests() -> #(Int, Int)

pub fn http_endpoint_tests_test() {
  let #(passed, failed) = run_http_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 16)
}
