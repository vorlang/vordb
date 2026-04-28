import gleeunit/should

/// Run all bucket tests (creates buckets, writes through them, verifies isolation).
@external(erlang, "vordb_bucket_tests", "run_all")
fn run_bucket_tests() -> #(Int, Int)

pub fn bucket_tests_test() {
  let #(passed, failed) = run_bucket_tests()
  should.equal(failed, 0)
  should.be_true(passed >= 12)
}
