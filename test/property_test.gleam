import gleeunit/should

/// Run LWW property tests (commutativity, associativity, idempotency, monotonic, key preservation).
@external(erlang, "vordb_property_tests", "run_lww_properties")
fn run_lww_properties() -> #(Int, Int)

/// Run OR-Set property tests.
@external(erlang, "vordb_property_tests", "run_or_set_properties")
fn run_or_set_properties() -> #(Int, Int)

/// Run Counter property tests.
@external(erlang, "vordb_property_tests", "run_counter_properties")
fn run_counter_properties() -> #(Int, Int)

pub fn lww_crdt_properties_test() {
  let #(passed, failed) = run_lww_properties()
  should.equal(failed, 0)
  should.be_true(passed >= 5)
}

pub fn or_set_crdt_properties_test() {
  let #(passed, failed) = run_or_set_properties()
  should.equal(failed, 0)
  should.be_true(passed >= 4)
}

pub fn counter_crdt_properties_test() {
  let #(passed, failed) = run_counter_properties()
  should.equal(failed, 0)
  should.be_true(passed >= 5)
}
