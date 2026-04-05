/// Term serialization for RocksDB values.
/// Wraps Erlang's term_to_binary/binary_to_term via FFI.
import gleam/dynamic.{type Dynamic}

@external(erlang, "vordb_ffi", "term_to_binary")
pub fn encode(term: Dynamic) -> BitArray

@external(erlang, "vordb_ffi", "binary_to_term")
pub fn decode(binary: BitArray) -> Dynamic
