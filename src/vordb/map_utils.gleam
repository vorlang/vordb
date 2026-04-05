/// Map utility functions.
import gleam/dict
import gleam/list

/// Select specific keys from a dict.
pub fn select_keys(
  source: dict.Dict(String, a),
  keys: List(String),
) -> dict.Dict(String, a) {
  keys
  |> list.filter_map(fn(key) {
    case dict.get(source, key) {
      Ok(val) -> Ok(#(key, val))
      Error(_) -> Error(Nil)
    }
  })
  |> dict.from_list()
}
