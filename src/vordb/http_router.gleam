/// HTTP REST API for VorDB using mist.
import gleam/bytes_tree
import gleam/dynamic.{type Dynamic}
import gleam/http.{Delete, Get, Post, Put}
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/json
import mist.{type Connection}

// FFI for message construction
@external(erlang, "vordb_ffi", "make_put_msg")
fn make_put_msg(key: String, value: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_get_msg")
fn make_get_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_delete_msg")
fn make_delete_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_add_msg")
fn make_set_add_msg(key: String, element: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_remove_msg")
fn make_set_remove_msg(key: String, element: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_set_members_msg")
fn make_set_members_msg(key: String) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_increment_msg")
fn make_counter_increment_msg(key: String, amount: Int) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_decrement_msg")
fn make_counter_decrement_msg(key: String, amount: Int) -> Dynamic

@external(erlang, "vordb_ffi", "make_counter_value_msg")
fn make_counter_value_msg(key: String) -> Dynamic

// FFI for JSON parsing and response decoding
@external(erlang, "vordb_http_ffi", "parse_json_body")
fn parse_json_body(body: BitArray) -> Dynamic

@external(erlang, "vordb_http_ffi", "json_get_string")
fn json_get_string(parsed: Dynamic, key: String) -> Result(String, Nil)

@external(erlang, "vordb_http_ffi", "json_get_int")
fn json_get_int(parsed: Dynamic, key: String, default: Int) -> Int

@external(erlang, "vordb_http_ffi", "format_kv_get_response")
fn format_kv_get_response(key: String, agent_resp: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_kv_put_response")
fn format_kv_put_response(key: String, agent_resp: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_kv_delete_response")
fn format_kv_delete_response(key: String, agent_resp: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_set_members_response")
fn format_set_members_response(key: String, agent_resp: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_counter_value_response")
fn format_counter_value_response(key: String, agent_resp: Dynamic) -> String

@external(erlang, "vordb_coordinator", "write")
fn coordinator_write(key: String, message: Dynamic) -> Result(Dynamic, Dynamic)

@external(erlang, "vordb_coordinator", "read")
fn coordinator_read(key: String, message: Dynamic) -> Result(Dynamic, Dynamic)

@external(erlang, "vordb_http_ffi", "is_not_found")
fn is_not_found(agent_resp: Dynamic) -> Bool

/// Build the HTTP handler for mist.
pub fn handler(
  req: Request(Connection),
  num_vnodes: Int,
) -> Response(mist.ResponseData) {
  let path_segments = request.path_segments(req)
  let method = req.method

  case method, path_segments {
    Put, ["kv", key] -> handle_kv_put(req, key, num_vnodes)
    Get, ["kv", key] -> handle_kv_get(key, num_vnodes)
    Delete, ["kv", key] -> handle_kv_delete(key, num_vnodes)
    Post, ["set", key, "add"] -> handle_set_add(req, key, num_vnodes)
    Post, ["set", key, "remove"] -> handle_set_remove(req, key, num_vnodes)
    Get, ["set", key] -> handle_set_members(key, num_vnodes)
    Post, ["counter", key, "increment"] -> handle_counter_inc(req, key, num_vnodes)
    Post, ["counter", key, "decrement"] -> handle_counter_dec(req, key, num_vnodes)
    Get, ["counter", key] -> handle_counter_value(key, num_vnodes)
    Get, ["status"] -> handle_status(num_vnodes)
    Post, ["admin", "full-sync"] -> handle_full_sync()
    Get, ["metrics"] -> handle_metrics()
    _, _ -> json_response(404, "{\"error\":\"not_found\"}")
  }
}

@external(erlang, "vordb_metrics", "format_prometheus")
fn metrics_text() -> String

fn handle_metrics() -> Response(mist.ResponseData) {
  let body = metrics_text()
  response.new(200)
  |> response.set_header("content-type", "text/plain; version=0.0.4; charset=utf-8")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}

fn handle_kv_put(req: Request(Connection), key: String, nv: Int) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "value") {
        Ok(value) ->
          case coordinator_write(key, make_put_msg(key, value)) {
            Ok(resp) -> json_response(200, format_kv_put_response(key, resp))
            Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
          }
        Error(_) -> json_response(400, "{\"error\":\"missing value\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_kv_get(key: String, nv: Int) -> Response(mist.ResponseData) {
  case coordinator_read(key, make_get_msg(key)) {
    Ok(resp) ->
      case is_not_found(resp) {
        True -> json_response(404, "{\"error\":\"not_found\",\"key\":\"" <> key <> "\"}")
        False -> json_response(200, format_kv_get_response(key, resp))
      }
    Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
  }
}

fn handle_kv_delete(key: String, nv: Int) -> Response(mist.ResponseData) {
  case coordinator_write(key, make_delete_msg(key)) {
    Ok(resp) -> json_response(200, format_kv_delete_response(key, resp))
    Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
  }
}

fn handle_set_add(req: Request(Connection), key: String, nv: Int) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "element") {
        Ok(element) ->
          case coordinator_write(key, make_set_add_msg(key, element)) {
            Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
            Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
          }
        Error(_) -> json_response(400, "{\"error\":\"missing element\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_set_remove(req: Request(Connection), key: String, nv: Int) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "element") {
        Ok(element) ->
          case coordinator_write(key, make_set_remove_msg(key, element)) {
            Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
            Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
          }
        Error(_) -> json_response(400, "{\"error\":\"missing element\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_set_members(key: String, nv: Int) -> Response(mist.ResponseData) {
  case coordinator_read(key, make_set_members_msg(key)) {
    Ok(resp) ->
      case is_not_found(resp) {
        True -> json_response(404, "{\"error\":\"not_found\",\"key\":\"" <> key <> "\"}")
        False -> json_response(200, format_set_members_response(key, resp))
      }
    Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
  }
}

fn handle_counter_inc(req: Request(Connection), key: String, nv: Int) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      let amount = json_get_int(body, "amount", 1)
      case coordinator_write(key, make_counter_increment_msg(key, amount)) {
        Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
        Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_counter_dec(req: Request(Connection), key: String, nv: Int) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      let amount = json_get_int(body, "amount", 1)
      case coordinator_write(key, make_counter_decrement_msg(key, amount)) {
        Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
        Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_counter_value(key: String, nv: Int) -> Response(mist.ResponseData) {
  case coordinator_read(key, make_counter_value_msg(key)) {
    Ok(resp) ->
      case is_not_found(resp) {
        True -> json_response(404, "{\"error\":\"not_found\",\"key\":\"" <> key <> "\"}")
        False -> json_response(200, format_counter_value_response(key, resp))
      }
    Error(_) -> json_response(500, "{\"error\":\"internal_error\"}")
  }
}

fn handle_status(num_vnodes: Int) -> Response(mist.ResponseData) {
  let body = json.to_string(json.object([
    #("status", json.string("running")),
    #("num_vnodes", json.int(num_vnodes)),
  ]))
  json_response(200, body)
}

@external(erlang, "vordb_ffi", "gossip_send_full_sync")
fn do_full_sync() -> Dynamic

fn handle_full_sync() -> Response(mist.ResponseData) {
  let _ = do_full_sync()
  json_response(200, "{\"ok\":true,\"action\":\"full_sync_triggered\"}")
}

fn json_response(status: Int, body: String) -> Response(mist.ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "application/json")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}

/// Start mist HTTP server on given port. Called from Erlang tests.
pub fn start_mist(
  port: Int,
  num_vnodes: Int,
) -> Result(Dynamic, Dynamic) {
  let handler_fn = fn(req) { safe_handler(req, num_vnodes) }
  mist.new(handler_fn)
  |> mist.port(port)
  |> mist.start()
  |> result_to_dynamic()
}

fn safe_handler(
  req: Request(Connection),
  num_vnodes: Int,
) -> Response(mist.ResponseData) {
  do_safe_handler(req, num_vnodes)
}

@external(erlang, "vordb_gleam_helpers", "safe_http_handler")
fn do_safe_handler(
  req: Request(Connection),
  num_vnodes: Int,
) -> Response(mist.ResponseData)

@external(erlang, "vordb_gleam_helpers", "to_dynamic")
fn to_dynamic(a: a) -> Dynamic

fn result_to_dynamic(r: Result(a, b)) -> Result(Dynamic, Dynamic) {
  case r {
    Ok(v) -> Ok(to_dynamic(v))
    Error(e) -> Error(to_dynamic(e))
  }
}
