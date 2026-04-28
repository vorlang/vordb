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

@external(erlang, "vordb_coordinator", "bucket_write")
fn coord_bucket_write(bucket: String, op: Dynamic, params: Dynamic) -> Result(Dynamic, Dynamic)

@external(erlang, "vordb_coordinator", "bucket_read")
fn coord_bucket_read(bucket: String, params: Dynamic) -> Result(Dynamic, Dynamic)

@external(erlang, "vordb_http_ffi", "is_not_found")
fn is_not_found(agent_resp: Dynamic) -> Bool

@external(erlang, "vordb_bucket_registry", "create_bucket")
fn bucket_create(config: Dynamic) -> Dynamic

@external(erlang, "vordb_bucket_registry", "get_bucket")
fn bucket_get(name: String) -> Dynamic

@external(erlang, "vordb_bucket_registry", "list_buckets")
fn bucket_list() -> Dynamic

@external(erlang, "vordb_bucket_registry", "delete_bucket")
fn bucket_delete(name: String) -> Dynamic

@external(erlang, "vordb_http_ffi", "format_bucket_response")
fn format_bucket_response(result: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_bucket_list_response")
fn format_bucket_list_response(buckets: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "format_bucket_error")
fn format_bucket_error(err: Dynamic) -> String

@external(erlang, "vordb_http_ffi", "is_bucket_error")
fn is_bucket_error(result: Dynamic) -> Bool

@external(erlang, "vordb_http_ffi", "make_atom")
fn make_atom(s: String) -> Dynamic

/// Build the HTTP handler for mist.
pub fn handler(
  req: Request(Connection),
  num_vnodes: Int,
) -> Response(mist.ResponseData) {
  let path_segments = request.path_segments(req)
  let method = req.method

  case method, path_segments {
    // Bucket management
    Post, ["buckets"] -> handle_bucket_create(req)
    Get, ["buckets"] -> handle_bucket_list()
    Get, ["buckets", name] -> handle_bucket_get(name)
    Delete, ["buckets", name] -> handle_bucket_delete(name)

    // Unified bucket data operations
    Put, ["bucket", bucket, key] -> handle_bucket_put(req, bucket, key)
    Get, ["bucket", bucket, key] -> handle_bucket_read(bucket, key)
    Delete, ["bucket", bucket, key] -> handle_bucket_del(bucket, key)
    Post, ["bucket", bucket, key, "add"] -> handle_bucket_set_add(req, bucket, key)
    Post, ["bucket", bucket, key, "remove"] -> handle_bucket_set_remove(req, bucket, key)
    Post, ["bucket", bucket, key, "increment"] -> handle_bucket_counter_inc(req, bucket, key)
    Post, ["bucket", bucket, key, "decrement"] -> handle_bucket_counter_dec(req, bucket, key)

    // Legacy type-specific endpoints (route to default buckets)
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

// ===== Bucket management handlers =====

fn handle_bucket_create(req: Request(Connection)) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "name") {
        Ok(name) -> {
          let type_str = case json_get_string(body, "type") { Ok(t) -> t Error(_) -> "lww" }
          let ttl = json_get_int(body, "ttl_seconds", 0)
          let n = json_get_int(body, "replication_n", 0)
          // Check for consistency preset or explicit W/R
          let #(w, r) = case json_get_string(body, "consistency") {
            Ok(preset) -> resolve_preset(preset)
            Error(_) -> #(
              json_get_int(body, "write_quorum", 0),
              json_get_int(body, "read_quorum", 0),
            )
          }
          let config = make_bucket_config_full(name, type_str, ttl, n, w, r)
          let result = bucket_create(config)
          case is_bucket_error(result) {
            True -> json_response(409, format_bucket_error(result))
            False -> json_response(201, "{\"ok\":true,\"bucket\":\"" <> name <> "\"}")
          }
        }
        Error(_) -> json_response(400, "{\"error\":\"missing name\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

@external(erlang, "vordb_http_ffi", "make_bucket_config")
fn make_bucket_config_full(name: String, type_str: String, ttl: Int, n: Int, w: Int, r: Int) -> Dynamic

@external(erlang, "vordb_http_ffi", "resolve_consistency_preset")
fn resolve_preset(preset: String) -> #(Int, Int)

fn handle_bucket_list() -> Response(mist.ResponseData) {
  let buckets = bucket_list()
  json_response(200, format_bucket_list_response(buckets))
}

fn handle_bucket_get(name: String) -> Response(mist.ResponseData) {
  let result = bucket_get(name)
  case is_bucket_error(result) {
    True -> json_response(404, "{\"error\":\"bucket_not_found\",\"bucket\":\"" <> name <> "\"}")
    False -> json_response(200, format_bucket_response(result))
  }
}

fn handle_bucket_delete(name: String) -> Response(mist.ResponseData) {
  let result = bucket_delete(name)
  case is_bucket_error(result) {
    True -> json_response(404, "{\"error\":\"bucket_not_found\",\"bucket\":\"" <> name <> "\"}")
    False -> json_response(200, "{\"ok\":true,\"deleted\":\"" <> name <> "\"}")
  }
}

// ===== Unified bucket data handlers =====

fn handle_bucket_put(req: Request(Connection), bucket: String, key: String) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "value") {
        Ok(value) -> {
          let params = make_bucket_params_put(key, value)
          case coord_bucket_write(bucket, make_atom("put"), params) {
            Ok(resp) -> json_response(200, format_kv_put_response(key, resp))
            Error(err) -> bucket_write_error(err)
          }
        }
        Error(_) -> json_response(400, "{\"error\":\"missing value\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_bucket_read(bucket: String, key: String) -> Response(mist.ResponseData) {
  let params = make_bucket_params_key(key)
  case coord_bucket_read(bucket, params) {
    Ok(resp) ->
      case is_not_found(resp) {
        True -> json_response(404, "{\"error\":\"not_found\",\"key\":\"" <> key <> "\"}")
        False -> json_response(200, format_bucket_read_response(key, resp))
      }
    Error(err) -> bucket_read_error(err)
  }
}

fn handle_bucket_del(bucket: String, key: String) -> Response(mist.ResponseData) {
  let params = make_bucket_params_key(key)
  case coord_bucket_write(bucket, make_atom("delete"), params) {
    Ok(resp) -> json_response(200, format_kv_delete_response(key, resp))
    Error(err) -> bucket_write_error(err)
  }
}

fn handle_bucket_set_add(req: Request(Connection), bucket: String, key: String) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "element") {
        Ok(element) -> {
          let params = make_bucket_params_element(key, element)
          case coord_bucket_write(bucket, make_atom("set_add"), params) {
            Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
            Error(err) -> bucket_write_error(err)
          }
        }
        Error(_) -> json_response(400, "{\"error\":\"missing element\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_bucket_set_remove(req: Request(Connection), bucket: String, key: String) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      case json_get_string(body, "element") {
        Ok(element) -> {
          let params = make_bucket_params_element(key, element)
          case coord_bucket_write(bucket, make_atom("set_remove"), params) {
            Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
            Error(err) -> bucket_write_error(err)
          }
        }
        Error(_) -> json_response(400, "{\"error\":\"missing element\"}")
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_bucket_counter_inc(req: Request(Connection), bucket: String, key: String) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      let amount = json_get_int(body, "amount", 1)
      let params = make_bucket_params_amount(key, amount)
      case coord_bucket_write(bucket, make_atom("counter_increment"), params) {
        Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
        Error(err) -> bucket_write_error(err)
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn handle_bucket_counter_dec(req: Request(Connection), bucket: String, key: String) -> Response(mist.ResponseData) {
  case mist.read_body(req, 1_000_000) {
    Ok(r) -> {
      let body = parse_json_body(r.body)
      let amount = json_get_int(body, "amount", 1)
      let params = make_bucket_params_amount(key, amount)
      case coord_bucket_write(bucket, make_atom("counter_decrement"), params) {
        Ok(_) -> json_response(200, "{\"ok\":true,\"key\":\"" <> key <> "\"}")
        Error(err) -> bucket_write_error(err)
      }
    }
    Error(_) -> json_response(400, "{\"error\":\"bad request\"}")
  }
}

fn bucket_write_error(err: Dynamic) -> Response(mist.ResponseData) {
  let msg = format_bucket_error(err)
  json_response(400, msg)
}

fn bucket_read_error(err: Dynamic) -> Response(mist.ResponseData) {
  let msg = format_bucket_error(err)
  json_response(404, msg)
}

@external(erlang, "vordb_http_ffi", "make_bucket_config")
fn make_bucket_config(name: String, type_str: String, ttl: Int, n: Int) -> Dynamic

@external(erlang, "vordb_http_ffi", "make_bucket_params_put")
fn make_bucket_params_put(key: String, value: String) -> Dynamic

@external(erlang, "vordb_http_ffi", "make_bucket_params_key")
fn make_bucket_params_key(key: String) -> Dynamic

@external(erlang, "vordb_http_ffi", "make_bucket_params_element")
fn make_bucket_params_element(key: String, element: String) -> Dynamic

@external(erlang, "vordb_http_ffi", "make_bucket_params_amount")
fn make_bucket_params_amount(key: String, amount: Int) -> Dynamic

@external(erlang, "vordb_http_ffi", "format_bucket_read_response")
fn format_bucket_read_response(key: String, resp: Dynamic) -> String

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
