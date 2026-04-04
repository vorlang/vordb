defmodule VorDB.HTTP.RouterTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias VorDB.HTTP.Router
  alias VorDB.TestHelpers

  @opts Router.init([])

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    _pid = TestHelpers.start_kv_store_registered(:test_node)

    on_exit(fn ->
      try do
        GenServer.stop(Vor.Agent.KvStore, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end

      TestHelpers.cleanup_dir(dir)
    end)

    :ok
  end

  test "PUT /kv/:key returns 200 with timestamp" do
    conn =
      conn(:put, "/kv/mykey", Jason.encode!(%{value: "hello"}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["ok"] == true
    assert body["key"] == "mykey"
    assert is_integer(body["timestamp"])
  end

  test "GET /kv/:key returns 200 with value" do
    # PUT first
    conn(:put, "/kv/mykey", Jason.encode!(%{value: "hello"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn = conn(:get, "/kv/mykey") |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["key"] == "mykey"
    assert body["value"] == "hello"
  end

  test "GET missing key returns 404" do
    conn = conn(:get, "/kv/nonexistent") |> Router.call(@opts)

    assert conn.status == 404
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "not_found"
  end

  test "DELETE /kv/:key returns 200" do
    conn(:put, "/kv/mykey", Jason.encode!(%{value: "hello"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn = conn(:delete, "/kv/mykey") |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["deleted"] == true
  end

  test "DELETE then GET returns 404" do
    conn(:put, "/kv/mykey", Jason.encode!(%{value: "hello"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn(:delete, "/kv/mykey") |> Router.call(@opts)

    conn = conn(:get, "/kv/mykey") |> Router.call(@opts)
    assert conn.status == 404
  end

  test "GET /status returns node info" do
    conn = conn(:get, "/status") |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert Map.has_key?(body, "node")
    assert Map.has_key?(body, "node_id")
  end

  test "unknown route returns 404" do
    conn = conn(:get, "/unknown") |> Router.call(@opts)
    assert conn.status == 404
  end
end
