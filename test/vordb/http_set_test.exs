defmodule VorDB.HTTP.SetTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias VorDB.HTTP.Router
  alias VorDB.TestHelpers

  @opts Router.init([])

  setup do
    {_storage_pid, dir} = TestHelpers.start_storage()
    TestHelpers.start_vnode_stack(:test_node)

    on_exit(fn ->
      TestHelpers.stop_vnode_stack()
      TestHelpers.cleanup_dir(dir)
    end)

    :ok
  end

  test "POST /set/:key/add returns 200" do
    conn =
      conn(:post, "/set/myset/add", Jason.encode!(%{element: "alice"}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["ok"] == true
  end

  test "GET /set/:key returns members" do
    conn(:post, "/set/myset/add", Jason.encode!(%{element: "alice"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn(:post, "/set/myset/add", Jason.encode!(%{element: "bob"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn = conn(:get, "/set/myset") |> Router.call(@opts)

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["key"] == "myset"
    assert Enum.sort(body["members"]) == ["alice", "bob"]
  end

  test "GET /set/:key on missing key returns 404" do
    conn = conn(:get, "/set/nonexistent") |> Router.call(@opts)
    assert conn.status == 404
  end

  test "POST /set/:key/remove returns 200" do
    conn(:post, "/set/myset/add", Jason.encode!(%{element: "alice"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn =
      conn(:post, "/set/myset/remove", Jason.encode!(%{element: "alice"}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert conn.status == 200
  end

  test "remove then GET excludes element" do
    conn(:post, "/set/myset/add", Jason.encode!(%{element: "alice"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn(:post, "/set/myset/add", Jason.encode!(%{element: "bob"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn(:post, "/set/myset/remove", Jason.encode!(%{element: "alice"}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn = conn(:get, "/set/myset") |> Router.call(@opts)
    body = Jason.decode!(conn.resp_body)
    assert body["members"] == ["bob"]
  end
end
