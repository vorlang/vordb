defmodule VorDB.HTTP.CounterTest do
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

  test "POST /counter/:key/increment returns 200" do
    conn =
      conn(:post, "/counter/hits/increment", Jason.encode!(%{}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert conn.status == 200
    assert Jason.decode!(conn.resp_body)["ok"] == true
  end

  test "POST /counter/:key/increment with amount" do
    conn(:post, "/counter/hits/increment", Jason.encode!(%{amount: 5}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn = conn(:get, "/counter/hits") |> Router.call(@opts)
    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["value"] == 5
  end

  test "POST /counter/:key/decrement returns 200" do
    conn(:post, "/counter/hits/increment", Jason.encode!(%{}))
    |> put_req_header("content-type", "application/json")
    |> Router.call(@opts)

    conn =
      conn(:post, "/counter/hits/decrement", Jason.encode!(%{}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)

    assert conn.status == 200

    conn = conn(:get, "/counter/hits") |> Router.call(@opts)
    assert Jason.decode!(conn.resp_body)["value"] == 0
  end

  test "GET /counter/:key returns value" do
    for _ <- 1..3 do
      conn(:post, "/counter/hits/increment", Jason.encode!(%{}))
      |> put_req_header("content-type", "application/json")
      |> Router.call(@opts)
    end

    conn = conn(:get, "/counter/hits") |> Router.call(@opts)
    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["key"] == "hits"
    assert body["value"] == 3
  end

  test "GET /counter/:key on missing returns 404" do
    conn = conn(:get, "/counter/nonexistent") |> Router.call(@opts)
    assert conn.status == 404
  end
end
