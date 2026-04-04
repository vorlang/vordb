defmodule VorDB.HTTP.Router do
  @moduledoc "REST API for VorDB. Minimal Plug router."

  use Plug.Router

  plug :match
  plug Plug.Parsers, parsers: [:json], json_decoder: Jason
  plug :dispatch

  # PUT /kv/:key — write a value
  put "/kv/:key" do
    value = conn.body_params["value"]

    case value do
      nil ->
        send_json(conn, 400, %{error: "missing 'value' in request body"})

      _ ->
        # Vor emit format: {:ok, %{timestamp: ts}}
        {:ok, %{timestamp: timestamp}} = GenServer.call(Vor.Agent.KvStore, {:put, %{key: key, value: value}})
        send_json(conn, 200, %{ok: true, key: key, timestamp: timestamp})
    end
  end

  # GET /kv/:key — read a value
  get "/kv/:key" do
    # Vor emit format: {:value, %{key: k, val: v, found: atom}}
    case GenServer.call(Vor.Agent.KvStore, {:get, %{key: key}}) do
      {:value, %{key: ^key, val: value, found: :true}} ->
        send_json(conn, 200, %{key: key, value: value})

      {:value, %{key: ^key, found: :false}} ->
        send_json(conn, 404, %{error: "not_found", key: key})
    end
  end

  # DELETE /kv/:key — delete a value
  delete "/kv/:key" do
    # Vor emit format: {:deleted, %{key: k, timestamp: ts}}
    {:deleted, %{key: ^key, timestamp: timestamp}} =
      GenServer.call(Vor.Agent.KvStore, {:delete, %{key: key}})

    send_json(conn, 200, %{deleted: true, key: key, timestamp: timestamp})
  end

  # GET /status — node status
  get "/status" do
    send_json(conn, 200, %{
      node: to_string(node()),
      node_id: to_string(Application.get_env(:vordb, :node_id)),
      peers: Enum.map(VorDB.Cluster.peers(), &to_string/1),
      connected: Enum.map(Node.list(), &to_string/1)
    })
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  defp send_json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end
end
