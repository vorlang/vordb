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
        {:ok, %{timestamp: timestamp}} = VorDB.VnodeRouter.call(key,{:put, %{key: key, value: value}})
        send_json(conn, 200, %{ok: true, key: key, timestamp: timestamp})
    end
  end

  # GET /kv/:key — read a value
  get "/kv/:key" do
    # Vor emit format: {:value, %{key: k, val: v, found: atom}}
    case VorDB.VnodeRouter.call(key,{:get, %{key: key}}) do
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
      VorDB.VnodeRouter.call(key,{:delete, %{key: key}})

    send_json(conn, 200, %{deleted: true, key: key, timestamp: timestamp})
  end

  # POST /set/:key/add — add element to set
  post "/set/:key/add" do
    element = conn.body_params["element"]

    case element do
      nil ->
        send_json(conn, 400, %{error: "missing 'element' in request body"})

      _ ->
        {:set_ok, %{}} = VorDB.VnodeRouter.call(key,{:set_add, %{key: key, element: element}})
        send_json(conn, 200, %{ok: true, key: key})
    end
  end

  # POST /set/:key/remove — remove element from set
  post "/set/:key/remove" do
    element = conn.body_params["element"]

    case element do
      nil ->
        send_json(conn, 400, %{error: "missing 'element' in request body"})

      _ ->
        {:set_ok, %{}} = VorDB.VnodeRouter.call(key,{:set_remove, %{key: key, element: element}})
        send_json(conn, 200, %{ok: true, key: key})
    end
  end

  # GET /set/:key — get set members
  get "/set/:key" do
    case VorDB.VnodeRouter.call(key,{:set_members, %{key: key}}) do
      {:set_members, %{key: ^key, members: members}} ->
        send_json(conn, 200, %{key: key, members: members})

      {:set_not_found, %{key: ^key}} ->
        send_json(conn, 404, %{error: "not_found", key: key})
    end
  end

  # POST /counter/:key/increment
  post "/counter/:key/increment" do
    amount = Map.get(conn.body_params, "amount", 1)

    {:counter_ok, %{}} =
      VorDB.VnodeRouter.call(key,{:counter_increment, %{key: key, amount: amount}})

    send_json(conn, 200, %{ok: true, key: key})
  end

  # POST /counter/:key/decrement
  post "/counter/:key/decrement" do
    amount = Map.get(conn.body_params, "amount", 1)

    {:counter_ok, %{}} =
      VorDB.VnodeRouter.call(key,{:counter_decrement, %{key: key, amount: amount}})

    send_json(conn, 200, %{ok: true, key: key})
  end

  # GET /counter/:key
  get "/counter/:key" do
    case VorDB.VnodeRouter.call(key,{:counter_value, %{key: key}}) do
      {:counter_value, %{key: ^key, val: value}} ->
        send_json(conn, 200, %{key: key, value: value})

      {:counter_not_found, %{key: ^key}} ->
        send_json(conn, 404, %{error: "not_found", key: key})
    end
  end

  # GET /status — node status
  get "/status" do
    num_vnodes = Application.get_env(:vordb, :num_vnodes, 16)

    send_json(conn, 200, %{
      node: to_string(node()),
      node_id: to_string(Application.get_env(:vordb, :node_id)),
      peers: Enum.map(VorDB.Cluster.peers(), &to_string/1),
      connected: Enum.map(Node.list(), &to_string/1),
      num_vnodes: num_vnodes
    })
  end

  # POST /admin/join — join cluster via seed node
  post "/admin/join" do
    seed = conn.body_params["seed_node"]

    case seed do
      nil ->
        send_json(conn, 400, %{error: "missing 'seed_node' in request body"})

      _ ->
        case VorDB.Membership.join(String.to_atom(seed)) do
          {:ok, members} ->
            send_json(conn, 200, %{joined: true, members: Enum.map(members, &to_string/1)})

          {:error, reason} ->
            send_json(conn, 500, %{error: to_string(reason)})
        end
    end
  end

  # POST /admin/leave — leave cluster
  post "/admin/leave" do
    VorDB.Membership.leave()
    send_json(conn, 200, %{leaving: true})
  end

  # GET /admin/members — view membership
  get "/admin/members" do
    members = VorDB.Membership.members()
    send_json(conn, 200, %{members: Enum.map(members, &to_string/1)})
  end

  # POST /admin/full-sync — trigger full-state sync to all peers
  post "/admin/full-sync" do
    VorDB.Gossip.send_full_sync()
    send_json(conn, 200, %{ok: true, action: "full_sync_triggered"})
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
