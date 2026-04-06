-module(vordb_http_tests).
-export([run_all/0]).

%% HTTP endpoint tests. Starts full stack (storage, registry, vnodes, mist)
%% and makes real HTTP requests via httpc.

-define(PORT, 14099).
-define(BASE, "http://127.0.0.1:" ++ integer_to_list(?PORT)).
-define(RING_SIZE, 8).
-define(NVAL, 3).

run_all() ->
    setup(),
    Results = [
        test("PUT /kv/:key returns 200", fun test_put_kv/0),
        test("GET /kv/:key returns value", fun test_get_kv/0),
        test("GET /kv/:key missing returns 404", fun test_get_kv_missing/0),
        test("DELETE /kv/:key returns 200", fun test_delete_kv/0),
        test("DELETE then GET returns 404", fun test_delete_then_get/0),
        test("POST /set/:key/add returns 200", fun test_set_add/0),
        test("GET /set/:key returns members", fun test_set_members/0),
        test("GET /set/:key missing returns 404", fun test_set_missing/0),
        test("POST /set/:key/remove works", fun test_set_remove/0),
        test("POST /counter/:key/increment returns 200", fun test_counter_inc/0),
        test("GET /counter/:key returns value", fun test_counter_value/0),
        test("GET /counter/:key missing returns 404", fun test_counter_missing/0),
        test("POST /counter/:key/decrement works", fun test_counter_dec/0),
        test("GET /status returns 200", fun test_status/0),
        test("GET /unknown returns 404", fun test_unknown_route/0),
        test("PUT /kv missing value returns 400", fun test_put_missing_value/0)
    ],
    teardown(),
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length([fail || {_, {fail, _}} <- Results]),
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("HTTP FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

setup() ->
    application:ensure_all_started(inets),
    application:ensure_all_started(ssl),
    %% Start storage
    Dir = <<"/tmp/vordb_http_test_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    catch vordb_ffi:storage_stop(),
    {ok, _} = vordb_ffi:storage_start(Dir),
    %% Init cache table
    vordb_cache:init(),
    %% Start ring manager — single node owns all partitions
    catch gen_server:stop(vordb_ring_manager),
    {ok, _} = vordb_ring_manager:start_link(?RING_SIZE, ?NVAL, [<<"test_node">>], <<"test_node">>),
    %% Start registry + dirty tracker
    vordb_registry:start(),
    catch gen_server:stop(vordb_dirty_tracker),
    {ok, _} = vordb_dirty_tracker:start_link([{peers, []}, {num_vnodes, ?RING_SIZE}]),
    %% Start vnodes for all partitions this node owns
    MyPartitions = vordb_ring_manager:my_partitions(),
    lists:foreach(fun(V) ->
        {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
            [{node_id, test_node}, {vnode_id, V}, {sync_interval_ms, 600000}], []),
        vordb_registry:register({kv_store, V}, Pid)
    end, MyPartitions),
    %% Start mist HTTP server
    {ok, _} = 'vordb@http_router':start_mist(?PORT, ?RING_SIZE),
    timer:sleep(100),
    put(http_dir, Dir),
    put(http_partitions, MyPartitions),
    ok.

teardown() ->
    %% Stop vnodes
    MyPartitions = case get(http_partitions) of undefined -> []; P -> P end,
    lists:foreach(fun(V) ->
        case vordb_registry:lookup({kv_store, V}) of
            {ok, Pid} -> catch gen_server:stop(Pid);
            _ -> ok
        end
    end, MyPartitions),
    catch gen_server:stop(vordb_ring_manager),
    catch gen_server:stop(vordb_dirty_tracker),
    catch vordb_ffi:storage_stop(),
    Dir = get(http_dir),
    case Dir of
        undefined -> ok;
        _ -> os:cmd("rm -rf " ++ binary_to_list(Dir))
    end,
    ok.

test(Name, Fun) ->
    try
        Fun(),
        {Name, ok}
    catch C:R:_S ->
        {Name, {fail, {C, R}}}
    end.

%% ===== HTTP Helpers =====

http_put(Path, Body) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} =
        httpc:request(put, {Url, [{"content-type", "application/json"}], "application/json", Body}, [], []),
    {Code, RespBody}.

http_get(Path) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} = httpc:request(get, {Url, []}, [], []),
    {Code, RespBody}.

http_delete(Path) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} = httpc:request(delete, {Url, []}, [], []),
    {Code, RespBody}.

http_post(Path, Body) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} =
        httpc:request(post, {Url, [{"content-type", "application/json"}], "application/json", Body}, [], []),
    {Code, RespBody}.

%% ===== KV Tests =====

test_put_kv() ->
    {200, Body} = http_put("/kv/mykey", "{\"value\":\"hello\"}"),
    true = string:find(Body, "\"ok\":true") =/= nomatch.

test_get_kv() ->
    {200, _} = http_put("/kv/gettest", "{\"value\":\"world\"}"),
    {200, Body} = http_get("/kv/gettest"),
    true = string:find(Body, "\"value\":\"world\"") =/= nomatch.

test_get_kv_missing() ->
    {404, Body} = http_get("/kv/nonexistent_xyz"),
    true = string:find(Body, "not_found") =/= nomatch.

test_delete_kv() ->
    {200, _} = http_put("/kv/deltest", "{\"value\":\"bye\"}"),
    {200, Body} = http_delete("/kv/deltest"),
    true = string:find(Body, "\"deleted\":true") =/= nomatch.

test_delete_then_get() ->
    {200, _} = http_put("/kv/delget", "{\"value\":\"temp\"}"),
    {200, _} = http_delete("/kv/delget"),
    {404, _} = http_get("/kv/delget").

%% ===== Set Tests =====

test_set_add() ->
    {200, Body} = http_post("/set/myset/add", "{\"element\":\"alice\"}"),
    true = string:find(Body, "\"ok\":true") =/= nomatch.

test_set_members() ->
    {200, _} = http_post("/set/memtest/add", "{\"element\":\"alice\"}"),
    {200, _} = http_post("/set/memtest/add", "{\"element\":\"bob\"}"),
    {200, Body} = http_get("/set/memtest"),
    true = string:find(Body, "alice") =/= nomatch,
    true = string:find(Body, "bob") =/= nomatch.

test_set_missing() ->
    {404, _} = http_get("/set/nonexistent_set_xyz").

test_set_remove() ->
    {200, _} = http_post("/set/rmtest/add", "{\"element\":\"alice\"}"),
    {200, _} = http_post("/set/rmtest/add", "{\"element\":\"bob\"}"),
    {200, _} = http_post("/set/rmtest/remove", "{\"element\":\"alice\"}"),
    {200, Body} = http_get("/set/rmtest"),
    true = string:find(Body, "bob") =/= nomatch,
    nomatch = string:find(Body, "alice").

%% ===== Counter Tests =====

test_counter_inc() ->
    {200, Body} = http_post("/counter/hits/increment", "{}"),
    true = string:find(Body, "\"ok\":true") =/= nomatch.

test_counter_value() ->
    {200, _} = http_post("/counter/valtest/increment", "{\"amount\":5}"),
    {200, Body} = http_get("/counter/valtest"),
    true = string:find(Body, "\"value\":5") =/= nomatch.

test_counter_missing() ->
    {404, _} = http_get("/counter/nonexistent_counter_xyz").

test_counter_dec() ->
    {200, _} = http_post("/counter/dectest/increment", "{\"amount\":10}"),
    {200, _} = http_post("/counter/dectest/decrement", "{\"amount\":3}"),
    {200, Body} = http_get("/counter/dectest"),
    true = string:find(Body, "\"value\":7") =/= nomatch.

%% ===== Other Tests =====

test_status() ->
    {200, Body} = http_get("/status"),
    true = string:find(Body, "running") =/= nomatch.

test_unknown_route() ->
    {404, _} = http_get("/unknown/path").

test_put_missing_value() ->
    {400, Body} = http_put("/kv/badreq", "{}"),
    true = string:find(Body, "missing") =/= nomatch.
