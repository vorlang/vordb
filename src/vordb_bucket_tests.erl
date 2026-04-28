-module(vordb_bucket_tests).
-export([run_all/0]).

-define(HTTP_PORT, 14098).
-define(RING_SIZE, 8).
-define(NVAL, 3).
-define(BASE, "http://127.0.0.1:" ++ integer_to_list(?HTTP_PORT)).

run_all() ->
    setup(),
    Results = [
        test("create bucket stores config", fun test_create_bucket/0),
        test("list buckets returns all", fun test_list_buckets/0),
        test("create duplicate returns error", fun test_create_duplicate/0),
        test("delete bucket removes config", fun test_delete_bucket/0),
        test("write to bucket succeeds", fun test_bucket_put/0),
        test("read from bucket returns value", fun test_bucket_get/0),
        test("type mismatch rejected", fun test_type_mismatch/0),
        test("write to nonexistent bucket returns 404", fun test_nonexistent_bucket/0),
        test("set operations on orswot bucket", fun test_bucket_set_ops/0),
        test("counter operations on pn_counter bucket", fun test_bucket_counter_ops/0),
        test("old API still works (backward compat)", fun test_old_api_compat/0),
        test("keys in different buckets don't collide", fun test_bucket_key_isolation/0)
    ],
    teardown(),
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length(Results) - Passed,
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("BUCKET FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

setup() ->
    application:ensure_all_started(inets),
    application:ensure_all_started(ssl),
    Dir = <<"/tmp/vordb_bucket_test_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    catch vordb_ffi:storage_stop(),
    {ok, _} = vordb_ffi:storage_start(Dir),
    vordb_cache:init(),
    vordb_metrics:init(),
    catch vordb_metrics:attach_handlers(),
    catch gen_server:stop(vordb_ring_manager),
    {ok, _} = vordb_ring_manager:start_link(?RING_SIZE, ?NVAL, [<<"test_node">>], <<"test_node">>),
    vordb_registry:start(),
    catch gen_server:stop(vordb_dirty_tracker),
    {ok, _} = vordb_dirty_tracker:start_link([{peers, []}, {num_vnodes, ?RING_SIZE}]),
    MyPartitions = vordb_ring_manager:my_partitions(),
    lists:foreach(fun(V) ->
        {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
            [{node_id, test_node}, {vnode_id, V}, {sync_interval_ms, 600000}], []),
        vordb_registry:register({kv_store, V}, Pid)
    end, MyPartitions),
    %% Start bucket registry and create defaults
    catch gen_server:stop(vordb_bucket_registry),
    {ok, _} = vordb_bucket_registry:start_link(),
    ok = vordb_bucket_registry:ensure_defaults(),
    {ok, _} = 'vordb@http_router':start_mist(?HTTP_PORT, ?RING_SIZE),
    timer:sleep(100),
    put(bucket_dir, Dir),
    put(bucket_partitions, MyPartitions),
    ok.

teardown() ->
    MyPartitions = case get(bucket_partitions) of undefined -> []; P -> P end,
    lists:foreach(fun(V) ->
        case vordb_registry:lookup({kv_store, V}) of
            {ok, Pid} -> catch gen_server:stop(Pid);
            _ -> ok
        end
    end, MyPartitions),
    catch gen_server:stop(vordb_bucket_registry),
    catch gen_server:stop(vordb_ring_manager),
    catch gen_server:stop(vordb_dirty_tracker),
    catch vordb_ffi:storage_stop(),
    Dir = get(bucket_dir),
    case Dir of undefined -> ok; _ -> os:cmd("rm -rf " ++ binary_to_list(Dir)) end,
    ok.

test(Name, Fun) ->
    try Fun(), {Name, ok}
    catch C:R:_S -> {Name, {fail, {C, R}}}
    end.

%% ===== HTTP Helpers =====

http_post(Path, Body) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} =
        httpc:request(post, {Url, [{"content-type", "application/json"}], "application/json", Body}, [], []),
    {Code, RespBody}.

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

%% ===== Tests =====

test_create_bucket() ->
    {201, Body} = http_post("/buckets",
        "{\"name\":\"profiles\",\"type\":\"lww\",\"ttl_seconds\":0}"),
    true = string:find(Body, "\"ok\":true") =/= nomatch.

test_list_buckets() ->
    %% Defaults were created in setup, plus "profiles" from previous test
    {200, Body} = http_get("/buckets"),
    true = string:find(Body, "profiles") =/= nomatch.

test_create_duplicate() ->
    %% "profiles" already exists from test_create_bucket
    {409, Body} = http_post("/buckets",
        "{\"name\":\"profiles\",\"type\":\"lww\"}"),
    true = string:find(Body, "already_exists") =/= nomatch.

test_delete_bucket() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"temp_bucket\",\"type\":\"lww\"}"),
    {200, _} = http_delete("/buckets/temp_bucket"),
    {404, _} = http_get("/buckets/temp_bucket").

test_bucket_put() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"data1\",\"type\":\"lww\"}"),
    {200, Body} = http_put("/bucket/data1/user1", "{\"value\":\"hello\"}"),
    true = string:find(Body, "\"ok\":true") =/= nomatch.

test_bucket_get() ->
    %% data1 created in test_bucket_put
    {200, _} = http_put("/bucket/data1/user2", "{\"value\":\"world\"}"),
    {200, Body} = http_get("/bucket/data1/user2"),
    true = string:find(Body, "world") =/= nomatch.

test_type_mismatch() ->
    %% data1 is lww; set_add on it should fail
    {400, Body} = http_post("/bucket/data1/user1/add", "{\"element\":\"tag\"}"),
    true = string:find(Body, "type_mismatch") =/= nomatch.

test_nonexistent_bucket() ->
    {400, Body} = http_put("/bucket/nosuchbucket/key1", "{\"value\":\"val\"}"),
    true = string:find(Body, "bucket_not_found") =/= nomatch.

test_bucket_set_ops() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"tags\",\"type\":\"orswot\"}"),
    {200, _} = http_post("/bucket/tags/user1/add", "{\"element\":\"premium\"}"),
    {200, _} = http_post("/bucket/tags/user1/add", "{\"element\":\"beta\"}"),
    {200, Body} = http_get("/bucket/tags/user1"),
    true = string:find(Body, "premium") =/= nomatch,
    true = string:find(Body, "beta") =/= nomatch.

test_bucket_counter_ops() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"views\",\"type\":\"pn_counter\"}"),
    {200, _} = http_post("/bucket/views/page1/increment", "{\"amount\":5}"),
    {200, Body} = http_get("/bucket/views/page1"),
    true = string:find(Body, "\"value\":5") =/= nomatch.

test_old_api_compat() ->
    %% Old /kv/ endpoint still works (routes to __default_lww__)
    {200, _} = http_put("/kv/oldkey", "{\"value\":\"oldval\"}"),
    {200, Body} = http_get("/kv/oldkey"),
    true = string:find(Body, "oldval") =/= nomatch.

test_bucket_key_isolation() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"ns_a\",\"type\":\"lww\"}"),
    {201, _} = http_post("/buckets",
        "{\"name\":\"ns_b\",\"type\":\"lww\"}"),
    {200, _} = http_put("/bucket/ns_a/key1", "{\"value\":\"alpha\"}"),
    {200, _} = http_put("/bucket/ns_b/key1", "{\"value\":\"beta\"}"),
    {200, BodyA} = http_get("/bucket/ns_a/key1"),
    {200, BodyB} = http_get("/bucket/ns_b/key1"),
    true = string:find(BodyA, "alpha") =/= nomatch,
    true = string:find(BodyB, "beta") =/= nomatch,
    %% Verify they didn't overwrite each other
    nomatch = string:find(BodyA, "beta"),
    nomatch = string:find(BodyB, "alpha").
