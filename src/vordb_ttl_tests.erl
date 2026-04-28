-module(vordb_ttl_tests).
-export([run_all/0]).

-define(HTTP_PORT, 14097).
-define(RING_SIZE, 8).
-define(NVAL, 3).
-define(BASE, "http://127.0.0.1:" ++ integer_to_list(?HTTP_PORT)).

run_all() ->
    setup(),
    Results = [
        test("key expires after TTL", fun test_lww_expires/0),
        test("no-TTL bucket keys never expire", fun test_no_ttl/0),
        test("write resets TTL", fun test_lww_ttl_reset/0),
        test("set expires after TTL", fun test_set_expires/0),
        test("set_add resets TTL", fun test_set_ttl_reset/0),
        test("counter expires after TTL", fun test_counter_expires/0),
        test("expired key purged by sweep", fun test_sweep_purges/0),
        test("old entries without expires_at never expire", fun test_backward_compat/0)
    ],
    teardown(),
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length(Results) - Passed,
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("TTL FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

setup() ->
    application:ensure_all_started(inets),
    application:ensure_all_started(ssl),
    Dir = <<"/tmp/vordb_ttl_test_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
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
    catch gen_server:stop(vordb_bucket_registry),
    {ok, _} = vordb_bucket_registry:start_link(),
    ok = vordb_bucket_registry:ensure_defaults(),
    {ok, _} = 'vordb@http_router':start_mist(?HTTP_PORT, ?RING_SIZE),
    timer:sleep(100),
    put(ttl_dir, Dir),
    put(ttl_partitions, MyPartitions),
    ok.

teardown() ->
    MyPartitions = case get(ttl_partitions) of undefined -> []; P -> P end,
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
    Dir = get(ttl_dir),
    case Dir of undefined -> ok; _ -> os:cmd("rm -rf " ++ binary_to_list(Dir)) end,
    ok.

test(Name, Fun) ->
    try Fun(), {Name, ok}
    catch C:R:_S -> {Name, {fail, {C, R}}}
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

http_post(Path, Body) ->
    Url = ?BASE ++ Path,
    {ok, {{_, Code, _}, _, RespBody}} =
        httpc:request(post, {Url, [{"content-type", "application/json"}], "application/json", Body}, [], []),
    {Code, RespBody}.

%% ===== Tests =====

test_lww_expires() ->
    %% Bucket with 1 second TTL
    {201, _} = http_post("/buckets",
        "{\"name\":\"ttl1\",\"type\":\"lww\",\"ttl_seconds\":1}"),
    {200, _} = http_put("/bucket/ttl1/key1", "{\"value\":\"hello\"}"),
    %% Immediately readable
    {200, Body1} = http_get("/bucket/ttl1/key1"),
    true = string:find(Body1, "hello") =/= nomatch,
    %% Wait past TTL
    timer:sleep(1500),
    {404, _} = http_get("/bucket/ttl1/key1").

test_no_ttl() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"nottl\",\"type\":\"lww\",\"ttl_seconds\":0}"),
    {200, _} = http_put("/bucket/nottl/key1", "{\"value\":\"persist\"}"),
    timer:sleep(2000),
    {200, Body} = http_get("/bucket/nottl/key1"),
    true = string:find(Body, "persist") =/= nomatch.

test_lww_ttl_reset() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"ttl2\",\"type\":\"lww\",\"ttl_seconds\":2}"),
    %% Write at T=0
    {200, _} = http_put("/bucket/ttl2/key1", "{\"value\":\"v1\"}"),
    %% Sleep 1s
    timer:sleep(1000),
    %% Rewrite at T=1 (resets TTL to T=1+2=T=3)
    {200, _} = http_put("/bucket/ttl2/key1", "{\"value\":\"v2\"}"),
    %% Sleep 1.5s (T=2.5 — past original TTL but within reset TTL)
    timer:sleep(1500),
    {200, Body} = http_get("/bucket/ttl2/key1"),
    true = string:find(Body, "v2") =/= nomatch,
    %% Sleep 1.5s more (T=4 — past reset TTL of T=3)
    timer:sleep(1500),
    {404, _} = http_get("/bucket/ttl2/key1").

test_set_expires() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"ttlset\",\"type\":\"orswot\",\"ttl_seconds\":1}"),
    {200, _} = http_post("/bucket/ttlset/s1/add", "{\"element\":\"alice\"}"),
    {200, Body1} = http_get("/bucket/ttlset/s1"),
    true = string:find(Body1, "alice") =/= nomatch,
    timer:sleep(1500),
    {404, _} = http_get("/bucket/ttlset/s1").

test_set_ttl_reset() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"ttlset2\",\"type\":\"orswot\",\"ttl_seconds\":2}"),
    {200, _} = http_post("/bucket/ttlset2/s1/add", "{\"element\":\"alice\"}"),
    timer:sleep(1000),
    %% Add another element — resets TTL
    {200, _} = http_post("/bucket/ttlset2/s1/add", "{\"element\":\"bob\"}"),
    timer:sleep(1500),
    %% Should still be alive (TTL reset)
    {200, Body} = http_get("/bucket/ttlset2/s1"),
    true = string:find(Body, "alice") =/= nomatch,
    true = string:find(Body, "bob") =/= nomatch.

test_counter_expires() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"ttlcnt\",\"type\":\"pn_counter\",\"ttl_seconds\":1}"),
    {200, _} = http_post("/bucket/ttlcnt/c1/increment", "{\"amount\":5}"),
    {200, Body1} = http_get("/bucket/ttlcnt/c1"),
    true = string:find(Body1, "5") =/= nomatch,
    timer:sleep(1500),
    {404, _} = http_get("/bucket/ttlcnt/c1").

test_sweep_purges() ->
    {201, _} = http_post("/buckets",
        "{\"name\":\"sweep\",\"type\":\"lww\",\"ttl_seconds\":1}"),
    %% Write 10 keys
    lists:foreach(fun(I) ->
        K = "k" ++ integer_to_list(I),
        {200, _} = http_put("/bucket/sweep/" ++ K, "{\"value\":\"val\"}")
    end, lists:seq(1, 10)),
    timer:sleep(1500),
    %% Run sweep on all partitions
    Parts = vordb_ring_manager:my_partitions(),
    Purged = lists:sum([vordb_cache:sweep_expired(P) || P <- Parts]),
    true = Purged >= 10.

test_backward_compat() ->
    %% Old API (no bucket, no TTL) — keys should never expire
    {200, _} = http_put("/kv/oldttl", "{\"value\":\"forever\"}"),
    timer:sleep(1000),
    {200, Body} = http_get("/kv/oldttl"),
    true = string:find(Body, "forever") =/= nomatch.
