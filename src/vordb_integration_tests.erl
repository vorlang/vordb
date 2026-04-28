-module(vordb_integration_tests).
-export([
    run_kv_store_tests/0,
    run_storage_tests/0,
    run_all/0
]).

%% ===== KvStore Agent Tests =====

run_kv_store_tests() ->
    Results = [
        test("put_then_get", fun test_put_get/0),
        test("get_missing", fun test_get_missing/0),
        test("delete_then_get", fun test_delete_get/0),
        test("put_overwrite", fun test_put_overwrite/0),
        test("set_add_members", fun test_set_add_members/0),
        test("counter_inc_val", fun test_counter_inc_val/0),
        test("counter_missing", fun test_counter_missing/0),
        test("types_independent", fun test_types_independent/0),
        test("sync_merge", fun test_sync_merge/0)
    ],
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length([fail || {_, {fail, _}} <- Results]),
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

run_storage_tests() ->
    Results = [
        test("put_get_roundtrip", fun test_storage_put_get/0),
        test("get_all_vnode", fun test_storage_get_all_vnode/0),
        test("put_all_batch", fun test_storage_put_all/0),
        test("overwrite", fun test_storage_overwrite/0),
        test("restart_persist", fun test_storage_restart/0),
        test("shared_rocksdb", fun test_storage_shared/0)
    ],
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length([fail || {_, {fail, _}} <- Results]),
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

run_all() ->
    {KvP, KvF} = run_kv_store_tests(),
    {StP, StF} = run_storage_tests(),
    {KvP + StP, KvF + StF}.

test(Name, Fun) ->
    try
        Fun(),
        {Name, ok}
    catch C:R:_S ->
        {Name, {fail, {C, R}}}
    end.

%% ===== Setup/Teardown =====

setup() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    Pid = vordb_test_helpers:start_kv_store(test_node, 0),
    {Pid, Dir}.

teardown(Pid, Dir) ->
    gen_server:stop(Pid),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

%% ===== KvStore Tests =====

test_put_get() ->
    {Pid, Dir} = setup(),
    {ok, _} = gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"hello">>, ttl_seconds => 0}}),
    {value, #{found := true, val := <<"hello">>}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
    teardown(Pid, Dir).

test_get_missing() ->
    {Pid, Dir} = setup(),
    {value, #{found := false}} = gen_server:call(Pid, {get, #{key => <<"nonexistent">>}}),
    teardown(Pid, Dir).

test_delete_get() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"hello">>, ttl_seconds => 0}}),
    gen_server:call(Pid, {delete, #{key => <<"x">>, ttl_seconds => 0}}),
    {value, #{found := false}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
    teardown(Pid, Dir).

test_put_overwrite() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"v1">>, ttl_seconds => 0}}),
    gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"v2">>, ttl_seconds => 0}}),
    {value, #{found := true, val := <<"v2">>}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
    teardown(Pid, Dir).

test_set_add_members() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {set_add, #{key => <<"s1">>, element => <<"alice">>, ttl_seconds => 0}}),
    {set_members, #{members := Members}} = gen_server:call(Pid, {set_members, #{key => <<"s1">>}}),
    true = lists:member(<<"alice">>, Members),
    teardown(Pid, Dir).

test_counter_inc_val() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {counter_increment, #{key => <<"c1">>, amount => 1, ttl_seconds => 0}}),
    gen_server:call(Pid, {counter_increment, #{key => <<"c1">>, amount => 1, ttl_seconds => 0}}),
    gen_server:call(Pid, {counter_increment, #{key => <<"c1">>, amount => 1, ttl_seconds => 0}}),
    {counter_value, #{val := 3}} = gen_server:call(Pid, {counter_value, #{key => <<"c1">>}}),
    teardown(Pid, Dir).

test_counter_missing() ->
    {Pid, Dir} = setup(),
    {counter_not_found, _} = gen_server:call(Pid, {counter_value, #{key => <<"nonexistent">>}}),
    teardown(Pid, Dir).

test_types_independent() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"hello">>, ttl_seconds => 0}}),
    gen_server:call(Pid, {set_add, #{key => <<"x">>, element => <<"alice">>, ttl_seconds => 0}}),
    gen_server:call(Pid, {counter_increment, #{key => <<"x">>, amount => 42, ttl_seconds => 0}}),
    {value, #{found := true}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
    {set_members, _} = gen_server:call(Pid, {set_members, #{key => <<"x">>}}),
    {counter_value, #{val := 42}} = gen_server:call(Pid, {counter_value, #{key => <<"x">>}}),
    teardown(Pid, Dir).

test_sync_merge() ->
    {Pid, Dir} = setup(),
    gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"local">>, ttl_seconds => 0}}),
    RemoteEntry = #{value => <<"remote">>, timestamp => 99999, node_id => remote},
    RemoteStore = #{<<"y">> => RemoteEntry},
    gen_server:cast(Pid, {lww_sync, #{remote_lww_store => RemoteStore}}),
    gen_server:call(Pid, {get_stores, #{}}),
    {value, #{found := true}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
    {value, #{found := true}} = gen_server:call(Pid, {get, #{key => <<"y">>}}),
    teardown(Pid, Dir).

%% ===== Storage Tests =====

test_storage_put_get() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    Entry = #{value => <<"hello">>, timestamp => 1000, node_id => node1},
    ok = vordb_ffi:storage_put_lww(5, <<"key1">>, Entry),
    #{<<"key1">> := Entry} = vordb_ffi:storage_get_all_lww(5),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

test_storage_get_all_vnode() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    E1 = #{value => <<"a">>, timestamp => 1, node_id => n1},
    E2 = #{value => <<"b">>, timestamp => 2, node_id => n2},
    ok = vordb_ffi:storage_put_lww(5, <<"key1">>, E1),
    ok = vordb_ffi:storage_put_lww(5, <<"key2">>, E2),
    ok = vordb_ffi:storage_put_lww(6, <<"key1">>, #{value => <<"other">>, timestamp => 3, node_id => n3}),
    All5 = vordb_ffi:storage_get_all_lww(5),
    2 = maps:size(All5),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

test_storage_put_all() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    Entries = #{<<"a">> => #{value => <<"v1">>, timestamp => 1, node_id => n1},
                <<"b">> => #{value => <<"v2">>, timestamp => 2, node_id => n2}},
    ok = vordb_ffi:storage_put_all_lww(7, Entries),
    All = vordb_ffi:storage_get_all_lww(7),
    2 = maps:size(All),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

test_storage_overwrite() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    E1 = #{value => <<"v1">>, timestamp => 1, node_id => n1},
    E2 = #{value => <<"v2">>, timestamp => 2, node_id => n1},
    ok = vordb_ffi:storage_put_lww(0, <<"key1">>, E1),
    ok = vordb_ffi:storage_put_lww(0, <<"key1">>, E2),
    #{<<"key1">> := E2} = vordb_ffi:storage_get_all_lww(0),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

test_storage_restart() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    Entry = #{value => <<"persist">>, timestamp => 1000, node_id => node1},
    ok = vordb_ffi:storage_put_lww(0, <<"key1">>, Entry),
    vordb_ffi:storage_stop(),
    {ok, _} = vordb_ffi:storage_start(list_to_binary(Dir)),
    #{<<"key1">> := Entry} = vordb_ffi:storage_get_all_lww(0),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).

test_storage_shared() ->
    {ok, Dir} = vordb_test_helpers:start_storage(),
    ok = vordb_ffi:storage_put_lww(0, <<"x">>, #{value => <<"v0">>, timestamp => 1, node_id => n1}),
    ok = vordb_ffi:storage_put_lww(1, <<"x">>, #{value => <<"v1">>, timestamp => 2, node_id => n1}),
    1 = maps:size(vordb_ffi:storage_get_all_lww(0)),
    1 = maps:size(vordb_ffi:storage_get_all_lww(1)),
    vordb_ffi:storage_stop(),
    vordb_test_helpers:cleanup_dir(Dir).
