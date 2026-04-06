-module(vordb_cluster_tests).
-export([run_all/0]).

%% Multi-instance integration tests using manual gossip dispatch.
%% Each "node" is a KvStore agent with its own vnode_id.

run_all() ->
    Results = [
        test("write node1, read node2 after gossip", fun test_write_read_gossip/0),
        test("writes to 3 nodes converge", fun test_three_node_converge/0),
        test("concurrent writes — LWW resolves", fun test_concurrent_lww/0),
        test("delete propagates via gossip", fun test_delete_propagation/0),
        test("node restart recovers from RocksDB", fun test_restart_recovery/0),
        test("set add gossip convergence", fun test_set_gossip/0),
        test("counter gossip convergence", fun test_counter_gossip/0),
        test("concurrent add/remove on OR-Set — add wins", fun test_orset_concurrent/0),
        test("mixed types gossip", fun test_mixed_gossip/0),
        test("restarted node re-syncs", fun test_restart_resync/0),
        test("partition and recovery", fun test_partition_recovery/0)
    ],
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length(Results) - Passed,
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("CLUSTER FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

test(Name, Fun) ->
    try Fun(), {Name, ok}
    catch C:R:_S -> {Name, {fail, {C, R}}}
    end.

%% ===== Helpers =====

setup() ->
    vordb_registry:start(),
    vordb_cache:init(),
    Dir = <<"/tmp/vordb_cluster_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    catch vordb_ffi:storage_stop(),
    catch gen_server:stop(vordb_dirty_tracker),
    {ok, _} = vordb_ffi:storage_start(Dir),
    {ok, _} = vordb_dirty_tracker:start_link([{peers, []}, {num_vnodes, 4}]),
    Dir.

start_node(NodeId) ->
    {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
        [{node_id, NodeId}, {vnode_id, 0}, {sync_interval_ms, 600000}], []),
    #{pid => Pid, node_id => NodeId}.

stop_node(#{pid := Pid}) ->
    catch gen_server:stop(Pid).

teardown(Dir, Nodes) ->
    lists:foreach(fun stop_node/1, Nodes),
    catch gen_server:stop(vordb_dirty_tracker),
    catch vordb_ffi:storage_stop(),
    os:cmd("rm -rf " ++ binary_to_list(Dir)).

put_kv(#{pid := Pid}, Key, Value) ->
    gen_server:call(Pid, {put, #{key => Key, value => Value}}).

get_kv(#{pid := Pid}, Key) ->
    gen_server:call(Pid, {get, #{key => Key}}).

delete_kv(#{pid := Pid}, Key) ->
    gen_server:call(Pid, {delete, #{key => Key}}).

set_add(#{pid := Pid}, Key, Element) ->
    gen_server:call(Pid, {set_add, #{key => Key, element => Element}}).

set_members(#{pid := Pid}, Key) ->
    gen_server:call(Pid, {set_members, #{key => Key}}).

counter_inc(#{pid := Pid}, Key, Amount) ->
    gen_server:call(Pid, {counter_increment, #{key => Key, amount => Amount}}).

counter_val(#{pid := Pid}, Key) ->
    gen_server:call(Pid, {counter_value, #{key => Key}}).

get_stores(#{pid := Pid}) ->
    {stores, S} = gen_server:call(Pid, {get_stores, #{}}),
    S.

gossip_lww(From, To) ->
    #{lww := Store} = get_stores(From),
    gen_server:cast(maps:get(pid, To), {lww_sync, #{remote_lww_store => Store}}),
    %% Synchronize
    get_stores(To).

gossip_sets(From, To) ->
    #{sets := Store} = get_stores(From),
    gen_server:cast(maps:get(pid, To), {set_sync, #{remote_set_store => Store}}),
    get_stores(To).

gossip_counters(From, To) ->
    #{counters := Store} = get_stores(From),
    gen_server:cast(maps:get(pid, To), {counter_sync, #{remote_counter_store => Store}}),
    get_stores(To).

gossip_all(From, To) ->
    gossip_lww(From, To),
    gossip_sets(From, To),
    gossip_counters(From, To).

full_mesh(Nodes) ->
    [gossip_all(F, T) || F <- Nodes, T <- Nodes, F =/= T],
    ok.

%% ===== Tests =====

test_write_read_gossip() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    put_kv(N1, <<"x">>, <<"hello">>),
    gossip_lww(N1, N2),
    {value, #{found := true, val := <<"hello">>}} = get_kv(N2, <<"x">>),
    teardown(Dir, [N1, N2]).

test_three_node_converge() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2), N3 = start_node(node3),
    put_kv(N1, <<"a">>, <<"from1">>),
    put_kv(N2, <<"b">>, <<"from2">>),
    put_kv(N3, <<"c">>, <<"from3">>),
    full_mesh([N1, N2, N3]), full_mesh([N1, N2, N3]),
    lists:foreach(fun(N) ->
        {value, #{found := true}} = get_kv(N, <<"a">>),
        {value, #{found := true}} = get_kv(N, <<"b">>),
        {value, #{found := true}} = get_kv(N, <<"c">>)
    end, [N1, N2, N3]),
    teardown(Dir, [N1, N2, N3]).

test_concurrent_lww() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    put_kv(N1, <<"x">>, <<"first">>),
    timer:sleep(2),
    put_kv(N2, <<"x">>, <<"second">>),
    gossip_lww(N1, N2), gossip_lww(N2, N1),
    {value, #{val := <<"second">>}} = get_kv(N1, <<"x">>),
    {value, #{val := <<"second">>}} = get_kv(N2, <<"x">>),
    teardown(Dir, [N1, N2]).

test_delete_propagation() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    put_kv(N1, <<"x">>, <<"hello">>),
    gossip_lww(N1, N2),
    {value, #{found := true}} = get_kv(N2, <<"x">>),
    delete_kv(N2, <<"x">>),
    gossip_lww(N2, N1),
    {value, #{found := false}} = get_kv(N1, <<"x">>),
    teardown(Dir, [N1, N2]).

test_restart_recovery() ->
    Dir = setup(),
    N1 = start_node(node1),
    put_kv(N1, <<"x">>, <<"persist">>),
    stop_node(N1),
    N1b = start_node(node1),
    {value, #{found := true, val := <<"persist">>}} = get_kv(N1b, <<"x">>),
    teardown(Dir, [N1b]).

test_set_gossip() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    set_add(N1, <<"s1">>, <<"alice">>),
    gossip_sets(N1, N2),
    {set_members, #{members := M}} = set_members(N2, <<"s1">>),
    true = lists:member(<<"alice">>, M),
    teardown(Dir, [N1, N2]).

test_counter_gossip() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    counter_inc(N1, <<"hits">>, 5),
    counter_inc(N2, <<"hits">>, 3),
    gossip_counters(N1, N2), gossip_counters(N2, N1),
    {counter_value, #{val := 8}} = counter_val(N1, <<"hits">>),
    {counter_value, #{val := 8}} = counter_val(N2, <<"hits">>),
    teardown(Dir, [N1, N2]).

test_orset_concurrent() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    set_add(N1, <<"s1">>, <<"alice">>),
    gossip_sets(N1, N2),
    %% N1 removes, N2 re-adds concurrently
    gen_server:call(maps:get(pid, N1), {set_remove, #{key => <<"s1">>, element => <<"alice">>}}),
    set_add(N2, <<"s1">>, <<"alice">>),
    gossip_sets(N1, N2), gossip_sets(N2, N1),
    {set_members, #{members := M1}} = set_members(N1, <<"s1">>),
    {set_members, #{members := M2}} = set_members(N2, <<"s1">>),
    true = lists:member(<<"alice">>, M1),
    true = lists:member(<<"alice">>, M2),
    teardown(Dir, [N1, N2]).

test_mixed_gossip() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    put_kv(N1, <<"k">>, <<"val">>),
    set_add(N1, <<"s">>, <<"alice">>),
    counter_inc(N1, <<"c">>, 5),
    gossip_all(N1, N2),
    {value, #{found := true}} = get_kv(N2, <<"k">>),
    {set_members, _} = set_members(N2, <<"s">>),
    {counter_value, #{val := 5}} = counter_val(N2, <<"c">>),
    teardown(Dir, [N1, N2]).

test_restart_resync() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2),
    put_kv(N1, <<"x">>, <<"before">>),
    gossip_lww(N1, N2),
    stop_node(N2),
    put_kv(N1, <<"y">>, <<"during">>),
    N2b = start_node(node2),
    gossip_lww(N1, N2b),
    {value, #{found := true, val := <<"before">>}} = get_kv(N2b, <<"x">>),
    {value, #{found := true, val := <<"during">>}} = get_kv(N2b, <<"y">>),
    teardown(Dir, [N1, N2b]).

test_partition_recovery() ->
    Dir = setup(),
    N1 = start_node(node1), N2 = start_node(node2), N3 = start_node(node3),
    put_kv(N1, <<"x">>, <<"before">>),
    full_mesh([N1, N2, N3]),
    %% Partition: only gossip N1<->N2, not N3
    put_kv(N1, <<"y">>, <<"during">>),
    gossip_lww(N1, N2), gossip_lww(N2, N1),
    {value, #{found := false}} = get_kv(N3, <<"y">>),
    %% Heal
    gossip_lww(N1, N3),
    {value, #{found := true, val := <<"before">>}} = get_kv(N3, <<"x">>),
    {value, #{found := true, val := <<"during">>}} = get_kv(N3, <<"y">>),
    teardown(Dir, [N1, N2, N3]).
