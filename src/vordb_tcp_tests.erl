-module(vordb_tcp_tests).
-export([run_all/0]).

-define(TCP_PORT, 15001).
-define(RING_SIZE, 8).

run_all() ->
    setup(),
    Results = [
        test("PUT via TCP returns ok", fun test_put/0),
        test("GET via TCP returns value", fun test_get/0),
        test("GET missing returns not_found", fun test_get_missing/0),
        test("DELETE via TCP", fun test_delete/0),
        test("set_add and set_members via TCP", fun test_set/0),
        test("counter_increment and counter_value via TCP", fun test_counter/0),
        test("request_id echoed", fun test_request_id/0),
        test("invalid protobuf returns error", fun test_invalid/0)
    ],
    teardown(),
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length(Results) - Passed,
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("TCP FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.

setup() ->
    Dir = <<"/tmp/vordb_tcp_test_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    catch vordb_ffi:storage_stop(),
    catch vordb_tcp:stop(),
    {ok, _} = vordb_ffi:storage_start(Dir),
    lists:foreach(fun(P) -> gen_server:call(vordb_storage, {cf_create, P}) end, lists:seq(0, ?RING_SIZE - 1)),
    vordb_registry:start(),
    vordb_cache:init(),
    vordb_metrics:init(),
    catch vordb_metrics:attach_handlers(),
    vordb_dirty_tracker:init(),
    catch gen_server:stop(vordb_ring_manager),
    {ok, _} = vordb_ring_manager:start_link(?RING_SIZE, 3, [<<"test_node">>], <<"test_node">>),
    MyParts = vordb_ring_manager:my_partitions(),
    lists:foreach(fun(P) ->
        {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
            [{node_id, test_node}, {vnode_id, P}, {sync_interval_ms, 600000}], []),
        vordb_registry:register({kv_store, P}, Pid),
        vordb_dirty_tracker:init_partition(P, [])
    end, MyParts),
    {ok, _} = vordb_tcp:start_link(?TCP_PORT),
    timer:sleep(100),
    put(tcp_dir, Dir),
    put(tcp_partitions, MyParts),
    ok.

teardown() ->
    catch vordb_tcp:stop(),
    Parts = case get(tcp_partitions) of undefined -> []; P -> P end,
    lists:foreach(fun(P) ->
        case vordb_registry:lookup({kv_store, P}) of
            {ok, Pid} -> catch gen_server:stop(Pid);
            _ -> ok
        end
    end, Parts),
    catch gen_server:stop(vordb_ring_manager),
    vordb_dirty_tracker:stop(),
    catch vordb_ffi:storage_stop(),
    Dir = get(tcp_dir),
    case Dir of undefined -> ok; _ -> os:cmd("rm -rf " ++ binary_to_list(Dir)) end.

test(Name, Fun) ->
    try Fun(), {Name, ok}
    catch C:R:_S -> {Name, {fail, {C, R}}}
    end.

%% Helpers

tcp_request(Msg) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", ?TCP_PORT, [binary, {active, false}, {packet, 4}]),
    Bin = vordb_pb:encode_msg(Msg, 'Request'),
    ok = gen_tcp:send(Sock, Bin),
    {ok, RespBin} = gen_tcp:recv(Sock, 0, 5000),
    gen_tcp:close(Sock),
    vordb_pb:decode_msg(RespBin, 'Response').

%% Tests

test_put() ->
    Req = #{request_id => 1, body => {put, #{key => <<"mykey">>, value => <<"hello">>}}},
    #{body := {ok, #{timestamp := Ts}}} = tcp_request(Req),
    true = Ts > 0.

test_get() ->
    tcp_request(#{request_id => 1, body => {put, #{key => <<"gettest">>, value => <<"world">>}}}),
    #{body := {value, #{key := <<"gettest">>, value := <<"world">>}}} =
        tcp_request(#{request_id => 2, body => {get, #{key => <<"gettest">>}}}).

test_get_missing() ->
    #{body := {not_found, _}} =
        tcp_request(#{request_id => 1, body => {get, #{key => <<"nonexistent_tcp">>}}}).

test_delete() ->
    tcp_request(#{request_id => 1, body => {put, #{key => <<"deltest">>, value => <<"bye">>}}}),
    #{body := {ok, _}} =
        tcp_request(#{request_id => 2, body => {delete, #{key => <<"deltest">>}}}),
    #{body := {not_found, _}} =
        tcp_request(#{request_id => 3, body => {get, #{key => <<"deltest">>}}}).

test_set() ->
    tcp_request(#{request_id => 1, body => {set_add, #{key => <<"myset">>, element => <<"alice">>}}}),
    tcp_request(#{request_id => 2, body => {set_add, #{key => <<"myset">>, element => <<"bob">>}}}),
    #{body := {set_members, #{members := Members}}} =
        tcp_request(#{request_id => 3, body => {set_members, #{key => <<"myset">>}}}),
    true = lists:member(<<"alice">>, Members),
    true = lists:member(<<"bob">>, Members).

test_counter() ->
    tcp_request(#{request_id => 1, body => {counter_increment, #{key => <<"hits">>, amount => 5}}}),
    #{body := {counter_value, #{value := 5}}} =
        tcp_request(#{request_id => 2, body => {counter_value, #{key => <<"hits">>}}}).

test_request_id() ->
    #{request_id := 42} =
        tcp_request(#{request_id => 42, body => {get, #{key => <<"any">>}}}).

test_invalid() ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", ?TCP_PORT, [binary, {active, false}, {packet, 4}]),
    ok = gen_tcp:send(Sock, <<0, 1, 2, 3>>),
    {ok, RespBin} = gen_tcp:recv(Sock, 0, 5000),
    gen_tcp:close(Sock),
    #{body := {error, _}} = vordb_pb:decode_msg(RespBin, 'Response').
