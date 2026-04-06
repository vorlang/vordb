-module(vordb_test_helpers).
-export([start_storage/0, start_dirty_tracker/0, start_kv_store/2,
         start_ring_manager/0, start_ring_manager/3,
         stop_storage/0, stop_ring_manager/0,
         cleanup_dir/1, make_entry/3]).

-define(TEST_RING_SIZE, 8).
-define(TEST_NVAL, 3).

start_storage() ->
    Dir = filename:join(os:getenv("TMPDIR", "/tmp"),
        "vordb_test_" ++ integer_to_list(erlang:unique_integer([positive]))),
    filelib:ensure_dir(filename:join(Dir, "dummy")),
    case whereis(vordb_storage) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end,
    {ok, _} = vordb_ffi:storage_start(list_to_binary(Dir)),
    {ok, Dir}.

stop_storage() ->
    case whereis(vordb_storage) of
        undefined -> ok;
        _ -> catch vordb_ffi:storage_stop()
    end.

start_ring_manager() ->
    start_ring_manager(?TEST_RING_SIZE, ?TEST_NVAL, [<<"test_node">>]).

start_ring_manager(RingSize, NVal, Nodes) ->
    case whereis(vordb_ring_manager) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end,
    {ok, _} = vordb_ring_manager:start_link(RingSize, NVal, Nodes, <<"test_node">>),
    ok.

stop_ring_manager() ->
    case whereis(vordb_ring_manager) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid)
    end.

start_dirty_tracker() ->
    case whereis(vordb_dirty_tracker) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end,
    {ok, _} = vordb_dirty_tracker:start_link([{peers, []}, {num_vnodes, ?TEST_RING_SIZE}]),
    ok.

start_kv_store(NodeId, VnodeId) ->
    %% Ensure dependencies running
    case whereis(vordb_dirty_tracker) of
        undefined -> start_dirty_tracker();
        _ -> ok
    end,
    case whereis(vordb_ring_manager) of
        undefined -> start_ring_manager();
        _ -> ok
    end,
    vordb_registry:start(),
    vordb_cache:init(),
    {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
        [{node_id, NodeId}, {vnode_id, VnodeId}, {sync_interval_ms, 600000}], []),
    Pid.

cleanup_dir(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

make_entry(Value, Timestamp, NodeId) ->
    #{value => Value, timestamp => Timestamp, node_id => NodeId}.
