-module(vordb_bench_node).
-export([start/1, stop/0]).

%% Boots an in-VM VorDB node with TCP server bound to TcpPort.
%% Mirrors vordb_tcp_tests setup but with config tuned for benchmarking.

-define(RING_SIZE, 64).
-define(NVAL, 1).

start(TcpPort) ->
    Dir = list_to_binary(
            "/tmp/vordb_bench_" ++ integer_to_list(erlang:unique_integer([positive]))),
    catch vordb_ffi:storage_stop(),
    catch vordb_tcp:stop(),
    {ok, _} = vordb_ffi:storage_start(Dir),
    lists:foreach(fun(P) ->
        gen_server:call(vordb_storage, {cf_create, P})
    end, lists:seq(0, ?RING_SIZE - 1)),
    vordb_registry:start(),
    vordb_cache:init(),
    vordb_metrics:init(),
    catch vordb_metrics:attach_handlers(),
    vordb_dirty_tracker:init(),
    catch gen_server:stop(vordb_ring_manager),
    {ok, _} = vordb_ring_manager:start_link(
                ?RING_SIZE, ?NVAL, [<<"bench_node">>], <<"bench_node">>),
    MyParts = vordb_ring_manager:my_partitions(),
    lists:foreach(fun(P) ->
        {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
            [{node_id, bench_node}, {vnode_id, P}, {sync_interval_ms, 600000}], []),
        vordb_registry:register({kv_store, P}, Pid),
        vordb_dirty_tracker:init_partition(P, [])
    end, MyParts),
    {ok, _} = vordb_tcp:start_link(TcpPort),
    timer:sleep(150),
    put(bench_dir, Dir),
    put(bench_parts, MyParts),
    {ok, #{dir => Dir, partitions => MyParts, port => TcpPort}}.

stop() ->
    catch vordb_tcp:stop(),
    Parts = case get(bench_parts) of undefined -> []; P -> P end,
    lists:foreach(fun(P) ->
        case vordb_registry:lookup({kv_store, P}) of
            {ok, Pid} -> catch gen_server:stop(Pid);
            _ -> ok
        end
    end, Parts),
    catch gen_server:stop(vordb_ring_manager),
    catch vordb_dirty_tracker:stop(),
    catch vordb_ffi:storage_stop(),
    case get(bench_dir) of
        undefined -> ok;
        Dir -> os:cmd("rm -rf " ++ binary_to_list(Dir))
    end,
    ok.
