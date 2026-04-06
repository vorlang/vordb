-module(vordb_cache).
-export([
    init/0,
    cache_put/4, cache_get/3, cache_delete/3,
    cache_clear_partition/1, cache_load_partition/4,
    %% Vor agent extern targets (called via Erlang.vordb_cache.*)
    put_lww/3, put_set/3, put_counter/3,
    delete_lww/2,
    load_lww/2, load_sets/2, load_counters/2,
    %% Read path (called by coordinator)
    get_lww/2, get_set/2, get_counter/2
]).

-define(TABLE, vordb_cache).

%% ===== Table lifecycle =====

init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ets:new(?TABLE, [
                named_table, public, set,
                {read_concurrency, true},
                {write_concurrency, true}
            ]);
        _ -> ok
    end,
    ok.

%% ===== Generic operations =====

cache_put(Partition, Type, Key, Value) ->
    ets:insert(?TABLE, {{Partition, Type, Key}, Value}),
    ok.

cache_get(Partition, Type, Key) ->
    case ets:lookup(?TABLE, {Partition, Type, Key}) of
        [{_, Value}] -> {ok, Value};
        [] -> {error, not_found}
    end.

cache_delete(Partition, Type, Key) ->
    ets:delete(?TABLE, {Partition, Type, Key}),
    ok.

cache_clear_partition(Partition) ->
    ets:match_delete(?TABLE, {{Partition, '_', '_'}, '_'}),
    ok.

cache_load_partition(Partition, LwwData, SetData, CounterData) ->
    Entries =
        maps:fold(fun(K, V, Acc) -> [{{Partition, lww, K}, V} | Acc] end, [], LwwData) ++
        maps:fold(fun(K, V, Acc) -> [{{Partition, set, K}, V} | Acc] end, [], SetData) ++
        maps:fold(fun(K, V, Acc) -> [{{Partition, counter, K}, V} | Acc] end, [], CounterData),
    case Entries of
        [] -> ok;
        _ -> ets:insert(?TABLE, Entries)
    end,
    ok.

%% ===== Vor agent extern targets =====

put_lww(VnodeId, Key, Value) -> cache_put(VnodeId, lww, Key, Value).
put_set(VnodeId, Key, Value) -> cache_put(VnodeId, set, Key, Value).
put_counter(VnodeId, Key, Value) -> cache_put(VnodeId, counter, Key, Value).
delete_lww(VnodeId, Key) -> cache_delete(VnodeId, lww, Key).

load_lww(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, lww, K, V) end, ok, Entries).
load_sets(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, set, K, V) end, ok, Entries).
load_counters(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, counter, K, V) end, ok, Entries).

%% ===== Coordinator read path =====

get_lww(Partition, Key) -> cache_get(Partition, lww, Key).
get_set(Partition, Key) -> cache_get(Partition, set, Key).
get_counter(Partition, Key) -> cache_get(Partition, counter, Key).
