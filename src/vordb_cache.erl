-module(vordb_cache).
-export([
    init/0,
    cache_put/4, cache_get/3, cache_delete/3,
    cache_clear_partition/1, cache_load_partition/4,
    %% Vor agent extern targets (called via Erlang.vordb_cache.*)
    put_lww/3, put_set/3, put_counter/3,
    put_set_ttl/3, put_counter_ttl/3,
    delete_lww/2,
    load_lww/2, load_sets/2, load_counters/2,
    %% Read path (called by coordinator)
    get_lww/2, get_set/2, get_counter/2,
    get_ttl/3,
    %% TTL sweep
    sweep_expired/1
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
put_set_ttl(VnodeId, Key, ExpiresAt) ->
    case ExpiresAt of 0 -> ok; _ -> cache_put(VnodeId, set_ttl, Key, ExpiresAt) end.
put_counter_ttl(VnodeId, Key, ExpiresAt) ->
    case ExpiresAt of 0 -> ok; _ -> cache_put(VnodeId, counter_ttl, Key, ExpiresAt) end.
delete_lww(VnodeId, Key) -> cache_delete(VnodeId, lww, Key).

load_lww(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, lww, K, V) end, ok, Entries).
load_sets(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, set, K, V) end, ok, Entries).
load_counters(VnodeId, Entries) ->
    maps:fold(fun(K, V, _) -> cache_put(VnodeId, counter, K, V) end, ok, Entries).

%% ===== Coordinator read path =====

get_lww(Partition, Key) ->
    R = cache_get(Partition, lww, Key),
    case R of {ok, _} -> catch vordb_metrics:emit_cache_hit(lww); _ -> catch vordb_metrics:emit_cache_miss(lww) end,
    R.
get_set(Partition, Key) ->
    R = cache_get(Partition, set, Key),
    case R of {ok, _} -> catch vordb_metrics:emit_cache_hit(set); _ -> catch vordb_metrics:emit_cache_miss(set) end,
    R.
get_counter(Partition, Key) ->
    R = cache_get(Partition, counter, Key),
    case R of {ok, _} -> catch vordb_metrics:emit_cache_hit(counter); _ -> catch vordb_metrics:emit_cache_miss(counter) end,
    R.

%% Get TTL expires_at for a key. Returns 0 if no TTL set.
get_ttl(Partition, Type, Key) ->
    TtlType = ttl_type(Type),
    case cache_get(Partition, TtlType, Key) of
        {ok, ExpiresAt} -> ExpiresAt;
        _ -> 0
    end.

ttl_type(lww) -> lww;  %% LWW stores expires_at inside the entry map
ttl_type(set) -> set_ttl;
ttl_type(counter) -> counter_ttl;
ttl_type(Other) -> Other.

%% Sweep expired entries from cache for a partition.
%% Returns count of purged entries.
sweep_expired(Partition) ->
    Now = erlang:system_time(millisecond),
    %% LWW: check expires_at inside entry maps
    LwwPurged = sweep_lww(Partition, Now),
    %% Sets and counters: check separate TTL entries
    SetPurged = sweep_type_ttl(Partition, set, set_ttl, Now),
    CounterPurged = sweep_type_ttl(Partition, counter, counter_ttl, Now),
    LwwPurged + SetPurged + CounterPurged.

sweep_lww(Partition, Now) ->
    %% Select all LWW entries for this partition
    Pattern = {{Partition, lww, '_'}, '_'},
    Entries = ets:match_object(?TABLE, Pattern),
    lists:foldl(fun({{_Part, lww, Key}, Entry}, Count) ->
        ExpiresAt = case is_map(Entry) of
            true -> maps:get(expires_at, Entry, 0);
            false -> 0
        end,
        case ExpiresAt > 0 andalso Now >= ExpiresAt of
            true ->
                ets:delete(?TABLE, {Partition, lww, Key}),
                Count + 1;
            false -> Count
        end
    end, 0, Entries).

sweep_type_ttl(Partition, DataType, TtlType, Now) ->
    Pattern = {{Partition, TtlType, '_'}, '_'},
    Entries = ets:match_object(?TABLE, Pattern),
    lists:foldl(fun({{_Part, _TtlT, Key}, ExpiresAt}, Count) ->
        case ExpiresAt > 0 andalso Now >= ExpiresAt of
            true ->
                ets:delete(?TABLE, {Partition, TtlType, Key}),
                ets:delete(?TABLE, {Partition, DataType, Key}),
                Count + 1;
            false -> Count
        end
    end, 0, Entries).
