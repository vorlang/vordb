-module(vordb_coordinator).
-export([write/2, read/2, bucket_write/3, bucket_read/2]).

%% Coordinator — routes writes to all N replicas, reads from ETS cache.
%% Not a process — runs in the caller's process (HTTP handler).
%%
%% bucket_write/3 and bucket_read/2 add bucket awareness: they look up the
%% bucket config (persistent_term — no IPC), prefix the key with the bucket
%% name, validate the operation type, and delegate to the existing write/read.

-define(REMOTE_TIMEOUT, 5000).

%% ===== Write =====
%% Sends to all replicas. Returns after local (or primary) succeeds.

write(Key, Message) ->
    Start = erlang:monotonic_time(microsecond),
    Result = do_write(Key, Message),
    Duration = erlang:monotonic_time(microsecond) - Start,
    Op = element(1, Message),
    Status = case Result of {ok, _} -> ok; _ -> error end,
    catch vordb_metrics:emit_request(Op, http, Status, Duration),
    Result.

do_write(Key, Message) ->
    Partition = vordb_ring_manager:key_partition(Key),
    PrefList = vordb_ring_manager:key_nodes(Key),
    IsLocal = vordb_ring_manager:is_local(Partition),
    SelfNode = vordb_ring_manager:self_node(),

    case IsLocal of
        true ->
            case local_call(Partition, Message) of
                {error, _} = Err -> Err;
                Result ->
                    RemotePeers = [N || N <- PrefList, N =/= SelfNode],
                    fan_out_async(Partition, Message, RemotePeers),
                    {ok, Result}
            end;
        false ->
            case PrefList of
                [] -> {error, no_replica_available};
                [Primary | Rest] ->
                    case remote_call(Primary, Partition, Message) of
                        {ok, Result} ->
                            fan_out_async(Partition, Message, Rest),
                            {ok, Result};
                        {error, _} ->
                            try_next_replica(Rest, Partition, Message)
                    end
            end
    end.

%% ===== Read =====
%% Reads from ETS cache — gen_server not involved.
%% Falls back to gen_server call for unknown message types.

read(Key, Message) ->
    Start = erlang:monotonic_time(microsecond),
    Result = do_read(Key, Message),
    Duration = erlang:monotonic_time(microsecond) - Start,
    Op = element(1, Message),
    Status = case Result of {ok, _} -> ok; _ -> error end,
    catch vordb_metrics:emit_request(Op, http, Status, Duration),
    Result.

do_read(Key, Message) ->
    Partition = vordb_ring_manager:key_partition(Key),
    IsLocal = vordb_ring_manager:is_local(Partition),

    case IsLocal of
        true -> read_local(Partition, Key, Message);
        false ->
            PrefList = vordb_ring_manager:key_nodes(Key),
            case PrefList of
                [] -> {error, no_replica_available};
                [Primary | _] -> read_remote(Primary, Partition, Key, Message)
            end
    end.

%% Local read — ETS cache, no gen_server
read_local(Partition, Key, Message) ->
    case classify_read(Message) of
        {lww, K} ->
            case vordb_cache:get_lww(Partition, K) of
                {ok, Entry} ->
                    case is_expired_lww(Entry) of
                        true -> {ok, {value, #{key => K, val => none, found => false}}};
                        false ->
                            case is_lww_tombstone(Entry) of
                                true -> {ok, {value, #{key => K, val => none, found => false}}};
                                false ->
                                    Val = extract_lww_value(Entry),
                                    {ok, {value, #{key => K, val => Val, found => true}}}
                            end
                    end;
                {error, _} ->
                    {ok, {value, #{key => Key, val => none, found => false}}}
            end;
        {set_members, K} ->
            case is_expired_ttl(Partition, set, K) of
                true -> {ok, {set_not_found, #{key => K}}};
                false ->
                    case vordb_cache:get_set(Partition, K) of
                        {ok, SetState} ->
                            Members = 'vordb@or_set':read_elements(SetState),
                            {ok, {set_members, #{key => K, members => Members}}};
                        {error, _} ->
                            {ok, {set_not_found, #{key => K}}}
                    end
            end;
        {counter_value, K} ->
            case is_expired_ttl(Partition, counter, K) of
                true -> {ok, {counter_not_found, #{key => K}}};
                false ->
                    case vordb_cache:get_counter(Partition, K) of
                        {ok, CounterState} ->
                            Val = 'vordb@counter':value(CounterState),
                            {ok, {counter_value, #{key => K, val => Val}}};
                        {error, _} ->
                            {ok, {counter_not_found, #{key => K}}}
                    end
            end;
        unknown ->
            %% Fallback to gen_server for unknown message types
            case local_call(Partition, Message) of
                {error, _} = Err -> Err;
                Result -> {ok, Result}
            end
    end.

%% Remote read — ETS cache on remote node
read_remote(Node, Partition, Key, Message) ->
    try
        NodeAtom = binary_to_atom(Node),
        case classify_read(Message) of
            {lww, K} ->
                case erpc:call(NodeAtom, vordb_cache, get_lww, [Partition, K], ?REMOTE_TIMEOUT) of
                    {ok, Entry} ->
                        case is_lww_tombstone(Entry) of
                            true -> {ok, {value, #{key => K, val => none, found => false}}};
                            false ->
                                Val = extract_lww_value(Entry),
                                {ok, {value, #{key => K, val => Val, found => true}}}
                        end;
                    {error, _} ->
                        {ok, {value, #{key => Key, val => none, found => false}}}
                end;
            {set_members, K} ->
                case erpc:call(NodeAtom, vordb_cache, get_set, [Partition, K], ?REMOTE_TIMEOUT) of
                    {ok, SetState} ->
                        Members = 'vordb@or_set':read_elements(SetState),
                        {ok, {set_members, #{key => K, members => Members}}};
                    {error, _} ->
                        {ok, {set_not_found, #{key => K}}}
                end;
            {counter_value, K} ->
                case erpc:call(NodeAtom, vordb_cache, get_counter, [Partition, K], ?REMOTE_TIMEOUT) of
                    {ok, CounterState} ->
                        Val = 'vordb@counter':value(CounterState),
                        {ok, {counter_value, #{key => K, val => Val}}};
                    {error, _} ->
                        {ok, {counter_not_found, #{key => K}}}
                end;
            unknown ->
                remote_call(Node, Partition, Message)
        end
    catch _:_ ->
        {error, unreachable}
    end.

%% Classify the read message to determine which cache to query
classify_read({get, #{key := K}}) -> {lww, K};
classify_read({set_members, #{key := K}}) -> {set_members, K};
classify_read({counter_value, #{key := K}}) -> {counter_value, K};
classify_read(_) -> unknown.

%% TTL expiration checks
is_expired_lww(Entry) when is_map(Entry) ->
    case maps:get(expires_at, Entry, 0) of
        0 -> false;
        ExpiresAt -> erlang:system_time(millisecond) >= ExpiresAt
    end;
is_expired_lww(_) -> false.

is_expired_ttl(Partition, Type, Key) ->
    case vordb_cache:get_ttl(Partition, Type, Key) of
        0 -> false;
        ExpiresAt -> erlang:system_time(millisecond) >= ExpiresAt
    end.

%% LWW tombstone detection — handles both map format and Gleam tuple format
is_lww_tombstone(#{value := '__tombstone__'}) -> true;
is_lww_tombstone({tombstone, _, _}) -> true;
is_lww_tombstone(_) -> false.

%% Extract value from LWW entry — handles both map and Gleam tuple format
extract_lww_value(#{value := V}) -> V;
extract_lww_value({lww_entry, V, _, _}) -> V;
extract_lww_value(_) -> none.

%% ===== Bucket-aware entry points =====

%% Write through a bucket. BucketName is the bucket, Op is the operation atom
%% (put, delete, set_add, set_remove, counter_increment, counter_decrement),
%% Params is a map of operation-specific fields (key, value, element, amount).
bucket_write(BucketName, Op, Params) ->
    case vordb_bucket_registry:get_bucket(BucketName) of
        {error, not_found} -> {error, bucket_not_found};
        {ok, Bucket} ->
            Type = maps:get(crdt_type, Bucket),
            case validate_op(Type, Op) of
                ok ->
                    Key = maps:get(key, Params),
                    FullKey = <<BucketName/binary, ":", Key/binary>>,
                    TTL = maps:get(ttl_seconds, Bucket, 0),
                    Message = build_message(Op, FullKey, Params#{ttl_seconds => TTL}),
                    write(FullKey, Message);
                {error, _} = Err -> Err
            end
    end.

%% Read through a bucket. Op is get, set_members, or counter_value.
bucket_read(BucketName, Params) ->
    case vordb_bucket_registry:get_bucket(BucketName) of
        {error, not_found} -> {error, bucket_not_found};
        {ok, Bucket} ->
            Type = maps:get(crdt_type, Bucket),
            Key = maps:get(key, Params),
            FullKey = <<BucketName/binary, ":", Key/binary>>,
            Message = build_read_message(Type, FullKey),
            read(FullKey, Message)
    end.

validate_op(lww, put) -> ok;
validate_op(lww, delete) -> ok;
validate_op(orswot, set_add) -> ok;
validate_op(orswot, set_remove) -> ok;
validate_op(pn_counter, counter_increment) -> ok;
validate_op(pn_counter, counter_decrement) -> ok;
validate_op(ExpectedType, Op) -> {error, {type_mismatch, ExpectedType, Op}}.

build_message(put, FullKey, Params) ->
    {put, #{key => FullKey, value => maps:get(value, Params),
            ttl_seconds => maps:get(ttl_seconds, Params, 0)}};
build_message(delete, FullKey, Params) ->
    {delete, #{key => FullKey, ttl_seconds => maps:get(ttl_seconds, Params, 0)}};
build_message(set_add, FullKey, Params) ->
    {set_add, #{key => FullKey, element => maps:get(element, Params),
                ttl_seconds => maps:get(ttl_seconds, Params, 0)}};
build_message(set_remove, FullKey, Params) ->
    {set_remove, #{key => FullKey, element => maps:get(element, Params),
                   ttl_seconds => maps:get(ttl_seconds, Params, 0)}};
build_message(counter_increment, FullKey, Params) ->
    {counter_increment, #{key => FullKey, amount => maps:get(amount, Params, 1),
                          ttl_seconds => maps:get(ttl_seconds, Params, 0)}};
build_message(counter_decrement, FullKey, Params) ->
    {counter_decrement, #{key => FullKey, amount => maps:get(amount, Params, 1),
                          ttl_seconds => maps:get(ttl_seconds, Params, 0)}}.

build_read_message(lww, FullKey) ->
    {get, #{key => FullKey}};
build_read_message(orswot, FullKey) ->
    {set_members, #{key => FullKey}};
build_read_message(pn_counter, FullKey) ->
    {counter_value, #{key => FullKey}}.

%% ===== Internal =====

local_call(Partition, Message) ->
    case vordb_registry:lookup({kv_store, Partition}) of
        {ok, Pid} ->
            try gen_server:call(Pid, Message, ?REMOTE_TIMEOUT)
            catch _:_ -> {error, local_vnode_error}
            end;
        {error, _} -> {error, vnode_not_found}
    end.

remote_call(Node, Partition, Message) ->
    try
        NodeAtom = binary_to_atom(Node),
        Result = erpc:call(NodeAtom, fun() ->
            case vordb_registry:lookup({kv_store, Partition}) of
                {ok, Pid} -> gen_server:call(Pid, Message, ?REMOTE_TIMEOUT);
                {error, _} -> {error, vnode_not_found}
            end
        end, ?REMOTE_TIMEOUT),
        {ok, Result}
    catch _:_ ->
        {error, unreachable}
    end.

fan_out_async(Partition, Message, Nodes) ->
    lists:foreach(fun(Node) ->
        try
            NodeAtom = binary_to_atom(Node),
            erpc:cast(NodeAtom, fun() ->
                case vordb_registry:lookup({kv_store, Partition}) of
                    {ok, Pid} -> gen_server:call(Pid, Message, ?REMOTE_TIMEOUT);
                    _ -> ok
                end
            end)
        catch _:_ -> ok
        end
    end, Nodes).

try_next_replica([], _Partition, _Message) ->
    {error, all_replicas_unreachable};
try_next_replica([Node | Rest], Partition, Message) ->
    case remote_call(Node, Partition, Message) of
        {ok, _} = Ok -> Ok;
        {error, _} -> try_next_replica(Rest, Partition, Message)
    end.
