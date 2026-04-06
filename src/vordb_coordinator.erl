-module(vordb_coordinator).
-export([write/2, read/2]).

%% Coordinator — routes writes to all N replicas, reads from ETS cache.
%% Not a process — runs in the caller's process (HTTP handler).

-define(REMOTE_TIMEOUT, 5000).

%% ===== Write =====
%% Sends to all replicas. Returns after local (or primary) succeeds.

write(Key, Message) ->
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
            case vordb_cache:get_set(Partition, K) of
                {ok, SetState} ->
                    Members = 'vordb@or_set':read_elements(SetState),
                    {ok, {set_members, #{key => K, members => Members}}};
                {error, _} ->
                    {ok, {set_not_found, #{key => K}}}
            end;
        {counter_value, K} ->
            case vordb_cache:get_counter(Partition, K) of
                {ok, CounterState} ->
                    Val = 'vordb@counter':value(CounterState),
                    {ok, {counter_value, #{key => K, val => Val}}};
                {error, _} ->
                    {ok, {counter_not_found, #{key => K}}}
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

%% LWW tombstone detection — handles both map format and Gleam tuple format
is_lww_tombstone(#{value := '__tombstone__'}) -> true;
is_lww_tombstone({tombstone, _, _}) -> true;
is_lww_tombstone(_) -> false.

%% Extract value from LWW entry — handles both map and Gleam tuple format
extract_lww_value(#{value := V}) -> V;
extract_lww_value({lww_entry, V, _, _}) -> V;
extract_lww_value(_) -> none.

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
