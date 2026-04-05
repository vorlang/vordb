-module(vordb_coordinator).
-export([write/2, read/2]).

%% Coordinator — routes writes to all N replicas, reads to best replica.
%% Not a process — runs in the caller's process (HTTP handler).

-define(REMOTE_TIMEOUT, 5000).

%% ===== Write =====
%% Sends to all replicas. Returns after local (or primary) succeeds.
%% Remote replicas receive async writes. Gossip catches up failures.

write(Key, Message) ->
    Partition = vordb_ring_manager:key_partition(Key),
    PrefList = vordb_ring_manager:key_nodes(Key),
    IsLocal = vordb_ring_manager:is_local(Partition),
    SelfNode = vordb_ring_manager:self_node(),

    case IsLocal of
        true ->
            %% Write locally (synchronous)
            case local_call(Partition, Message) of
                {error, _} = Err -> Err;
                Result ->
                    %% Fan out to remote replicas (async, fire-and-forget)
                    RemotePeers = [N || N <- PrefList, N =/= SelfNode],
                    fan_out_async(Partition, Message, RemotePeers),
                    {ok, Result}
            end;
        false ->
            %% Forward to primary (synchronous), fan out to rest (async)
            case PrefList of
                [] -> {error, no_replica_available};
                [Primary | Rest] ->
                    case remote_call(Primary, Partition, Message) of
                        {ok, Result} ->
                            fan_out_async(Partition, Message, Rest),
                            {ok, Result};
                        {error, _} ->
                            %% Primary unreachable, try next
                            try_next_replica(Rest, Partition, Message)
                    end
            end
    end.

%% ===== Read =====
%% Prefer local replica. Fallback to primary.

read(Key, Message) ->
    Partition = vordb_ring_manager:key_partition(Key),
    IsLocal = vordb_ring_manager:is_local(Partition),

    case IsLocal of
        true ->
            case local_call(Partition, Message) of
                {error, _} = Err -> Err;
                Result -> {ok, Result}
            end;
        false ->
            PrefList = vordb_ring_manager:key_nodes(Key),
            case PrefList of
                [] -> {error, no_replica_available};
                [Primary | _] -> remote_call(Primary, Partition, Message)
            end
    end.

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
