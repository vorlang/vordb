-module(vordb_quorum).
-export([quorum_write/4, quorum_read/6]).

%% Quorum write/read machinery. Spawns one process per replica, collects
%% W or R responses, returns to caller. Read path merges responses and
%% triggers async read repair for stale replicas.

-define(WRITE_TIMEOUT, 5000).
-define(READ_TIMEOUT, 3000).

%% ===== Quorum Write =====
%% Send the write to all replicas. Wait for W successes. Return after W met.

quorum_write(Partition, Message, Replicas, W) ->
    Self = self(),
    Ref = make_ref(),
    SelfNode = vordb_ring_manager:self_node(),
    lists:foreach(fun(Node) ->
        spawn(fun() ->
            Result = try
                case Node =:= SelfNode of
                    true -> local_write(Partition, Message);
                    false -> remote_write(Node, Partition, Message)
                end
            catch _:_ -> {error, unreachable}
            end,
            Self ! {Ref, Node, Result}
        end)
    end, Replicas),
    collect_write(Ref, W, length(Replicas), ?WRITE_TIMEOUT, [], []).

collect_write(_Ref, 0, _Rem, _Timeout, [Ok | _], _Errs) ->
    {ok, Ok};
collect_write(_Ref, _W, 0, _Timeout, _Oks, Errs) ->
    {error, {insufficient_replicas, Errs}};
collect_write(Ref, W, Rem, Timeout, Oks, Errs) ->
    receive
        {Ref, _Node, {ok, R}} ->
            collect_write(Ref, W - 1, Rem - 1, Timeout, [R | Oks], Errs);
        {Ref, _Node, ok} ->
            collect_write(Ref, W - 1, Rem - 1, Timeout, [ok | Oks], Errs);
        {Ref, Node, {error, _} = E} ->
            collect_write(Ref, W, Rem - 1, Timeout, Oks, [{Node, E} | Errs])
    after Timeout ->
        {error, {write_timeout, length(Oks), W}}
    end.

local_write(Partition, Message) ->
    case vordb_registry:lookup({kv_store, Partition}) of
        {ok, Pid} ->
            Result = gen_server:call(Pid, Message, ?WRITE_TIMEOUT),
            {ok, Result};
        {error, _} -> {error, vnode_not_found}
    end.

remote_write(Node, Partition, Message) ->
    NodeAtom = binary_to_atom(Node),
    erpc:call(NodeAtom, fun() ->
        case vordb_registry:lookup({kv_store, Partition}) of
            {ok, Pid} -> {ok, gen_server:call(Pid, Message, ?WRITE_TIMEOUT)};
            {error, _} -> {error, vnode_not_found}
        end
    end, ?WRITE_TIMEOUT).

%% ===== Quorum Read =====
%% Read from ALL N replicas. Return after R responses, merge, then async
%% repair any stale replicas from remaining responses.

quorum_read(Partition, Key, CrdtType, Replicas, R, TTLExpiresAt) ->
    Self = self(),
    Ref = make_ref(),
    SelfNode = vordb_ring_manager:self_node(),
    N = length(Replicas),
    lists:foreach(fun(Node) ->
        spawn(fun() ->
            Result = try
                case Node =:= SelfNode of
                    true -> local_read(Partition, CrdtType, Key);
                    false -> remote_read(Node, Partition, CrdtType, Key)
                end
            catch _:_ -> {error, unreachable}
            end,
            Self ! {Ref, Node, Result}
        end)
    end, Replicas),
    collect_read(Ref, R, N, ?READ_TIMEOUT, CrdtType, Partition, Key, TTLExpiresAt, []).

collect_read(Ref, 0, Rem, _Timeout, CrdtType, Partition, Key, TTLExpiresAt, Responses) ->
    %% R met — merge, return, and spawn background repair for remaining
    Merged = merge_responses(CrdtType, Responses),
    %% Check TTL on merged result
    case is_ttl_expired(CrdtType, Merged, TTLExpiresAt) of
        true ->
            flush_remaining(Ref, Rem),
            {ok, make_not_found(CrdtType, Key)};
        false ->
            spawn(fun() ->
                repair_remaining(Ref, Rem, ?READ_TIMEOUT, CrdtType, Partition, Key, Merged, Responses)
            end),
            {ok, format_read_result(CrdtType, Key, Merged)}
    end;
collect_read(_Ref, R, 0, _Timeout, _CrdtType, _Part, _Key, _TTL, Responses) when R > 0 ->
    %% All replicas responded but not enough successes to meet R
    {error, {insufficient_read_replicas, length(Responses), R}};
collect_read(Ref, R, Rem, Timeout, CrdtType, Partition, Key, TTLExpiresAt, Responses) ->
    receive
        {Ref, Node, {ok, Value}} ->
            collect_read(Ref, R - 1, Rem - 1, Timeout, CrdtType, Partition, Key,
                         TTLExpiresAt, [{Node, Value} | Responses]);
        {Ref, _Node, {error, _}} ->
            collect_read(Ref, R, Rem - 1, Timeout, CrdtType, Partition, Key,
                         TTLExpiresAt, Responses)
    after Timeout ->
        {error, {read_timeout, length(Responses), R}}
    end.

flush_remaining(_Ref, 0) -> ok;
flush_remaining(Ref, Rem) ->
    receive {Ref, _, _} -> flush_remaining(Ref, Rem - 1)
    after 0 -> ok
    end.

%% ===== Read response merging =====

merge_responses(lww, Responses) ->
    %% Pick highest timestamp; ties broken by node_id
    lists:foldl(fun({_Node, Entry}, Best) ->
        case Best of
            none -> Entry;
            _ -> pick_lww_winner(Entry, Best)
        end
    end, none, Responses);
merge_responses(orswot, Responses) ->
    Values = [V || {_, V} <- Responses],
    case Values of
        [] -> none;
        [First | Rest] ->
            lists:foldl(fun(V, Acc) ->
                'vordb@or_set':merge(Acc, V)
            end, First, Rest)
    end;
merge_responses(pn_counter, Responses) ->
    Values = [V || {_, V} <- Responses],
    case Values of
        [] -> none;
        [First | Rest] ->
            lists:foldl(fun(V, Acc) ->
                vordb_ffi:counter_merge(Acc, V)
            end, First, Rest)
    end.

pick_lww_winner(A, B) when is_map(A), is_map(B) ->
    TsA = maps:get(timestamp, A, 0),
    TsB = maps:get(timestamp, B, 0),
    if
        TsA > TsB -> A;
        TsA < TsB -> B;
        true ->
            NidA = maps:get(node_id, A, <<>>),
            NidB = maps:get(node_id, B, <<>>),
            if NidA >= NidB -> A; true -> B end
    end;
pick_lww_winner(A, _) -> A.

%% ===== TTL check on merged result =====

is_ttl_expired(lww, Entry, _) when is_map(Entry) ->
    case maps:get(expires_at, Entry, 0) of
        0 -> false;
        Exp -> erlang:system_time(millisecond) >= Exp
    end;
is_ttl_expired(_, _, 0) -> false;
is_ttl_expired(_, _, ExpiresAt) ->
    erlang:system_time(millisecond) >= ExpiresAt;
is_ttl_expired(_, _, _) -> false.

%% ===== Format read results =====

format_read_result(lww, Key, Entry) when is_map(Entry) ->
    case maps:get(value, Entry, none) of
        '__tombstone__' -> {value, #{key => Key, val => none, found => false}};
        Val -> {value, #{key => Key, val => Val, found => true}}
    end;
format_read_result(lww, Key, _) ->
    {value, #{key => Key, val => none, found => false}};
format_read_result(orswot, Key, none) ->
    {set_not_found, #{key => Key}};
format_read_result(orswot, Key, State) ->
    Members = 'vordb@or_set':read_elements(State),
    {set_members, #{key => Key, members => Members}};
format_read_result(pn_counter, Key, none) ->
    {counter_not_found, #{key => Key}};
format_read_result(pn_counter, Key, State) ->
    Val = vordb_ffi:counter_value(State),
    {counter_value, #{key => Key, val => Val}}.

make_not_found(lww, Key) ->
    {value, #{key => Key, val => none, found => false}};
make_not_found(orswot, Key) ->
    {set_not_found, #{key => Key}};
make_not_found(pn_counter, Key) ->
    {counter_not_found, #{key => Key}}.

%% ===== Read repair =====

repair_remaining(_Ref, 0, _Timeout, _Type, _Part, _Key, _Best, _AlreadySeen) -> ok;
repair_remaining(Ref, Rem, Timeout, CrdtType, Partition, Key, Best, AlreadySeen) ->
    receive
        {Ref, Node, {ok, Value}} ->
            case is_stale(CrdtType, Value, Best) of
                true -> send_repair(Node, Partition, Key, CrdtType, Best);
                false -> ok
            end,
            repair_remaining(Ref, Rem - 1, Timeout, CrdtType, Partition, Key, Best, AlreadySeen);
        {Ref, _Node, {error, _}} ->
            repair_remaining(Ref, Rem - 1, Timeout, CrdtType, Partition, Key, Best, AlreadySeen)
    after Timeout ->
        ok
    end,
    %% Also repair already-seen stale responses
    lists:foreach(fun({Node, Value}) ->
        case is_stale(CrdtType, Value, Best) of
            true -> send_repair(Node, Partition, Key, CrdtType, Best);
            false -> ok
        end
    end, AlreadySeen).

is_stale(lww, ReplicaEntry, MergedEntry) when is_map(ReplicaEntry), is_map(MergedEntry) ->
    maps:get(timestamp, ReplicaEntry, 0) < maps:get(timestamp, MergedEntry, 0);
is_stale(orswot, ReplicaState, MergedState) ->
    %% Stale if the replica clock is dominated by the merged clock
    ReplicaClock = get_orswot_clock(ReplicaState),
    MergedClock = get_orswot_clock(MergedState),
    clock_dominated(ReplicaClock, MergedClock);
is_stale(pn_counter, ReplicaState, MergedState) ->
    %% Stale if any node's count is behind
    RP = maps:get(p, ReplicaState, #{}),
    MP = maps:get(p, MergedState, #{}),
    RN = maps:get(n, ReplicaState, #{}),
    MN = maps:get(n, MergedState, #{}),
    any_behind(RP, MP) orelse any_behind(RN, MN);
is_stale(_, _, _) -> false.

get_orswot_clock(State) ->
    %% Gleam Orswot is {or_set_state, Entries, Clock}
    case State of
        {or_set_state, _, Clock} -> Clock;
        _ -> #{}
    end.

clock_dominated(Replica, Merged) ->
    maps:fold(fun(Node, MergedCount, Acc) ->
        Acc andalso maps:get(Node, Replica, 0) =< MergedCount
    end, true, Merged) andalso
    maps:size(Replica) =< maps:size(Merged).

any_behind(ReplicaMap, MergedMap) ->
    maps:fold(fun(Node, MergedCount, Acc) ->
        Acc orelse maps:get(Node, ReplicaMap, 0) < MergedCount
    end, false, MergedMap).

send_repair(Node, Partition, Key, lww, BestEntry) ->
    SelfNode = vordb_ring_manager:self_node(),
    NodeAtom = binary_to_atom(Node),
    erpc:cast(NodeAtom, fun() ->
        case vordb_registry:lookup({kv_store, Partition}) of
            {ok, Pid} -> gen_server:cast(Pid, {lww_sync, #{remote_lww_store => #{Key => BestEntry}}});
            _ -> ok
        end
    end);
send_repair(Node, Partition, Key, orswot, BestState) ->
    NodeAtom = binary_to_atom(Node),
    erpc:cast(NodeAtom, fun() ->
        case vordb_registry:lookup({kv_store, Partition}) of
            {ok, Pid} -> gen_server:cast(Pid, {set_sync, #{remote_set_store => #{Key => BestState}}});
            _ -> ok
        end
    end);
send_repair(Node, Partition, Key, pn_counter, BestState) ->
    NodeAtom = binary_to_atom(Node),
    erpc:cast(NodeAtom, fun() ->
        case vordb_registry:lookup({kv_store, Partition}) of
            {ok, Pid} -> gen_server:cast(Pid, {counter_sync, #{remote_counter_store => #{Key => BestState}}});
            _ -> ok
        end
    end).

%% ===== Local/remote reads =====

local_read(Partition, lww, Key) ->
    vordb_cache:get_lww(Partition, Key);
local_read(Partition, orswot, Key) ->
    vordb_cache:get_set(Partition, Key);
local_read(Partition, pn_counter, Key) ->
    vordb_cache:get_counter(Partition, Key).

remote_read(Node, Partition, CrdtType, Key) ->
    NodeAtom = binary_to_atom(Node),
    TypeAtom = case CrdtType of lww -> lww; orswot -> set; pn_counter -> counter end,
    erpc:call(NodeAtom, vordb_cache, cache_get, [Partition, TypeAtom, Key], ?READ_TIMEOUT).
