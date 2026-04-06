-module(vordb_dirty_tracker).

%% ETS-based dirty tracker — no GenServer, no serialization bottleneck.
%% Same public API as the old GenServer version.

-export([init/0, start_link/1, stop/0,
         mark_dirty/3, mark_dirty_keys/3,
         take_deltas/2, confirm_ack/3,
         expire_pending/1,
         init_partition/2, remove_partition/1, update_partition_peers/2,
         partition_peers/1,
         %% Legacy API compatibility
         add_peer/1, remove_peer/1]).

-define(ACK_TIMEOUT_MS, 5000).

%% ===== Initialization =====

init() ->
    ensure_table(vordb_dirty, [named_table, bag, public,
        {write_concurrency, true}, {read_concurrency, true}]),
    ensure_table(vordb_dirty_peers, [named_table, set, public,
        {read_concurrency, true}]),
    ensure_table(vordb_dirty_pending, [named_table, set, public]),
    ensure_table(vordb_dirty_seq, [named_table, set, public]),
    ok.

%% start_link for backward compatibility with supervisors — just init tables
start_link(_Opts) ->
    init(),
    %% Return a dummy pid (self) — no actual process needed
    {ok, self()}.

stop() ->
    %% Clear all ETS tables (for test cleanup)
    catch ets:delete_all_objects(vordb_dirty),
    catch ets:delete_all_objects(vordb_dirty_peers),
    catch ets:delete_all_objects(vordb_dirty_pending),
    catch ets:delete_all_objects(vordb_dirty_seq),
    ok.

ensure_table(Name, Opts) ->
    case ets:whereis(Name) of
        undefined -> ets:new(Name, Opts);
        _ -> ok
    end.

%% ===== Mark Dirty =====

mark_dirty(Partition, Type, Key) ->
    case ets:lookup(vordb_dirty_peers, Partition) of
        [{_, Peers}] ->
            Entries = [{{Partition, Peer, Type}, Key} || Peer <- Peers],
            ets:insert(vordb_dirty, Entries);
        [] -> ok
    end,
    ok.

mark_dirty_keys(Partition, Type, Keys) when is_map(Keys) ->
    mark_dirty_keys(Partition, Type, maps:keys(Keys));
mark_dirty_keys(Partition, Type, Keys) when is_list(Keys) ->
    case ets:lookup(vordb_dirty_peers, Partition) of
        [{_, Peers}] ->
            Entries = [{{Partition, Peer, Type}, Key}
                       || Peer <- Peers, Key <- Keys],
            ets:insert(vordb_dirty, Entries);
        [] -> ok
    end,
    ok.

%% ===== Take Deltas =====

take_deltas(Partition, Peer) ->
    LwwKeys = take_type_keys(Partition, Peer, lww),
    SetKeys = take_type_keys(Partition, Peer, set),
    CounterKeys = take_type_keys(Partition, Peer, counter),

    Result = #{lww => LwwKeys, set => SetKeys, counter => CounterKeys},

    case {LwwKeys, SetKeys, CounterKeys} of
        {[], [], []} ->
            {0, Result};
        _ ->
            %% Assign sequence number (atomic increment)
            Seq = ets:update_counter(vordb_dirty_seq,
                {Partition, Peer}, {2, 1}, {{Partition, Peer}, 0}),
            %% Record pending
            Now = erlang:system_time(millisecond),
            ets:insert(vordb_dirty_pending,
                {{Partition, Peer, Seq}, {Result, Now}}),
            {Seq, Result}
    end.

take_type_keys(Partition, Peer, Type) ->
    Entries = ets:take(vordb_dirty, {Partition, Peer, Type}),
    lists:usort([Key || {_, Key} <- Entries]).

%% ===== ACK =====

confirm_ack(Partition, Peer, Seq) ->
    ets:delete(vordb_dirty_pending, {Partition, Peer, Seq}),
    ok.

%% ===== Pending Expiry =====

expire_pending(TimeoutMs) ->
    Now = erlang:system_time(millisecond),
    Cutoff = Now - TimeoutMs,
    %% Select expired entries
    Expired = ets:select(vordb_dirty_pending, [
        {{'$1', {'$2', '$3'}},
         [{'<', '$3', Cutoff}],
         [{{'$1', '$2'}}]}
    ]),
    lists:foreach(fun({{Partition, Peer, Seq}, {KeysByType, _}}) ->
        %% Re-insert keys as dirty
        maps:foreach(fun(Type, Keys) ->
            Entries = [{{Partition, Peer, Type}, Key} || Key <- Keys],
            ets:insert(vordb_dirty, Entries)
        end, KeysByType),
        ets:delete(vordb_dirty_pending, {Partition, Peer, Seq})
    end, Expired),
    length(Expired).

%% ===== Partition Lifecycle =====

init_partition(Partition, Peers) ->
    ets:insert(vordb_dirty_peers, {Partition, Peers}),
    lists:foreach(fun(Peer) ->
        ets:insert(vordb_dirty_seq, {{Partition, Peer}, 0})
    end, Peers),
    ok.

remove_partition(Partition) ->
    ets:delete(vordb_dirty_peers, Partition),
    ets:match_delete(vordb_dirty, {{Partition, '_', '_'}, '_'}),
    ets:match_delete(vordb_dirty_pending, {{Partition, '_', '_'}, '_'}),
    ets:match_delete(vordb_dirty_seq, {{Partition, '_'}, '_'}),
    ok.

update_partition_peers(Partition, NewPeers) ->
    OldPeers = case ets:lookup(vordb_dirty_peers, Partition) of
        [{_, P}] -> P;
        [] -> []
    end,
    ets:insert(vordb_dirty_peers, {Partition, NewPeers}),
    %% Remove departed peers
    Departed = OldPeers -- NewPeers,
    lists:foreach(fun(Peer) ->
        ets:match_delete(vordb_dirty, {{Partition, Peer, '_'}, '_'}),
        ets:match_delete(vordb_dirty_pending, {{Partition, Peer, '_'}, '_'}),
        ets:delete(vordb_dirty_seq, {Partition, Peer})
    end, Departed),
    %% Init new peers
    Added = NewPeers -- OldPeers,
    lists:foreach(fun(Peer) ->
        ets:insert(vordb_dirty_seq, {{Partition, Peer}, 0})
    end, Added),
    ok.

partition_peers(Partition) ->
    case ets:lookup(vordb_dirty_peers, Partition) of
        [{_, Peers}] -> Peers;
        [] -> []
    end.

%% ===== Legacy API (backward compat) =====

add_peer(_Peer) -> ok.
remove_peer(_Peer) -> ok.
