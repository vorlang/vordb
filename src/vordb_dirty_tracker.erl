-module(vordb_dirty_tracker).
-behaviour(gen_server).

%% Public API
-export([start_link/1, stop/0,
         mark_dirty/3, mark_dirty_keys/3,
         take_deltas/2, confirm_ack/3,
         add_peer/1, remove_peer/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2]).

-define(ACK_TIMEOUT_MS, 5000).

%% ===== Public API =====

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

stop() -> gen_server:stop(?MODULE).

mark_dirty(VnodeIndex, Type, Key) ->
    gen_server:cast(?MODULE, {mark_dirty, VnodeIndex, Type, Key}).

mark_dirty_keys(VnodeIndex, Type, Keys) when is_map(Keys) ->
    gen_server:cast(?MODULE, {mark_dirty_keys, VnodeIndex, Type, maps:keys(Keys)});
mark_dirty_keys(VnodeIndex, Type, Keys) when is_list(Keys) ->
    gen_server:cast(?MODULE, {mark_dirty_keys, VnodeIndex, Type, Keys}).

take_deltas(VnodeIndex, Peer) ->
    gen_server:call(?MODULE, {take_deltas, VnodeIndex, Peer}).

confirm_ack(VnodeIndex, Peer, Seq) ->
    gen_server:cast(?MODULE, {confirm_ack, VnodeIndex, Peer, Seq}).

add_peer(Peer) -> gen_server:cast(?MODULE, {add_peer, Peer}).
remove_peer(Peer) -> gen_server:cast(?MODULE, {remove_peer, Peer}).

%% ===== gen_server callbacks =====

init(Opts) ->
    Peers = proplists:get_value(peers, Opts, []),
    NumVnodes = proplists:get_value(num_vnodes, Opts,
        application:get_env(vordb, num_vnodes, 16)),
    Trackers = maps:from_list([
        {{V, P}, empty_tracker()}
        || V <- lists:seq(0, NumVnodes - 1), P <- Peers
    ]),
    {ok, #{trackers => Trackers, peers => Peers, num_vnodes => NumVnodes}}.

handle_cast({mark_dirty, VnodeIndex, Type, Key}, #{trackers := T, peers := Peers} = State) ->
    T2 = lists:foldl(fun(Peer, Acc) ->
        CK = {VnodeIndex, Peer},
        case maps:get(CK, Acc, undefined) of
            undefined -> Acc;
            Tracker -> maps:put(CK, update_dirty(Tracker, Type, fun(S) -> sets:add_element(Key, S) end), Acc)
        end
    end, T, Peers),
    {noreply, State#{trackers := T2}};

handle_cast({mark_dirty_keys, _VnodeIndex, _Type, []}, State) ->
    {noreply, State};

handle_cast({mark_dirty_keys, VnodeIndex, Type, Keys}, #{trackers := T, peers := Peers} = State) ->
    KeySet = sets:from_list(Keys),
    T2 = lists:foldl(fun(Peer, Acc) ->
        CK = {VnodeIndex, Peer},
        case maps:get(CK, Acc, undefined) of
            undefined -> Acc;
            Tracker -> maps:put(CK, update_dirty(Tracker, Type, fun(S) -> sets:union(S, KeySet) end), Acc)
        end
    end, T, Peers),
    {noreply, State#{trackers := T2}};

handle_cast({confirm_ack, VnodeIndex, Peer, Seq}, #{trackers := T} = State) ->
    CK = {VnodeIndex, Peer},
    case maps:get(CK, T, undefined) of
        undefined -> {noreply, State};
        Tracker ->
            Pending = maps:get(pending, Tracker),
            T2 = maps:put(CK, Tracker#{pending := maps:remove(Seq, Pending)}, T),
            {noreply, State#{trackers := T2}}
    end;

handle_cast({add_peer, Peer}, #{trackers := T, peers := Peers, num_vnodes := NV} = State) ->
    NewPeers = lists:usort([Peer | Peers]),
    NewEntries = maps:from_list([{{V, Peer}, empty_tracker()} || V <- lists:seq(0, NV - 1)]),
    T2 = maps:merge(NewEntries, T),
    {noreply, State#{trackers := T2, peers := NewPeers}};

handle_cast({remove_peer, Peer}, #{trackers := T, peers := Peers} = State) ->
    NewPeers = lists:delete(Peer, Peers),
    T2 = maps:filter(fun({_V, P}, _) -> P =/= Peer end, T),
    {noreply, State#{trackers := T2, peers := NewPeers}}.

handle_call({take_deltas, VnodeIndex, Peer}, _From, #{trackers := T} = State) ->
    CK = {VnodeIndex, Peer},
    case maps:get(CK, T, undefined) of
        undefined ->
            {reply, {0, #{lww => [], set => [], counter => []}}, State};
        Tracker ->
            Now = erlang:system_time(millisecond),
            {Tracker2, ExpiredKeys} = expire_pending(Tracker, Now),

            %% Re-add expired keys to dirty
            Tracker3 = lists:foldl(fun({Ty, KS}, Acc) ->
                update_dirty(Acc, Ty, fun(S) -> sets:union(S, KS) end)
            end, Tracker2, ExpiredKeys),

            Dirty = maps:get(dirty, Tracker3),
            Result = #{
                lww => sets:to_list(maps:get(lww, Dirty)),
                set => sets:to_list(maps:get(set, Dirty)),
                counter => sets:to_list(maps:get(counter, Dirty))
            },

            HasData = maps:get(lww, Result) =/= [] orelse
                      maps:get(set, Result) =/= [] orelse
                      maps:get(counter, Result) =/= [],

            case HasData of
                true ->
                    Seq = maps:get(next_seq, Tracker3),
                    PendingEntry = {Dirty, Now},
                    NewTracker = Tracker3#{
                        dirty := #{lww => sets:new(), set => sets:new(), counter => sets:new()},
                        pending := maps:put(Seq, PendingEntry, maps:get(pending, Tracker3)),
                        next_seq := Seq + 1
                    },
                    T2 = maps:put(CK, NewTracker, T),
                    {reply, {Seq, Result}, State#{trackers := T2}};
                false ->
                    T2 = maps:put(CK, Tracker3, T),
                    {reply, {0, Result}, State#{trackers := T2}}
            end
    end.

%% ===== Private =====

empty_tracker() ->
    #{
        dirty => #{lww => sets:new(), set => sets:new(), counter => sets:new()},
        pending => #{},
        next_seq => 1
    }.

update_dirty(Tracker, Type, Fun) ->
    Dirty = maps:get(dirty, Tracker),
    Tracker#{dirty := maps:update_with(Type, Fun, Dirty)}.

expire_pending(Tracker, Now) ->
    Pending = maps:get(pending, Tracker),
    {Expired, Remaining} = maps:fold(fun(Seq, {_Keys, SentAt} = V, {ExpAcc, RemAcc}) ->
        case Now - SentAt > ?ACK_TIMEOUT_MS of
            true -> {[{Seq, V} | ExpAcc], RemAcc};
            false -> {ExpAcc, maps:put(Seq, V, RemAcc)}
        end
    end, {[], #{}}, Pending),
    ExpiredKeys = lists:flatmap(fun({_Seq, {KeysByType, _SentAt}}) ->
        [{Ty, KS} || {Ty, KS} <- maps:to_list(KeysByType)]
    end, Expired),
    {Tracker#{pending := Remaining}, ExpiredKeys}.
