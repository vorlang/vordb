-module(vordb_ring_gossip).
-behaviour(gen_server).

%% Ring gossip — periodically sends ring to random peers.
%% Also broadcasts ring immediately on membership changes.

-export([start_link/1, stop/0, broadcast_ring/0, handle_incoming/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_INTERVAL, 5000).
-define(GOSSIP_FANOUT, 3).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

stop() -> gen_server:stop(?MODULE).

%% Immediately broadcast current ring to ALL peers
broadcast_ring() ->
    gen_server:cast(?MODULE, broadcast_ring).

%% Handle incoming ring from a peer (called via erpc on receiving node)
handle_incoming(RingBin) ->
    gen_server:cast(?MODULE, {ring_received, RingBin}).

%% gen_server

init(Opts) ->
    Interval = proplists:get_value(ring_gossip_interval_ms, Opts, ?DEFAULT_INTERVAL),
    erlang:send_after(Interval, self(), ring_gossip_tick),
    {ok, #{interval => Interval}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(broadcast_ring, #{interval := _} = State) ->
    Ring = vordb_ring_manager:get_ring(),
    RingBin = 'vordb@ring':to_binary(Ring),
    Peers = nodes(),
    lists:foreach(fun(Peer) ->
        try erpc:cast(Peer, fun() ->
            vordb_ring_gossip:handle_incoming(RingBin)
        end) catch _:_ -> ok end
    end, Peers),
    {noreply, State};

handle_cast({ring_received, RingBin}, State) ->
    case 'vordb@ring':from_binary(RingBin) of
        {ok, ReceivedRing} ->
            CurrentRing = vordb_ring_manager:get_ring(),
            ReceivedVersion = element(6, ReceivedRing),  %% Ring.version field
            CurrentVersion = element(6, CurrentRing),
            case ReceivedVersion > CurrentVersion of
                true ->
                    apply_ring_update(CurrentRing, ReceivedRing);
                false ->
                    ok
            end;
        _ -> ok
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ring_gossip_tick, #{interval := Interval} = State) ->
    %% Send ring to random subset of peers
    Ring = vordb_ring_manager:get_ring(),
    RingBin = 'vordb@ring':to_binary(Ring),
    Peers = nodes(),
    Targets = pick_random(Peers, min(?GOSSIP_FANOUT, length(Peers))),
    lists:foreach(fun(Peer) ->
        try erpc:cast(Peer, fun() ->
            vordb_ring_gossip:handle_incoming(RingBin)
        end) catch _:_ -> ok end
    end, Targets),
    erlang:send_after(Interval, self(), ring_gossip_tick),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

%% Ring change cascade
apply_ring_update(OldRing, NewRing) ->
    SelfNode = vordb_ring_manager:self_node(),
    _Transfers = vordb_ring_manager:update_ring(NewRing),

    OldParts = 'vordb@ring':node_all_partitions(OldRing, SelfNode),
    NewParts = 'vordb@ring':node_all_partitions(NewRing, SelfNode),

    Gained = NewParts -- OldParts,
    Lost = OldParts -- NewParts,

    %% Start vnodes for newly gained partitions
    NodeId = vordb_ring_manager:self_node(),
    lists:foreach(fun(P) ->
        catch vordb_vnode_sup:start_vnode(NodeId, P)
    end, Gained),

    %% Initiate handoff for lost partitions
    lists:foreach(fun(P) ->
        case 'vordb@ring':partition_owner(NewRing, P) of
            {ok, NewOwner} ->
                catch vordb_handoff:initiate(P, NewOwner);
            _ -> ok
        end
    end, Lost),

    %% Update dirty tracker
    lists:foreach(fun(P) ->
        PrefList = 'vordb@ring':preference_list(NewRing, P),
        Peers = [N || N <- PrefList, N =/= SelfNode],
        vordb_dirty_tracker:update_partition_peers(P, Peers)
    end, NewParts),

    lists:foreach(fun(P) ->
        vordb_dirty_tracker:remove_partition(P)
    end, Lost),

    ok.

%% Pick N random elements from a list
pick_random([], _N) -> [];
pick_random(List, N) when N >= length(List) -> List;
pick_random(List, N) ->
    Shuffled = [X || {_, X} <- lists:sort([{rand:uniform(), E} || E <- List])],
    lists:sublist(Shuffled, N).
