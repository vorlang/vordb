-module(vordb_handoff).
-behaviour(gen_server).

%% Handoff manager — orchestrates partition data transfers between nodes.
%% Source pushes data chunks to target. Target merges via existing sync handlers.

-export([start_link/0, stop/0, initiate/2, status/0, cancel/1,
         handle_handoff_start/3, handle_handoff_chunk/3, handle_handoff_complete/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(CHUNK_SIZE, 100).
-define(HANDOFF_TIMEOUT, 30000).
-define(SEND_DELAY, 10).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() -> gen_server:stop(?MODULE).

initiate(Partition, TargetNode) ->
    gen_server:call(?MODULE, {initiate, Partition, TargetNode}).

status() ->
    gen_server:call(?MODULE, status).

cancel(Partition) ->
    gen_server:call(?MODULE, {cancel, Partition}).

%% Incoming handoff messages (called on TARGET node)
handle_handoff_start(Partition, FromNode, _TotalKeys) ->
    gen_server:cast(?MODULE, {handoff_start, Partition, FromNode}).

handle_handoff_chunk(Partition, _FromNode, ChunkData) ->
    %% Convert chunk to sync messages and cast to local vnode
    case maps:get(lww, ChunkData, #{}) of
        M when map_size(M) > 0 ->
            case vordb_registry:lookup({kv_store, Partition}) of
                {ok, Pid} -> gen_server:cast(Pid, {lww_sync, #{remote_lww_store => M}});
                _ -> ok
            end;
        _ -> ok
    end,
    case maps:get(sets, ChunkData, #{}) of
        M when map_size(M) > 0 ->
            case vordb_registry:lookup({kv_store, Partition}) of
                {ok, Pid} -> gen_server:cast(Pid, {set_sync, #{remote_set_store => M}});
                _ -> ok
            end;
        _ -> ok
    end,
    case maps:get(counters, ChunkData, #{}) of
        M when map_size(M) > 0 ->
            case vordb_registry:lookup({kv_store, Partition}) of
                {ok, Pid} -> gen_server:cast(Pid, {counter_sync, #{remote_counter_store => M}});
                _ -> ok
            end;
        _ -> ok
    end.

handle_handoff_complete(Partition, FromNode) ->
    %% Send ACK back to source
    try
        erpc:cast(binary_to_atom(FromNode), fun() ->
            gen_server:cast(?MODULE, {handoff_ack, Partition, vordb_ring_manager:self_node(), ok})
        end)
    catch _:_ -> ok
    end.

%% gen_server

init([]) ->
    {ok, #{active => #{}, completed => []}}.

handle_call({initiate, Partition, TargetNode}, _From, #{active := Active} = State) ->
    case maps:is_key(Partition, Active) of
        true ->
            {reply, {error, already_in_progress}, State};
        false ->
            case vordb_ring_manager:is_local(Partition) of
                false ->
                    {reply, {error, partition_not_local}, State};
                true ->
                    %% Spawn sender process
                    SelfNode = vordb_ring_manager:self_node(),
                    Self = self(),
                    Pid = spawn_link(fun() ->
                        do_handoff_send(Partition, TargetNode, SelfNode, Self)
                    end),
                    Record = #{partition => Partition, target => TargetNode,
                               sender_pid => Pid, state => streaming,
                               started_at => erlang:system_time(millisecond)},
                    {reply, ok, State#{active := maps:put(Partition, Record, Active)}}
            end
    end;

handle_call(status, _From, #{active := Active} = State) ->
    Records = maps:values(Active),
    {reply, Records, State};

handle_call({cancel, Partition}, _From, #{active := Active} = State) ->
    case maps:get(Partition, Active, undefined) of
        undefined -> {reply, {error, not_found}, State};
        #{sender_pid := Pid} ->
            exit(Pid, cancel),
            {reply, ok, State#{active := maps:remove(Partition, Active)}}
    end.

handle_cast({handoff_start, _Partition, _FromNode}, State) ->
    %% Target side — vnode should already be started
    {noreply, State};

handle_cast({handoff_ack, Partition, _FromNode, ok}, #{active := Active} = State) ->
    case maps:get(Partition, Active, undefined) of
        undefined -> {noreply, State};
        _Record ->
            %% Handoff complete — cleanup source partition
            catch vordb_vnode_sup:stop_vnode(Partition),
            catch vordb_ffi:storage_delete_partition(Partition),
            catch vordb_dirty_tracker:remove_partition(Partition),
            {noreply, State#{active := maps:remove(Partition, Active)}}
    end;

handle_cast({handoff_ack, Partition, _FromNode, {error, _Reason}}, #{active := Active} = State) ->
    %% Handoff failed — mark for retry
    case maps:get(Partition, Active, undefined) of
        undefined -> {noreply, State};
        Record ->
            NewRecord = Record#{state := failed},
            {noreply, State#{active := maps:put(Partition, NewRecord, Active)}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Sender process crashed — handoff will be retried on next ring change
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

%% Sender process (runs in spawned process)

do_handoff_send(Partition, TargetNode, SelfNode, Manager) ->
    TargetAtom = binary_to_atom(TargetNode),

    %% Signal target to prepare
    TotalKeys = vordb_ffi:storage_count_partition_keys(Partition),
    try erpc:cast(TargetAtom, fun() ->
        vordb_handoff:handle_handoff_start(Partition, SelfNode, TotalKeys)
    end) catch _:_ -> ok end,

    %% Stream data in chunks
    vordb_ffi:storage_iterate_partition(Partition, ?CHUNK_SIZE, fun(ChunkData) ->
        try erpc:cast(TargetAtom, fun() ->
            vordb_handoff:handle_handoff_chunk(Partition, SelfNode, ChunkData)
        end) catch _:_ -> ok end,
        timer:sleep(?SEND_DELAY)
    end),

    %% Signal completion
    try erpc:cast(TargetAtom, fun() ->
        vordb_handoff:handle_handoff_complete(Partition, SelfNode)
    end) catch _:_ -> ok end.
