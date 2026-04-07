-module(vordb_ring_manager).
-behaviour(gen_server).

%% Ring manager — owns ring state.
%% Updates (membership changes) go through the gen_server.
%% Reads bypass the gen_server entirely via persistent_term — every caller
%% (coordinator, TCP dispatch, gossip, metrics, vnode_sup, ffi) executes the
%% partition / preference-list computation locally in its own process.

-export([start_link/4, get_ring/0, my_partitions/0, is_local/1,
         key_partition/1, key_nodes/1, update_ring/1, self_node/0,
         persist_ring/0, load_persisted_ring/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-define(PT_RING,  {vordb_ring_manager, ring}).
-define(PT_SELF,  {vordb_ring_manager, self_node}).
-define(PT_PARTS, {vordb_ring_manager, my_partitions}).

start_link(RingSize, NVal, Nodes, SelfNode) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
        {RingSize, NVal, Nodes, SelfNode}, []).

%% ===== Read path — persistent_term, no gen_server call =====

get_ring() -> persistent_term:get(?PT_RING).

my_partitions() -> persistent_term:get(?PT_PARTS).

self_node() -> persistent_term:get(?PT_SELF).

is_local(Partition) ->
    lists:member(Partition, persistent_term:get(?PT_PARTS)).

key_partition(Key) ->
    'vordb@ring':key_to_partition(persistent_term:get(?PT_RING), Key).

key_nodes(Key) ->
    'vordb@ring':key_nodes(persistent_term:get(?PT_RING), Key).

%% ===== Write path — gen_server call =====

update_ring(NewRing) -> gen_server:call(?MODULE, {update_ring, NewRing}).

persist_ring() -> gen_server:call(?MODULE, persist_ring).

load_persisted_ring(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case 'vordb@ring':from_binary(Bin) of
                {ok, Ring} -> {ok, Ring};
                _ -> {error, invalid}
            end;
        _ -> {error, not_found}
    end.

%% ===== gen_server callbacks =====

init({RingSize, NVal, Nodes, SelfNode}) ->
    DataDir = application:get_env(vordb, data_dir, "data/node1"),
    RingPath = filename:join(DataDir, "ring.dat"),

    Ring = case load_persisted_ring(RingPath) of
        {ok, PersistedRing} -> PersistedRing;
        _ -> 'vordb@ring':new(RingSize, NVal, Nodes)
    end,
    MyParts = 'vordb@ring':node_all_partitions(Ring, SelfNode),

    publish(Ring, SelfNode, MyParts),
    catch do_persist(Ring, RingPath),

    {ok, #{ring => Ring, self_node => SelfNode, my_partitions => MyParts,
            ring_path => RingPath}}.

handle_call({update_ring, NewRing}, _From,
            #{ring := OldRing, self_node := SelfNode, ring_path := RingPath} = State) ->
    Diff = 'vordb@ring':diff(OldRing, NewRing),
    NewParts = 'vordb@ring':node_all_partitions(NewRing, SelfNode),
    publish(NewRing, SelfNode, NewParts),
    catch do_persist(NewRing, RingPath),
    {reply, Diff, State#{ring := NewRing, my_partitions := NewParts}};

handle_call(persist_ring, _From, #{ring := Ring, ring_path := RingPath} = State) ->
    Result = do_persist(Ring, RingPath),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% ===== Internal =====

publish(Ring, SelfNode, MyParts) ->
    persistent_term:put(?PT_RING, Ring),
    persistent_term:put(?PT_SELF, SelfNode),
    persistent_term:put(?PT_PARTS, MyParts).

do_persist(Ring, RingPath) ->
    Bin = 'vordb@ring':to_binary(Ring),
    filelib:ensure_dir(RingPath),
    file:write_file(RingPath, Bin).
