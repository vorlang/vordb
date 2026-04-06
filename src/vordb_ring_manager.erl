-module(vordb_ring_manager).
-behaviour(gen_server).

%% Ring manager — holds the current ring, provides queries.
%% Started early in supervision tree. All routing/gossip/vnode modules query it.

-export([start_link/4, get_ring/0, my_partitions/0, is_local/1,
         key_partition/1, key_nodes/1, update_ring/1, self_node/0,
         persist_ring/0, load_persisted_ring/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link(RingSize, NVal, Nodes, SelfNode) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
        {RingSize, NVal, Nodes, SelfNode}, []).

get_ring() -> gen_server:call(?MODULE, get_ring).
my_partitions() -> gen_server:call(?MODULE, my_partitions).
is_local(Partition) -> gen_server:call(?MODULE, {is_local, Partition}).
key_partition(Key) -> gen_server:call(?MODULE, {key_partition, Key}).
key_nodes(Key) -> gen_server:call(?MODULE, {key_nodes, Key}).
update_ring(NewRing) -> gen_server:call(?MODULE, {update_ring, NewRing}).
self_node() -> gen_server:call(?MODULE, self_node).

%% gen_server

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

init({RingSize, NVal, Nodes, SelfNode}) ->
    DataDir = application:get_env(vordb, data_dir, "data/node1"),
    RingPath = filename:join(DataDir, "ring.dat"),

    Ring = case load_persisted_ring(RingPath) of
        {ok, PersistedRing} -> PersistedRing;
        _ -> 'vordb@ring':new(RingSize, NVal, Nodes)
    end,
    MyParts = 'vordb@ring':node_all_partitions(Ring, SelfNode),

    %% Persist initial ring
    catch do_persist(Ring, RingPath),

    {ok, #{ring => Ring, self_node => SelfNode, my_partitions => MyParts,
            ring_path => RingPath}}.

handle_call(get_ring, _From, #{ring := Ring} = State) ->
    {reply, Ring, State};

handle_call(my_partitions, _From, #{my_partitions := Parts} = State) ->
    {reply, Parts, State};

handle_call({is_local, Partition}, _From, #{my_partitions := Parts} = State) ->
    {reply, lists:member(Partition, Parts), State};

handle_call({key_partition, Key}, _From, #{ring := Ring} = State) ->
    {reply, 'vordb@ring':key_to_partition(Ring, Key), State};

handle_call({key_nodes, Key}, _From, #{ring := Ring} = State) ->
    {reply, 'vordb@ring':key_nodes(Ring, Key), State};

handle_call(self_node, _From, #{self_node := SelfNode} = State) ->
    {reply, SelfNode, State};

handle_call({update_ring, NewRing}, _From, #{ring := OldRing, self_node := SelfNode, ring_path := RingPath} = State) ->
    Diff = 'vordb@ring':diff(OldRing, NewRing),
    NewParts = 'vordb@ring':node_all_partitions(NewRing, SelfNode),
    catch do_persist(NewRing, RingPath),
    {reply, Diff, State#{ring := NewRing, my_partitions := NewParts}};

handle_call(persist_ring, _From, #{ring := Ring, ring_path := RingPath} = State) ->
    Result = do_persist(Ring, RingPath),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

do_persist(Ring, RingPath) ->
    Bin = 'vordb@ring':to_binary(Ring),
    filelib:ensure_dir(RingPath),
    file:write_file(RingPath, Bin).
