-module(vordb_ring_manager).
-behaviour(gen_server).

%% Ring manager — holds the current ring, provides queries.
%% Started early in supervision tree. All routing/gossip/vnode modules query it.

-export([start_link/4, get_ring/0, my_partitions/0, is_local/1,
         key_partition/1, key_nodes/1, update_ring/1, self_node/0]).
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

init({RingSize, NVal, Nodes, SelfNode}) ->
    Ring = 'vordb@ring':new(RingSize, NVal, Nodes),
    MyParts = 'vordb@ring':node_all_partitions(Ring, SelfNode),
    {ok, #{ring => Ring, self_node => SelfNode, my_partitions => MyParts}}.

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

handle_call({update_ring, NewRing}, _From, #{ring := OldRing, self_node := SelfNode} = State) ->
    Diff = 'vordb@ring':diff(OldRing, NewRing),
    NewParts = 'vordb@ring':node_all_partitions(NewRing, SelfNode),
    {reply, Diff, State#{ring := NewRing, my_partitions := NewParts}}.

handle_cast(_Msg, State) ->
    {noreply, State}.
