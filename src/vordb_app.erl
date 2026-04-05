-module(vordb_app).
-behaviour(application).
-behaviour(supervisor).

-export([start/2, stop/1, init/1]).

start(_Type, _Args) ->
    supervisor:start_link({local, vordb_sup}, ?MODULE, []).

stop(_State) -> ok.

init([]) ->
    NodeId = application:get_env(vordb, node_id, node()),
    DataDir = application:get_env(vordb, data_dir, "data/node1"),
    SyncInterval = application:get_env(vordb, sync_interval_ms, 1000),
    RingSize = application:get_env(vordb, ring_size, 256),
    NVal = application:get_env(vordb, replication_n, 3),
    SelfNode = atom_to_binary(NodeId),

    %% Compute initial membership: self + configured peers
    Peers = application:get_env(vordb, peers, []),
    AllNodes = lists:usort([SelfNode | [atom_to_binary(P) || P <- Peers]]),

    Children = [
        %% Storage
        #{id => vordb_storage,
          start => {vordb_ffi, storage_start, [list_to_binary(DataDir)]},
          restart => permanent, type => worker},

        %% RingManager — holds current ring
        #{id => vordb_ring_manager,
          start => {vordb_ring_manager, start_link, [RingSize, NVal, AllNodes, SelfNode]},
          restart => permanent, type => worker},

        %% Membership
        #{id => vordb_membership,
          start => {vordb_membership, start_link, [[]]},
          restart => permanent, type => worker},

        %% DirtyTracker — will init partitions after ring is available
        #{id => vordb_dirty_tracker,
          start => {vordb_dirty_tracker, start_link, [[{peers, []}, {num_vnodes, RingSize}]]},
          restart => permanent, type => worker},

        %% Vnode Registry
        #{id => vordb_vnode_registry,
          start => {vordb_registry, start, []},
          restart => permanent, type => worker},

        %% Vnode Supervisor — ring-driven, queries RingManager for initial partitions
        #{id => vordb_vnode_sup,
          start => {vordb_vnode_sup, start_link, [[{node_id, NodeId}, {sync_interval_ms, SyncInterval}]]},
          restart => permanent, type => supervisor},

        %% Gossip
        #{id => vordb_gossip,
          start => {gen_server, start_link, [{local, vordb_gossip}, vordb_gossip_stub, [], []]},
          restart => permanent, type => worker}
    ],

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
