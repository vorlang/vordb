-module(vordb_app).
-behaviour(application).
-behaviour(supervisor).

-export([start/2, stop/1, init/1]).

start(_Type, _Args) ->
    supervisor:start_link({local, vordb_sup}, ?MODULE, []).

stop(_State) -> ok.

init([]) ->
    %% Initialize ETS registry before vnodes start
    vordb_registry:start(),

    NodeId = application:get_env(vordb, node_id, node1),
    DataDir = application:get_env(vordb, data_dir, "data/node1"),
    HttpPort = application:get_env(vordb, http_port, 4001),
    SyncInterval = application:get_env(vordb, sync_interval_ms, 1000),
    Peers = application:get_env(vordb, peers, []),
    NumVnodes = application:get_env(vordb, num_vnodes, 16),

    Children = [
        %% Storage — RocksDB gen_server
        #{id => vordb_storage,
          start => {vordb_ffi, storage_start, [list_to_binary(DataDir)]},
          restart => permanent, type => worker},

        %% Membership
        #{id => vordb_membership,
          start => {vordb_membership, start_link, [[]]},
          restart => permanent, type => worker},

        %% DirtyTracker
        #{id => vordb_dirty_tracker,
          start => {vordb_dirty_tracker, start_link, [[{peers, Peers}, {num_vnodes, NumVnodes}]]},
          restart => permanent, type => worker},

        %% Note: vordb_registry:start() must be called before vnodes start.
        %% It's called in init below.

        %% Vnode Supervisor
        #{id => vordb_vnode_sup,
          start => {vordb_vnode_sup, start_link, [[{node_id, NodeId}, {num_vnodes, NumVnodes}, {sync_interval_ms, SyncInterval}]]},
          restart => permanent, type => supervisor},

        %% Gossip — minimal gen_server (full sync on demand only)
        #{id => vordb_gossip,
          start => {'vordb@gossip', start_link_ffi, []},
          restart => permanent, type => worker}

        %% HTTP — started via Gleam mist, not in this supervisor
    ],

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
