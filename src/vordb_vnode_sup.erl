-module(vordb_vnode_sup).
-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).

init(Opts) ->
    NodeId = proplists:get_value(node_id, Opts, node1),
    NumVnodes = proplists:get_value(num_vnodes, Opts, 16),
    SyncInterval = proplists:get_value(sync_interval_ms, Opts, 1000),

    Children = [begin
        #{
            id => {kv_store, V},
            start => {vordb_vnode_starter, start_link, [NodeId, V, SyncInterval]},
            restart => permanent,
            type => worker
        }
    end || V <- lists:seq(0, NumVnodes - 1)],

    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
