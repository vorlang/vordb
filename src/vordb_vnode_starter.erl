-module(vordb_vnode_starter).
%% Starts a KvStore Vor agent and registers it in vordb_registry.
-export([start_link/3]).

start_link(NodeId, VnodeId, SyncIntervalMs) ->
    case gen_server:start_link('Elixir.Vor.Agent.KvStore',
            [{node_id, NodeId}, {vnode_id, VnodeId}, {sync_interval_ms, SyncIntervalMs}], []) of
        {ok, Pid} ->
            vordb_registry:register({kv_store, VnodeId}, Pid),
            {ok, Pid};
        Error -> Error
    end.
