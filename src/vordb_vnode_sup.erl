-module(vordb_vnode_sup).
-behaviour(supervisor).

%% Ring-driven vnode supervisor. Starts vnodes for partitions assigned
%% to this node by the ring. Supports dynamic start/stop on ring changes.

-export([start_link/1, init/1, start_vnode/2, stop_vnode/1,
         apply_ring_change/2, running_partitions/0]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).

init(Opts) ->
    NodeId = proplists:get_value(node_id, Opts, node()),
    SyncInterval = proplists:get_value(sync_interval_ms, Opts, 1000),

    %% Get initial partitions from RingManager
    MyPartitions = vordb_ring_manager:my_partitions(),

    Children = [vnode_child_spec(NodeId, P, SyncInterval) || P <- MyPartitions],

    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.

start_vnode(NodeId, Partition) ->
    SyncInterval = application:get_env(vordb, sync_interval_ms, 1000),
    ChildSpec = vnode_child_spec(NodeId, Partition, SyncInterval),
    supervisor:start_child(?MODULE, ChildSpec).

stop_vnode(Partition) ->
    ChildId = {kv_store, Partition},
    supervisor:terminate_child(?MODULE, ChildId),
    supervisor:delete_child(?MODULE, ChildId).

apply_ring_change(Transfers, SelfNode) ->
    %% Determine which partitions to start and stop
    CurrentParts = running_partitions(),
    NewMyParts = vordb_ring_manager:my_partitions(),

    %% Partitions to start: in new set but not running
    ToStart = [P || P <- NewMyParts, not lists:member(P, CurrentParts)],
    %% Partitions to stop: running but not in new set
    ToStop = [P || P <- CurrentParts, not lists:member(P, NewMyParts)],

    %% Stop departing vnodes
    lists:foreach(fun(P) ->
        catch stop_vnode(P)
    end, ToStop),

    %% Start new vnodes
    NodeId = vordb_ring_manager:self_node(),
    lists:foreach(fun(P) ->
        start_vnode(NodeId, P)
    end, ToStart),

    {started, length(ToStart), stopped, length(ToStop)}.

running_partitions() ->
    Children = supervisor:which_children(?MODULE),
    [P || {{kv_store, P}, _Pid, _Type, _Mods} <- Children].

%% Internal

vnode_child_spec(NodeId, Partition, SyncInterval) ->
    #{
        id => {kv_store, Partition},
        start => {vordb_vnode_starter, start_link, [NodeId, Partition, SyncInterval]},
        restart => permanent,
        type => worker
    }.
