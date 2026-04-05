-module(vordb_membership).
-behaviour(gen_server).

-export([start_link/1, stop/0, members/0, join/1, leave/0,
         handle_member_join/1, handle_member_leave/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

stop() -> gen_server:stop(?MODULE).

members() -> gen_server:call(?MODULE, members).

join(SeedNode) -> gen_server:call(?MODULE, {join, SeedNode}, 30000).

leave() -> gen_server:call(?MODULE, leave).

handle_member_join(NewNode) ->
    gen_server:cast(?MODULE, {member_joined, NewNode}).

handle_member_leave(LeavingNode) ->
    gen_server:cast(?MODULE, {member_left, LeavingNode}).

%% ===== gen_server =====

init(_Opts) ->
    {ok, #{members => [node()]}}.

handle_call(members, _From, #{members := Members} = State) ->
    {reply, Members, State};

handle_call({join, SeedNode}, _From, State) ->
    case net_kernel:connect_node(SeedNode) of
        true ->
            RemoteMembers = try
                erpc:call(SeedNode, ?MODULE, members, [], 10000)
            catch _:_ -> [SeedNode]
            end,
            %% Announce to all existing members
            lists:foreach(fun(M) ->
                case M =/= node() of
                    true -> erpc:cast(M, ?MODULE, handle_member_join, [node()]);
                    false -> ok
                end
            end, RemoteMembers),
            AllMembers = lists:usort([node() | RemoteMembers]),
            %% Update DirtyTracker
            lists:foreach(fun(M) ->
                case M =/= node() of
                    true -> vordb_dirty_tracker:add_peer(M);
                    false -> ok
                end
            end, AllMembers),
            {reply, {ok, AllMembers}, State#{members := AllMembers}};
        false ->
            {reply, {error, connect_failed}, State};
        ignored ->
            {reply, {error, not_distributed}, State}
    end;

handle_call(leave, _From, #{members := Members} = State) ->
    Peers = lists:delete(node(), Members),
    lists:foreach(fun(P) ->
        erpc:cast(P, ?MODULE, handle_member_leave, [node()])
    end, Peers),
    {reply, ok, State#{members := [node()]}}.

handle_cast({member_joined, NewNode}, #{members := Members} = State) ->
    case lists:member(NewNode, Members) of
        true -> {noreply, State};
        false ->
            NewMembers = lists:usort([NewNode | Members]),
            vordb_dirty_tracker:add_peer(NewNode),
            {noreply, State#{members := NewMembers}}
    end;

handle_cast({member_left, LeavingNode}, #{members := Members} = State) ->
    case lists:member(LeavingNode, Members) of
        false -> {noreply, State};
        true ->
            NewMembers = lists:delete(LeavingNode, Members),
            vordb_dirty_tracker:remove_peer(LeavingNode),
            {noreply, State#{members := NewMembers}}
    end.
