-module(vordb_registry).
%% Simple ETS-based process registry. Replaces Elixir.Registry.
-export([start/0, register/2, lookup/1, unregister/1]).

-define(TABLE, vordb_vnode_registry).

start() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ets:new(?TABLE, [named_table, public, set]),
            ok;
        _ -> ok
    end.

register(Key, Pid) ->
    ets:insert(?TABLE, {Key, Pid}),
    ok.

lookup(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{_, Pid}] -> {ok, Pid};
        [] -> {error, not_found}
    end.

unregister(Key) ->
    ets:delete(?TABLE, Key),
    ok.
