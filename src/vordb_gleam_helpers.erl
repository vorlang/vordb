-module(vordb_gleam_helpers).
-export([
    dt_start_opts/2,
    make_lww_sync_msg/1,
    to_dynamic/1,
    safe_http_handler/2
]).

%% Build DirtyTracker start_link opts proplist
dt_start_opts(Peers, NumVnodes) ->
    [{peers, Peers}, {num_vnodes, NumVnodes}].

%% Build lww_sync cast message for agent
make_lww_sync_msg(RemoteStore) ->
    {lww_sync, #{remote_lww_store => RemoteStore}}.

%% Wrap any term as dynamic (identity in Erlang)
to_dynamic(Term) -> Term.

%% Safe HTTP handler wrapper — catches and logs errors, returns 500 with details
safe_http_handler(Req, NumVnodes) ->
    try 'vordb@http_router':handler(Req, NumVnodes)
    catch C:R:S ->
        io:format("HTTP HANDLER CRASH: ~p:~p~n~p~n", [C, R, S]),
        ErrBody = iolist_to_binary(io_lib:format("{\"error\":\"~p:~p\"}", [C, R])),
        {response, 500, [{<<"content-type">>, <<"application/json">>}],
         {bytes, gleam@bytes_tree:from_string(ErrBody)}}
    end.

%% Test helper: put + get round-trip
test_kv_put_get() ->
    try
        {ok, Dir} = vordb_test_helpers:start_storage(),
        Pid = vordb_test_helpers:start_kv_store(test_node, 0),
        {ok, _} = gen_server:call(Pid, {put, #{key => <<"x">>, value => <<"hello">>}}),
        {value, #{found := Found}} = gen_server:call(Pid, {get, #{key => <<"x">>}}),
        gen_server:stop(Pid),
        vordb_ffi:storage_stop(),
        vordb_test_helpers:cleanup_dir(Dir),
        Found
    catch C:R:S ->
        io:format("test_kv_put_get FAILED: ~p:~p~n~p~n", [C, R, S]),
        false
    end.
