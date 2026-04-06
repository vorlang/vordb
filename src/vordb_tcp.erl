-module(vordb_tcp).
-export([start_link/1, stop/0]).

%% Binary TCP server — length-prefixed protobuf protocol.
%% Uses gen_tcp with {packet, 4} for automatic framing.

-define(MAX_MSG_SIZE, 16 * 1024 * 1024).

start_link(Port) ->
    Pid = spawn_link(fun() -> listen(Port) end),
    register(vordb_tcp, Pid),
    {ok, Pid}.

stop() ->
    case whereis(vordb_tcp) of
        undefined -> ok;
        Pid -> exit(Pid, normal)
    end.

listen(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [
        binary,
        {active, false},
        {reuseaddr, true},
        {packet, 4},
        {keepalive, true}
    ]),
    accept_loop(LSock).

accept_loop(LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            spawn(fun() -> client_loop(Sock) end),
            accept_loop(LSock);
        {error, closed} -> ok;
        {error, _} -> accept_loop(LSock)
    end.

client_loop(Sock) ->
    case gen_tcp:recv(Sock, 0, 30000) of
        {ok, Data} ->
            Response = handle_request(Data),
            gen_tcp:send(Sock, Response),
            client_loop(Sock);
        {error, closed} -> ok;
        {error, timeout} -> client_loop(Sock);
        {error, _} -> gen_tcp:close(Sock)
    end.

handle_request(Data) ->
    try
        Request = vordb_pb:decode_msg(Data, 'Request'),
        Result = dispatch(Request),
        vordb_pb:encode_msg(Result, 'Response')
    catch _:_ ->
        ErrResp = #{request_id => 0,
                    body => {error, #{message => <<"decode_error">>, code => <<"INVALID_REQUEST">>}}},
        vordb_pb:encode_msg(ErrResp, 'Response')
    end.

dispatch(#{request_id := ReqId, body := {put, #{key := Key, value := Value}}}) ->
    Msg = {put, #{key => Key, value => Value}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, {ok, #{timestamp := Ts}}} ->
            #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
        _ ->
            #{request_id => ReqId, body => {error, #{message => <<"write_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {get, #{key := Key}}}) ->
    Msg = {get, #{key => Key}},
    case vordb_coordinator:read(Key, Msg) of
        {ok, {value, #{found := true, val := Val}}} ->
            #{request_id => ReqId, body => {value, #{key => Key, value => Val}}};
        {ok, {value, #{found := false}}} ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}};
        _ ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}}
    end;

dispatch(#{request_id := ReqId, body := {delete, #{key := Key}}}) ->
    Msg = {delete, #{key => Key}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, {deleted, #{timestamp := Ts}}} ->
            #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
        _ ->
            #{request_id => ReqId, body => {error, #{message => <<"delete_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {set_add, #{key := Key, element := Element}}}) ->
    Msg = {set_add, #{key => Key, element => Element}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
        _ -> #{request_id => ReqId, body => {error, #{message => <<"set_add_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {set_remove, #{key := Key, element := Element}}}) ->
    Msg = {set_remove, #{key => Key, element => Element}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
        _ -> #{request_id => ReqId, body => {error, #{message => <<"set_remove_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {set_members, #{key := Key}}}) ->
    Msg = {set_members, #{key => Key}},
    case vordb_coordinator:read(Key, Msg) of
        {ok, {set_members, #{members := Members}}} ->
            #{request_id => ReqId, body => {set_members, #{key => Key, members => Members}}};
        {ok, {set_not_found, _}} ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}};
        _ ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}}
    end;

dispatch(#{request_id := ReqId, body := {counter_increment, #{key := Key, amount := Amount}}}) ->
    Msg = {counter_increment, #{key => Key, amount => Amount}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
        _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_inc_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {counter_decrement, #{key := Key, amount := Amount}}}) ->
    Msg = {counter_decrement, #{key => Key, amount => Amount}},
    case vordb_coordinator:write(Key, Msg) of
        {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
        _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_dec_failed">>, code => <<"INTERNAL">>}}}
    end;

dispatch(#{request_id := ReqId, body := {counter_value, #{key := Key}}}) ->
    Msg = {counter_value, #{key => Key}},
    case vordb_coordinator:read(Key, Msg) of
        {ok, {counter_value, #{val := Val}}} ->
            #{request_id => ReqId, body => {counter_value, #{key => Key, value => Val}}};
        {ok, {counter_not_found, _}} ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}};
        _ ->
            #{request_id => ReqId, body => {not_found, #{key => Key}}}
    end;

dispatch(#{request_id := ReqId}) ->
    #{request_id => ReqId, body => {error, #{message => <<"unknown_request">>, code => <<"BAD_REQUEST">>}}}.
