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

%% Each dispatch clause extracts the bucket field. If empty (proto3 default),
%% falls through to legacy direct-coordinator path. If set, routes through
%% bucket_write/bucket_read which does bucket lookup + key prefixing.

dispatch(#{request_id := ReqId, body := {put, #{key := Key, value := Value} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {put, #{key => Key, value => Value, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, {ok, #{timestamp := Ts}}} ->
                    #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
                _ ->
                    #{request_id => ReqId, body => {error, #{message => <<"write_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, put, #{key => Key, value => Value}) of
                {ok, {ok, #{timestamp := Ts}}} ->
                    #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
                {error, bucket_not_found} ->
                    #{request_id => ReqId, body => {error, #{message => <<"bucket_not_found">>, code => <<"NOT_FOUND">>}}};
                {error, {type_mismatch, _, _}} ->
                    #{request_id => ReqId, body => {error, #{message => <<"type_mismatch">>, code => <<"BAD_REQUEST">>}}};
                _ ->
                    #{request_id => ReqId, body => {error, #{message => <<"write_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {get, #{key := Key} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {get, #{key => Key}},
            case vordb_coordinator:read(Key, Msg) of
                {ok, {value, #{found := true, val := Val}}} ->
                    #{request_id => ReqId, body => {value, #{key => Key, value => Val}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_read(Bucket, #{key => Key}) of
                {ok, {value, #{found := true, val := Val}}} ->
                    #{request_id => ReqId, body => {value, #{key => Key, value => Val}}};
                {ok, {set_members, #{members := Members}}} ->
                    #{request_id => ReqId, body => {set_members, #{key => Key, members => Members}}};
                {ok, {counter_value, #{val := Val}}} ->
                    #{request_id => ReqId, body => {counter_value, #{key => Key, value => Val}}};
                {error, bucket_not_found} ->
                    #{request_id => ReqId, body => {error, #{message => <<"bucket_not_found">>, code => <<"NOT_FOUND">>}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {delete, #{key := Key} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {delete, #{key => Key, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, {deleted, #{timestamp := Ts}}} ->
                    #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
                _ ->
                    #{request_id => ReqId, body => {error, #{message => <<"delete_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, delete, #{key => Key}) of
                {ok, {deleted, #{timestamp := Ts}}} ->
                    #{request_id => ReqId, body => {ok, #{timestamp => Ts}}};
                _ ->
                    #{request_id => ReqId, body => {error, #{message => <<"delete_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {set_add, #{key := Key, element := Element} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {set_add, #{key => Key, element => Element, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"set_add_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, set_add, #{key => Key, element => Element}) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                {error, bucket_not_found} ->
                    #{request_id => ReqId, body => {error, #{message => <<"bucket_not_found">>, code => <<"NOT_FOUND">>}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"set_add_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {set_remove, #{key := Key, element := Element} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {set_remove, #{key => Key, element => Element, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"set_remove_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, set_remove, #{key => Key, element => Element}) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"set_remove_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {set_members, #{key := Key} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {set_members, #{key => Key}},
            case vordb_coordinator:read(Key, Msg) of
                {ok, {set_members, #{members := Members}}} ->
                    #{request_id => ReqId, body => {set_members, #{key => Key, members => Members}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_read(Bucket, #{key => Key}) of
                {ok, {set_members, #{members := Members}}} ->
                    #{request_id => ReqId, body => {set_members, #{key => Key, members => Members}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {counter_increment, #{key := Key, amount := Amount} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {counter_increment, #{key => Key, amount => Amount, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_inc_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, counter_increment, #{key => Key, amount => Amount}) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_inc_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {counter_decrement, #{key := Key, amount := Amount} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {counter_decrement, #{key => Key, amount => Amount, ttl_seconds => 0}},
            case vordb_coordinator:write(Key, Msg) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_dec_failed">>, code => <<"INTERNAL">>}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_write(Bucket, counter_decrement, #{key => Key, amount => Amount}) of
                {ok, _} -> #{request_id => ReqId, body => {ok, #{timestamp => 0}}};
                _ -> #{request_id => ReqId, body => {error, #{message => <<"counter_dec_failed">>, code => <<"INTERNAL">>}}}
            end
    end;

dispatch(#{request_id := ReqId, body := {counter_value, #{key := Key} = F}}) ->
    case get_bucket(F) of
        <<>> ->
            Msg = {counter_value, #{key => Key}},
            case vordb_coordinator:read(Key, Msg) of
                {ok, {counter_value, #{val := Val}}} ->
                    #{request_id => ReqId, body => {counter_value, #{key => Key, value => Val}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end;
        Bucket ->
            case vordb_coordinator:bucket_read(Bucket, #{key => Key}) of
                {ok, {counter_value, #{val := Val}}} ->
                    #{request_id => ReqId, body => {counter_value, #{key => Key, value => Val}}};
                _ ->
                    #{request_id => ReqId, body => {not_found, #{key => Key}}}
            end
    end;

dispatch(#{request_id := ReqId}) ->
    #{request_id => ReqId, body => {error, #{message => <<"unknown_request">>, code => <<"BAD_REQUEST">>}}}.

get_bucket(Fields) ->
    maps:get(bucket, Fields, <<>>).
