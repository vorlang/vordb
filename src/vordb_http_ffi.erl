-module(vordb_http_ffi).
-export([
    parse_json_body/1,
    json_get_string/2,
    json_get_int/3,
    format_kv_get_response/2,
    format_kv_put_response/2,
    format_kv_delete_response/2,
    format_set_members_response/2,
    format_counter_value_response/2,
    is_not_found/1,
    %% Bucket support
    make_bucket_config/4,
    make_bucket_params_put/2,
    make_bucket_params_key/1,
    make_bucket_params_element/2,
    make_bucket_params_amount/2,
    format_bucket_response/1,
    format_bucket_list_response/1,
    format_bucket_error/1,
    is_bucket_error/1,
    format_bucket_read_response/2,
    make_atom/1
]).

parse_json_body(Body) when is_binary(Body) ->
    try json:decode(Body) of
        {ok, Parsed} -> Parsed;
        Parsed when is_map(Parsed) -> Parsed;
        _ -> #{}
    catch _:_ -> #{}
    end;
parse_json_body(Body) when is_list(Body) ->
    parse_json_body(list_to_binary(Body));
parse_json_body(_) -> #{}.

json_get_string(Parsed, Key) when is_map(Parsed) ->
    %% Key from Gleam is a binary string. json:decode returns binary keys.
    BinKey = ensure_binary(Key),
    case maps:get(BinKey, Parsed, undefined) of
        undefined -> {error, nil};
        V when is_binary(V) -> {ok, V};
        _ -> {error, nil}
    end;
json_get_string(_, _) -> {error, nil}.

json_get_int(Parsed, Key, Default) when is_map(Parsed) ->
    BinKey = ensure_binary(Key),
    case maps:get(BinKey, Parsed, undefined) of
        undefined -> Default;
        V when is_integer(V) -> V;
        _ -> Default
    end;
json_get_int(_, _, Default) -> Default.

%% Response formatters — decode Vor agent response tuples and produce JSON strings

format_kv_put_response(Key, {ok, #{timestamp := Ts}}) ->
    iolist_to_binary([
        <<"{\"ok\":true,\"key\":\"">>, Key, <<"\",\"timestamp\":">>,
        integer_to_binary(Ts), <<"}">>
    ]);
format_kv_put_response(Key, _) ->
    iolist_to_binary([<<"{\"ok\":true,\"key\":\"">>, Key, <<"\"}">>]).

format_kv_get_response(Key, {value, #{val := Val}}) when is_binary(Val) ->
    iolist_to_binary([
        <<"{\"key\":\"">>, Key, <<"\",\"value\":\"">>, Val, <<"\"}">>
    ]);
format_kv_get_response(Key, _) ->
    iolist_to_binary([<<"{\"key\":\"">>, Key, <<"\"}">>]).

format_kv_delete_response(Key, {deleted, #{timestamp := Ts}}) ->
    iolist_to_binary([
        <<"{\"deleted\":true,\"key\":\"">>, Key, <<"\",\"timestamp\":">>,
        integer_to_binary(Ts), <<"}">>
    ]);
format_kv_delete_response(Key, _) ->
    iolist_to_binary([<<"{\"deleted\":true,\"key\":\"">>, Key, <<"\"}">>]).

format_set_members_response(Key, {set_members, #{members := Members}}) ->
    MembersJson = lists:map(fun(M) -> iolist_to_binary([<<"\"">>, M, <<"\"">>]) end, Members),
    MembersStr = iolist_to_binary(lists:join(<<",">>, MembersJson)),
    iolist_to_binary([
        <<"{\"key\":\"">>, Key, <<"\",\"members\":[">>, MembersStr, <<"]}">>
    ]);
format_set_members_response(Key, _) ->
    iolist_to_binary([<<"{\"key\":\"">>, Key, <<"\",\"members\":[]}">>]).

format_counter_value_response(Key, {counter_value, #{val := Val}}) ->
    iolist_to_binary([
        <<"{\"key\":\"">>, Key, <<"\",\"value\":">>, integer_to_binary(Val), <<"}">>
    ]);
format_counter_value_response(Key, _) ->
    iolist_to_binary([<<"{\"key\":\"">>, Key, <<"\",\"value\":0}">>]).

ensure_binary(B) when is_binary(B) -> B;
ensure_binary(A) when is_atom(A) -> atom_to_binary(A);
ensure_binary(L) when is_list(L) -> list_to_binary(L).

is_not_found({value, #{found := false}}) -> true;
is_not_found({set_not_found, _}) -> true;
is_not_found({counter_not_found, _}) -> true;
is_not_found(_) -> false.

%% ===== Bucket helpers =====

make_atom(S) when is_binary(S) -> binary_to_atom(S, utf8);
make_atom(S) when is_list(S) -> list_to_atom(S);
make_atom(A) when is_atom(A) -> A.

make_bucket_config(Name, TypeStr, Ttl, N) ->
    Type = case TypeStr of
        <<"lww">> -> lww;
        <<"orswot">> -> orswot;
        <<"pn_counter">> -> pn_counter;
        _ -> lww
    end,
    #{name => Name, crdt_type => Type, ttl_seconds => Ttl,
      replication_n => N, created_at => erlang:system_time(millisecond)}.

make_bucket_params_put(Key, Value) ->
    #{key => Key, value => Value}.

make_bucket_params_key(Key) ->
    #{key => Key}.

make_bucket_params_element(Key, Element) ->
    #{key => Key, element => Element}.

make_bucket_params_amount(Key, Amount) ->
    #{key => Key, amount => Amount}.

format_bucket_response({ok, Bucket}) when is_map(Bucket) ->
    Name = maps:get(name, Bucket, <<>>),
    Type = atom_to_binary(maps:get(crdt_type, Bucket, lww)),
    Ttl = integer_to_binary(maps:get(ttl_seconds, Bucket, 0)),
    N = integer_to_binary(maps:get(replication_n, Bucket, 0)),
    iolist_to_binary([
        <<"{\"name\":\"">>, Name, <<"\",\"type\":\"">>, Type,
        <<"\",\"ttl_seconds\":">>, Ttl, <<",\"replication_n\":">>, N, <<"}">>
    ]);
format_bucket_response(_) -> <<"{\"error\":\"bucket_not_found\"}">>.

format_bucket_list_response(Buckets) when is_list(Buckets) ->
    Items = lists:map(fun(B) ->
        Name = maps:get(name, B, <<>>),
        Type = atom_to_binary(maps:get(crdt_type, B, lww)),
        Ttl = integer_to_binary(maps:get(ttl_seconds, B, 0)),
        iolist_to_binary([
            <<"{\"name\":\"">>, Name, <<"\",\"type\":\"">>, Type,
            <<"\",\"ttl_seconds\":">>, Ttl, <<"}">>
        ])
    end, Buckets),
    iolist_to_binary([<<"[">>, lists:join(<<",">>, Items), <<"]">>]);
format_bucket_list_response(_) -> <<"[]">>.

%% Handles both wrapped ({error, X}) and unwrapped (X) forms.
%% Bucket management endpoints (create/delete/get) return raw {error, X}.
%% Coordinator bucket_write/bucket_read return {error, X} which Gleam's
%% Result type unwraps to just X before passing to the error handler.
format_bucket_error({error, Reason}) -> format_bucket_error(Reason);
format_bucket_error(already_exists) ->
    <<"{\"error\":\"bucket_already_exists\"}">>;
format_bucket_error(not_found) ->
    <<"{\"error\":\"bucket_not_found\"}">>;
format_bucket_error(bucket_not_found) ->
    <<"{\"error\":\"bucket_not_found\"}">>;
format_bucket_error({type_mismatch, Expected, Got}) ->
    iolist_to_binary([
        <<"{\"error\":\"type_mismatch\",\"expected\":\"">>,
        atom_to_binary(Expected), <<"\",\"got\":\"">>,
        atom_to_binary(Got), <<"\"}">>
    ]);
format_bucket_error({invalid, Reason}) ->
    iolist_to_binary([<<"{\"error\":\"invalid_config\",\"reason\":\"">>, Reason, <<"\"}">>]);
format_bucket_error(_) ->
    <<"{\"error\":\"unknown_error\"}">>.

is_bucket_error({error, _}) -> true;
is_bucket_error(_) -> false.

format_bucket_read_response(Key, Resp) ->
    %% Delegate to the appropriate type-specific formatter.
    case Resp of
        {value, _} -> format_kv_get_response(Key, Resp);
        {set_members, _} -> format_set_members_response(Key, Resp);
        {counter_value, _} -> format_counter_value_response(Key, Resp);
        _ -> iolist_to_binary([<<"{\"key\":\"">>, Key, <<"\"}">>])
    end.
