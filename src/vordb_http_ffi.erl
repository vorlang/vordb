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
    is_not_found/1
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
