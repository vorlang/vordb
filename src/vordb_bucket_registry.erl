-module(vordb_bucket_registry).
-behaviour(gen_server).

%% Bucket registry — named collections with per-bucket CRDT type, TTL, and
%% replication factor. All bucket configs stored as a single serialized map
%% in RocksDB default CF (key "vordb_buckets"). Cached in persistent_term
%% for zero-cost lookup on the hot path.
%%
%% The gen_server owns writes (create/delete); readers call
%% persistent_term:get and do a map lookup — no IPC.

-export([start_link/0, create_bucket/1, delete_bucket/1,
         get_bucket/1, list_buckets/0, ensure_defaults/0]).
-export([init/1, handle_call/3, handle_cast/2]).

-define(PT_BUCKETS, {vordb_bucket_registry, buckets}).
-define(META_KEY, <<"vordb_buckets">>).

%% Default bucket names for backward-compatible old API.
-define(DEFAULT_LWW,     <<"__default_lww__">>).
-define(DEFAULT_SET,     <<"__default_set__">>).
-define(DEFAULT_COUNTER, <<"__default_counter__">>).

%% ===== Read path — persistent_term, no gen_server call =====

get_bucket(Name) when is_binary(Name) ->
    Buckets = persistent_term:get(?PT_BUCKETS),
    case maps:get(Name, Buckets, undefined) of
        undefined -> {error, not_found};
        B -> {ok, B}
    end.

list_buckets() ->
    maps:values(persistent_term:get(?PT_BUCKETS)).

%% ===== Write path — gen_server call =====

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_bucket(Bucket) ->
    gen_server:call(?MODULE, {create, Bucket}).

delete_bucket(Name) ->
    gen_server:call(?MODULE, {delete, Name}).

ensure_defaults() ->
    gen_server:call(?MODULE, ensure_defaults).

%% ===== gen_server callbacks =====

init([]) ->
    Buckets = load_buckets(),
    persistent_term:put(?PT_BUCKETS, Buckets),
    {ok, #{buckets => Buckets}}.

handle_call({create, Bucket}, _From, #{buckets := Buckets} = State) ->
    Name = maps:get(name, Bucket),
    case maps:is_key(Name, Buckets) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            case validate_bucket(Bucket) of
                ok ->
                    NewBuckets = maps:put(Name, Bucket, Buckets),
                    save_and_publish(NewBuckets),
                    {reply, ok, State#{buckets := NewBuckets}};
                {error, _} = Err ->
                    {reply, Err, State}
            end
    end;

handle_call({delete, Name}, _From, #{buckets := Buckets} = State) ->
    case maps:is_key(Name, Buckets) of
        false ->
            {reply, {error, not_found}, State};
        true ->
            NewBuckets = maps:remove(Name, Buckets),
            save_and_publish(NewBuckets),
            {reply, ok, State#{buckets := NewBuckets}}
    end;

handle_call(ensure_defaults, _From, #{buckets := Buckets} = State) ->
    Now = erlang:system_time(millisecond),
    Defaults = [
        {?DEFAULT_LWW, #{name => ?DEFAULT_LWW, crdt_type => lww,
                          ttl_seconds => 0, replication_n => 0,
                          write_quorum => 0, read_quorum => 0, created_at => Now}},
        {?DEFAULT_SET, #{name => ?DEFAULT_SET, crdt_type => orswot,
                          ttl_seconds => 0, replication_n => 0,
                          write_quorum => 0, read_quorum => 0, created_at => Now}},
        {?DEFAULT_COUNTER, #{name => ?DEFAULT_COUNTER, crdt_type => pn_counter,
                              ttl_seconds => 0, replication_n => 0,
                              write_quorum => 0, read_quorum => 0, created_at => Now}}
    ],
    NewBuckets = lists:foldl(fun({Name, Config}, Acc) ->
        case maps:is_key(Name, Acc) of
            true -> Acc;
            false -> maps:put(Name, Config, Acc)
        end
    end, Buckets, Defaults),
    case NewBuckets =/= Buckets of
        true -> save_and_publish(NewBuckets);
        false -> ok
    end,
    {reply, ok, State#{buckets := NewBuckets}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% ===== Validation =====

validate_bucket(Bucket) ->
    Name = maps:get(name, Bucket),
    Type = maps:get(crdt_type, Bucket),
    TTL = maps:get(ttl_seconds, Bucket, 0),
    N = maps:get(replication_n, Bucket, 0),
    if
        not is_binary(Name) orelse byte_size(Name) =:= 0 ->
            {error, {invalid, <<"name must be non-empty binary">>}};
        byte_size(Name) > 64 ->
            {error, {invalid, <<"name must be <= 64 characters">>}};
        Type =/= lww andalso Type =/= orswot andalso Type =/= pn_counter ->
            {error, {invalid, <<"crdt_type must be lww, orswot, or pn_counter">>}};
        TTL < 0 ->
            {error, {invalid, <<"ttl_seconds must be >= 0">>}};
        N < 0 ->
            {error, {invalid, <<"replication_n must be >= 0">>}};
        true -> ok
    end.

%% ===== Persistence =====

save_and_publish(Buckets) ->
    gen_server:call(vordb_storage, {meta_put, ?META_KEY,
                                    erlang:term_to_binary(Buckets)}),
    persistent_term:put(?PT_BUCKETS, Buckets).

load_buckets() ->
    case whereis(vordb_storage) of
        undefined -> #{};
        _ ->
            case gen_server:call(vordb_storage, {meta_get, ?META_KEY}) of
                {ok, Bin} -> erlang:binary_to_term(Bin);
                _ -> #{}
            end
    end.
