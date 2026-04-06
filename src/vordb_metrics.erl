-module(vordb_metrics).
-export([init/0, attach_handlers/0, format_prometheus/0,
         update_gauges/0, start_gauge_updater/1,
         %% Manual emit helpers (for Erlang callers)
         emit_request/4, emit_cache_hit/1, emit_cache_miss/1,
         emit_gossip_delta/2]).

-define(TABLE, vordb_metrics).

%% Tags are low-cardinality only:
%%   operation: put|get|delete|set_add|set_remove|set_members|counter_increment|counter_decrement|counter_value
%%   protocol: http|tcp
%%   status: ok|not_found|error
%%   crdt_type: lww|set|counter

%% ===== Init =====

init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ets:new(?TABLE, [named_table, public, set,
                {write_concurrency, true}, {read_concurrency, true}]);
        _ -> ok
    end,
    ok.

%% ===== Telemetry Handlers =====

attach_handlers() ->
    telemetry:attach(<<"vordb.request.stop">>,
        [vordb, request, stop],
        fun handle_request_stop/4, #{}),
    telemetry:attach(<<"vordb.cache.hit">>,
        [vordb, cache, hit],
        fun handle_counter_event/4, #{}),
    telemetry:attach(<<"vordb.cache.miss">>,
        [vordb, cache, miss],
        fun handle_counter_event/4, #{}),
    telemetry:attach(<<"vordb.gossip.delta.sent">>,
        [vordb, gossip, delta, sent],
        fun handle_counter_event/4, #{}),
    ok.

handle_request_stop(_Event, #{duration := Duration}, #{operation := Op, protocol := Proto, status := Status}, _Config) ->
    %% Counter
    inc_counter({request_count, Op, Proto, Status}, 1),
    %% Histogram
    Buckets = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000],
    record_histogram({request_duration, Op, Status}, Duration, Buckets).

handle_counter_event(Event, #{count := N}, Meta, _Config) ->
    %% Derive counter name from event path
    Name = list_to_atom(lists:flatten(lists:join("_", [atom_to_list(E) || E <- Event]))),
    Tags = maps:to_list(Meta),
    inc_counter({Name, Tags}, N).

%% ===== Manual Emit =====

emit_request(Operation, Protocol, Status, DurationUs) ->
    telemetry:execute([vordb, request, stop],
        #{duration => DurationUs},
        #{operation => Operation, protocol => Protocol, status => Status}).

emit_cache_hit(CrdtType) ->
    telemetry:execute([vordb, cache, hit], #{count => 1}, #{crdt_type => CrdtType}).

emit_cache_miss(CrdtType) ->
    telemetry:execute([vordb, cache, miss], #{count => 1}, #{crdt_type => CrdtType}).

emit_gossip_delta(CrdtType, KeyCount) ->
    telemetry:execute([vordb, gossip, delta, sent], #{count => 1}, #{crdt_type => CrdtType}),
    inc_counter({gossip_delta_keys, CrdtType}, KeyCount).

%% ===== ETS Operations =====

inc_counter(Key, Amount) ->
    try ets:update_counter(?TABLE, Key, {2, Amount})
    catch error:badarg -> ets:insert_new(?TABLE, {Key, Amount})
    end.

set_gauge(Key, Value) ->
    ets:insert(?TABLE, {Key, Value}).

record_histogram(Name, Value, Buckets) ->
    %% Increment bucket
    Bucket = find_bucket(Value, Buckets),
    inc_counter({histogram_bucket, Name, Bucket}, 1),
    %% Track sum and count
    inc_counter({histogram_sum, Name}, Value),
    inc_counter({histogram_count, Name}, 1).

find_bucket(Value, [B | _]) when Value =< B -> B;
find_bucket(Value, [_ | Rest]) -> find_bucket(Value, Rest);
find_bucket(_Value, []) -> infinity.

%% ===== Gauge Updater =====

start_gauge_updater(IntervalMs) ->
    spawn_link(fun() -> gauge_loop(IntervalMs) end).

gauge_loop(IntervalMs) ->
    update_gauges(),
    timer:sleep(IntervalMs),
    gauge_loop(IntervalMs).

update_gauges() ->
    try
        case whereis(vordb_ring_manager) of
            undefined -> ok;
            _ ->
                Ring = vordb_ring_manager:get_ring(),
                set_gauge({gauge, ring_version}, element(6, Ring)),
                set_gauge({gauge, partitions_owned}, length(vordb_ring_manager:my_partitions())),
                set_gauge({gauge, cluster_connected}, length(nodes()))
        end
    catch _:_ -> ok
    end.

%% ===== Prometheus Formatter =====

format_prometheus() ->
    Entries = ets:tab2list(?TABLE),
    iolist_to_binary([
        format_entries(Entries)
    ]).

format_entries(Entries) ->
    lists:filtermap(fun
        ({{request_count, Op, Proto, Status}, Val}) ->
            {true, [<<"vordb_request_count_total{operation=\"">>,
                    a2b(Op), <<"\",protocol=\"">>, a2b(Proto),
                    <<"\",status=\"">>, a2b(Status), <<"\"} ">>,
                    integer_to_binary(Val), <<"\n">>]};
        ({{histogram_bucket, {request_duration, Op, Status}, Bucket}, Val}) ->
            Le = case Bucket of infinity -> <<"+Inf">>; _ -> integer_to_binary(Bucket) end,
            {true, [<<"vordb_request_duration_us_bucket{operation=\"">>,
                    a2b(Op), <<"\",status=\"">>, a2b(Status),
                    <<"\",le=\"">>, Le, <<"\"} ">>,
                    integer_to_binary(Val), <<"\n">>]};
        ({{histogram_sum, {request_duration, Op, Status}}, Val}) ->
            {true, [<<"vordb_request_duration_us_sum{operation=\"">>,
                    a2b(Op), <<"\",status=\"">>, a2b(Status),
                    <<"\"} ">>, integer_to_binary(Val), <<"\n">>]};
        ({{histogram_count, {request_duration, Op, Status}}, Val}) ->
            {true, [<<"vordb_request_duration_us_count{operation=\"">>,
                    a2b(Op), <<"\",status=\"">>, a2b(Status),
                    <<"\"} ">>, integer_to_binary(Val), <<"\n">>]};
        ({{vordb_cache_hit, Tags}, Val}) ->
            {true, [<<"vordb_cache_hit_total">>, format_tag_list(Tags),
                    <<" ">>, integer_to_binary(Val), <<"\n">>]};
        ({{vordb_cache_miss, Tags}, Val}) ->
            {true, [<<"vordb_cache_miss_total">>, format_tag_list(Tags),
                    <<" ">>, integer_to_binary(Val), <<"\n">>]};
        ({{vordb_gossip_delta_sent, Tags}, Val}) ->
            {true, [<<"vordb_gossip_delta_sent_total">>, format_tag_list(Tags),
                    <<" ">>, integer_to_binary(Val), <<"\n">>]};
        ({{gossip_delta_keys, Type}, Val}) ->
            {true, [<<"vordb_gossip_delta_keys_total{crdt_type=\"">>,
                    a2b(Type), <<"\"} ">>, integer_to_binary(Val), <<"\n">>]};
        ({{gauge, Name}, Val}) ->
            {true, [<<"vordb_">>, a2b(Name), <<" ">>, integer_to_binary(Val), <<"\n">>]};
        (_) -> false
    end, Entries).

format_tag_list(Tags) ->
    case Tags of
        [] -> <<>>;
        _ ->
            Pairs = [<<(a2b(K))/binary, "=\"", (a2b(V))/binary, "\"">>
                     || {K, V} <- Tags],
            <<"{", (iolist_to_binary(lists:join(<<",">>, Pairs)))/binary, "}">>
    end.

a2b(A) when is_atom(A) -> atom_to_binary(A);
a2b(B) when is_binary(B) -> B;
a2b(L) when is_list(L) -> list_to_binary(L).
