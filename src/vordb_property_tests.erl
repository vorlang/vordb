-module(vordb_property_tests).
-export([run_lww_properties/0, run_or_set_properties/0, run_counter_properties/0, run_all/0]).

%% Simple property tests using random generation.
%% Not StreamData-based but tests the same CRDT properties.

-define(RUNS, 50).
-define(NODES, [n1, n2, n3, n4, n5]).

run_all() ->
    {LP, LF} = run_lww_properties(),
    {OP, OF} = run_or_set_properties(),
    {CP, CF} = run_counter_properties(),
    {LP + OP + CP, LF + OF + CF}.

%% ===== LWW Properties (through Vor agent) =====

run_lww_properties() ->
    Results = [
        run_property("lww_commutative", ?RUNS, fun prop_lww_commutative/0),
        run_property("lww_associative", 30, fun prop_lww_associative/0),
        run_property("lww_idempotent", ?RUNS, fun prop_lww_idempotent/0),
        run_property("lww_monotonic_ts", ?RUNS, fun prop_lww_monotonic/0),
        run_property("lww_preserves_keys", ?RUNS, fun prop_lww_preserves_keys/0)
    ],
    count_results(Results).

prop_lww_commutative() ->
    A = rand_lww_store(5),
    B = rand_lww_store(5),
    vor_merge(A, B) =:= vor_merge(B, A).

prop_lww_associative() ->
    A = rand_lww_store(3),
    B = rand_lww_store(3),
    C = rand_lww_store(3),
    AB = vor_merge(A, B),
    ABC_left = vor_merge(AB, C),
    BC = vor_merge(B, C),
    ABC_right = vor_merge(A, BC),
    ABC_left =:= ABC_right.

prop_lww_idempotent() ->
    A = rand_lww_store(5),
    vor_merge(A, A) =:= A.

prop_lww_monotonic() ->
    A = rand_lww_store(5),
    B = rand_lww_store(5),
    Merged = vor_merge(A, B),
    lists:all(fun(K) ->
        #{timestamp := MT} = maps:get(K, Merged),
        case maps:get(K, A, undefined) of
            undefined -> true;
            #{timestamp := AT} -> MT >= AT
        end andalso
        case maps:get(K, B, undefined) of
            undefined -> true;
            #{timestamp := BT} -> MT >= BT
        end
    end, maps:keys(Merged)).

prop_lww_preserves_keys() ->
    A = rand_lww_store(5),
    B = rand_lww_store(5),
    Merged = vor_merge(A, B),
    AKeys = maps:keys(A),
    BKeys = maps:keys(B),
    AllPresent = lists:all(fun(K) -> maps:is_key(K, Merged) end, AKeys ++ BKeys),
    NoExtra = maps:size(Merged) =:= length(lists:usort(AKeys ++ BKeys)),
    AllPresent andalso NoExtra.

vor_merge(Base, Incoming) ->
    vordb_registry:start(),
    Dir = <<"/tmp/vordb_prop_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    %% Stop any leftover processes
    catch gen_server:stop(vordb_dirty_tracker),
    catch vordb_ffi:storage_stop(),
    {ok, _} = vordb_ffi:storage_start(Dir),
    {ok, _} = vordb_dirty_tracker:start_link([{peers, []}, {num_vnodes, 4}]),
    {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
        [{node_id, prop_node}, {vnode_id, 0}, {sync_interval_ms, 600000}], []),
    case maps:size(Base) > 0 of
        true -> gen_server:cast(Pid, {lww_sync, #{remote_lww_store => Base}});
        false -> ok
    end,
    gen_server:cast(Pid, {lww_sync, #{remote_lww_store => Incoming}}),
    {stores, #{lww := Result}} = gen_server:call(Pid, {get_stores, #{}}),
    gen_server:stop(Pid),
    catch vordb_ffi:storage_stop(),
    catch gen_server:stop(vordb_dirty_tracker),
    os:cmd("rm -rf " ++ binary_to_list(Dir)),
    Result.

%% ===== OR-Set Properties =====

run_or_set_properties() ->
    Results = [
        run_property("orset_commutative", ?RUNS, fun prop_orset_commutative/0),
        run_property("orset_associative", 30, fun prop_orset_associative/0),
        run_property("orset_idempotent", ?RUNS, fun prop_orset_idempotent/0),
        run_property("orset_add_preserved", ?RUNS, fun prop_orset_add_preserved/0),
        run_property("orset_stores_commutative", ?RUNS, fun prop_orset_stores_commutative/0)
    ],
    count_results(Results).

prop_orset_commutative() ->
    A = rand_orset(),
    B = rand_orset(),
    vordb_ffi:orset_merge_stores(#{k => A}, #{k => B}) =:=
    vordb_ffi:orset_merge_stores(#{k => B}, #{k => A}).

prop_orset_associative() ->
    A = rand_orset(),
    B = rand_orset(),
    C = rand_orset(),
    AB = merge_orset(A, B),
    ABC_l = merge_orset(AB, C),
    BC = merge_orset(B, C),
    ABC_r = merge_orset(A, BC),
    ABC_l =:= ABC_r.

prop_orset_idempotent() ->
    A = rand_orset(),
    merge_orset(A, A) =:= A.

prop_orset_add_preserved() ->
    Base = rand_orset(),
    Element = rand_bin(),
    Tag = vordb_ffi:orset_make_tag(
        lists:nth(rand:uniform(length(?NODES)), ?NODES),
        rand:uniform(10000),
        rand:uniform(1000)),
    WithAdd = vordb_ffi:orset_add_element(Base, Element, Tag),
    Other = rand_orset(),
    Merged = merge_orset(WithAdd, Other),
    %% Element is present unless Other has the exact same tag in tombstones
    OtherTombs = maps:get(Element, maps:get(tombstones, Other), #{}),
    case maps:is_key(Tag, OtherTombs) of
        true -> true;  %% Tag tombstoned by other, element may be absent — OK
        false -> lists:member(Element, vordb_ffi:orset_read_elements(Merged))
    end.

prop_orset_stores_commutative() ->
    A = rand_orset_store(),
    B = rand_orset_store(),
    vordb_ffi:orset_merge_stores(A, B) =:= vordb_ffi:orset_merge_stores(B, A).

merge_orset(A, B) ->
    #{k := Merged} = vordb_ffi:orset_merge_stores(#{k => A}, #{k => B}),
    Merged.

%% ===== Counter Properties =====

run_counter_properties() ->
    Results = [
        run_property("counter_commutative", ?RUNS, fun prop_counter_commutative/0),
        run_property("counter_associative", 30, fun prop_counter_associative/0),
        run_property("counter_idempotent", ?RUNS, fun prop_counter_idempotent/0),
        run_property("counter_monotonic_counts", ?RUNS, fun prop_counter_monotonic/0),
        run_property("counter_stores_commutative", ?RUNS, fun prop_counter_stores_commutative/0)
    ],
    count_results(Results).

prop_counter_commutative() ->
    A = rand_counter(),
    B = rand_counter(),
    merge_counter(A, B) =:= merge_counter(B, A).

prop_counter_associative() ->
    A = rand_counter(),
    B = rand_counter(),
    C = rand_counter(),
    AB = merge_counter(A, B),
    ABC_l = merge_counter(AB, C),
    BC = merge_counter(B, C),
    ABC_r = merge_counter(A, BC),
    ABC_l =:= ABC_r.

prop_counter_idempotent() ->
    A = rand_counter(),
    merge_counter(A, A) =:= A.

prop_counter_monotonic() ->
    A = rand_counter(),
    B = rand_counter(),
    Merged = merge_counter(A, B),
    MP = maps:get(p, Merged),
    MN = maps:get(n, Merged),
    lists:all(fun(N) ->
        maps:get(N, MP, 0) >= maps:get(N, maps:get(p, A), 0) andalso
        maps:get(N, MP, 0) >= maps:get(N, maps:get(p, B), 0)
    end, maps:keys(MP)) andalso
    lists:all(fun(N) ->
        maps:get(N, MN, 0) >= maps:get(N, maps:get(n, A), 0) andalso
        maps:get(N, MN, 0) >= maps:get(N, maps:get(n, B), 0)
    end, maps:keys(MN)).

prop_counter_stores_commutative() ->
    A = rand_counter_store(),
    B = rand_counter_store(),
    vordb_ffi:counter_merge_stores(A, B) =:= vordb_ffi:counter_merge_stores(B, A).

merge_counter(A, B) ->
    #{k := Merged} = vordb_ffi:counter_merge_stores(#{k => A}, #{k => B}),
    Merged.

%% ===== Random Generators =====

rand_lww_store(MaxKeys) ->
    N = rand:uniform(MaxKeys),
    maps:from_list([{rand_key(), rand_lww_entry()} || _ <- lists:seq(1, N)]).

rand_lww_entry() ->
    #{value => rand_bin(), timestamp => rand:uniform(100000),
      node_id => lists:nth(rand:uniform(length(?NODES)), ?NODES)}.

rand_orset() ->
    N = rand:uniform(5),
    lists:foldl(fun(_, S) ->
        case rand:uniform(2) of
            1 -> vordb_ffi:orset_add_element(S, rand_bin(),
                    vordb_ffi:orset_make_tag(
                        lists:nth(rand:uniform(length(?NODES)), ?NODES),
                        rand:uniform(10000),
                        rand:uniform(1000)));
            2 -> vordb_ffi:orset_remove_element(S, rand_bin())
        end
    end, vordb_ffi:orset_get_or_empty(#{}, unused), lists:seq(1, N)).

rand_orset_store() ->
    N = rand:uniform(3),
    maps:from_list([{rand_key(), rand_orset()} || _ <- lists:seq(1, N)]).

rand_counter() ->
    N = rand:uniform(5),
    lists:foldl(fun(_, C) ->
        Node = lists:nth(rand:uniform(length(?NODES)), ?NODES),
        case rand:uniform(2) of
            1 -> vordb_ffi:counter_increment(C, Node, rand:uniform(100));
            2 -> vordb_ffi:counter_decrement(C, Node, rand:uniform(100))
        end
    end, vordb_ffi:counter_get_or_empty(#{}, unused), lists:seq(1, N)).

rand_counter_store() ->
    N = rand:uniform(3),
    maps:from_list([{rand_key(), rand_counter()} || _ <- lists:seq(1, N)]).

rand_key() ->
    list_to_binary("k" ++ integer_to_list(rand:uniform(10))).

rand_bin() ->
    list_to_binary("v" ++ integer_to_list(rand:uniform(100))).

%% ===== Helpers =====

run_property(Name, Runs, Fun) ->
    Results = [try Fun() catch _:_ -> false end || _ <- lists:seq(1, Runs)],
    Passed = length([true || true <- Results]),
    case Passed =:= Runs of
        true -> {Name, ok};
        false -> {Name, {fail, {passed, Passed, total, Runs}}}
    end.

count_results(Results) ->
    Passed = length([ok || {_, ok} <- Results]),
    Failed = length(Results) - Passed,
    lists:foreach(fun({Name, {fail, Reason}}) ->
        io:format("PROPERTY FAIL ~s: ~p~n", [Name, Reason]);
        ({_, ok}) -> ok
    end, Results),
    {Passed, Failed}.
