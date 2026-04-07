-module(vordb_bench).
-export([main/1, scenario1/0, scenario2/0, scenario5/0, scenario9/0, scenario_eprof/0]).

%% VorDB benchmark — Scenarios 1 (single-op latency) and 2 (throughput).
%%
%% Self-contained: boots an in-VM VorDB node, drives it via the protobuf TCP
%% protocol, reports latency percentiles + throughput.

-define(PORT, 25001).
-define(VALUE_SIZE, 100).

%% ============================================================
%% Entry
%% ============================================================

main(Args) ->
    Scenario = case Args of
        [S | _] when is_atom(S) -> S;
        _ -> all
    end,
    io:format("~n=== VorDB Benchmark ===~n", []),
    application:ensure_all_started(telemetry),
    %% Quiet logger so warnings don't drown the output
    logger:set_primary_config(level, error),
    case Scenario of
        scenario7 ->
            %% Cluster scenario manages its own peer nodes
            vordb_bench_cluster:scenario7();
        _ ->
            io:format("Booting in-VM node on TCP port ~p...~n", [?PORT]),
            {ok, _} = vordb_bench_node:start(?PORT),
            try
                case Scenario of
                    scenario1 -> scenario1();
                    scenario2 -> scenario2();
                    scenario5 -> scenario5();
                    scenario9 -> scenario9();
                    eprof     -> scenario_eprof();
                    all       -> scenario1(), scenario2()
                end
            after
                vordb_bench_node:stop()
            end
    end,
    io:format("~n=== Done ===~n", []),
    halt(0).

%% ============================================================
%% Scenario 1: Single-op latency (1 client, sequential)
%% ============================================================

scenario1() ->
    Ops = 10000,
    io:format("~n--- Scenario 1: single-op latency (1 client, ~p ops) ---~n", [Ops]),
    {ok, Sock} = connect(),
    Value = binary:copy(<<"x">>, ?VALUE_SIZE),

    %% Warmup
    warmup(Sock, 200, Value),

    PutLat = run_put_loop(Sock, Ops, Value, []),
    GetLat = run_get_loop(Sock, Ops, []),

    gen_tcp:close(Sock),
    report("PUT (TCP)", PutLat),
    report("GET (TCP)", GetLat),
    ok.

run_put_loop(_Sock, 0, _Value, Acc) -> Acc;
run_put_loop(Sock, N, Value, Acc) ->
    Key = key(N),
    T0 = erlang:monotonic_time(microsecond),
    _ = tcp_put(Sock, Key, Value),
    T1 = erlang:monotonic_time(microsecond),
    run_put_loop(Sock, N - 1, Value, [T1 - T0 | Acc]).

run_get_loop(_Sock, 0, Acc) -> Acc;
run_get_loop(Sock, N, Acc) ->
    Key = key(N),
    T0 = erlang:monotonic_time(microsecond),
    _ = tcp_get(Sock, Key),
    T1 = erlang:monotonic_time(microsecond),
    run_get_loop(Sock, N - 1, [T1 - T0 | Acc]).

%% ============================================================
%% Scenario 2: Throughput under load (variable concurrency)
%% ============================================================

scenario2() ->
    ClientLevels = [1, 10, 50, 100, 200],
    DurationMs = 10000,
    KeySpace = 100000,
    CsvFile = "results/scenario2.csv",
    filelib:ensure_dir(CsvFile),
    {ok, Csv} = file:open(CsvFile, [write]),
    io:fwrite(Csv, "clients,put_per_sec,get_per_sec,total_per_sec,put_p50,put_p95,put_p99,get_p50,get_p95,get_p99,errors~n", []),
    put(bench_csv, Csv),
    io:format("~n--- Scenario 2: throughput under load (80%% GET / 20%% PUT, ~ps per level) ---~n",
              [DurationMs div 1000]),
    io:format("clients | put/s   | get/s   | total/s | put p50/p95/p99 us  | get p50/p95/p99 us  | err~n", []),
    io:format("--------+---------+---------+---------+---------------------+---------------------+----~n", []),

    %% Warmup: prepopulate ~5k keys so GETs hit something
    io:format("[warmup: prepopulating 5000 keys...]~n", []),
    {ok, WarmSock} = connect(),
    Value = binary:copy(<<"v">>, ?VALUE_SIZE),
    lists:foreach(fun(I) ->
        _ = tcp_put(WarmSock, key(I), Value)
    end, lists:seq(1, 5000)),
    gen_tcp:close(WarmSock),
    io:format("[warmup done]~n", []),

    lists:foreach(fun(N) ->
        run_concurrent(N, DurationMs, KeySpace, Value)
    end, ClientLevels),
    file:close(Csv),
    io:format("~n[results saved to ~s]~n", [CsvFile]),
    ok.

run_concurrent(NumClients, DurationMs, KeySpace, Value) ->
    io:format("[running ~w clients for ~ws...]~n", [NumClients, DurationMs div 1000]),
    StatsRef = ets:new(bench_stats, [public, set, {write_concurrency, true}]),
    ets:insert(StatsRef, {{put, lats}, []}),
    ets:insert(StatsRef, {{get, lats}, []}),
    ets:insert(StatsRef, {{put, count}, 0}),
    ets:insert(StatsRef, {{get, count}, 0}),
    ets:insert(StatsRef, {errors, 0}),

    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Refs = [begin
        {Pid, Ref} = spawn_monitor(fun() ->
            client_loop(Parent, StatsRef, Deadline, KeySpace, Value)
        end),
        {Pid, Ref}
    end || _ <- lists:seq(1, NumClients)],

    %% Wait for all clients to terminate (normal or crash)
    lists:foreach(fun({_Pid, Ref}) ->
        receive
            {'DOWN', Ref, process, _, _} -> ok
        end
    end, Refs),
    %% Drain any leftover {done, _} messages
    flush_done(),

    PutLats = lists:append(get_lats(StatsRef, put)),
    GetLats = lists:append(get_lats(StatsRef, get)),
    PutCount = length(PutLats),
    GetCount = length(GetLats),
    [{errors, Errors}] = ets:lookup(StatsRef, errors),
    Secs = DurationMs / 1000,
    PutPs = trunc(PutCount / Secs),
    GetPs = trunc(GetCount / Secs),
    {P50p, P95p, P99p} = pcts(PutLats),
    {P50g, P95g, P99g} = pcts(GetLats),
    io:format("~7w | ~7w | ~7w | ~7w | ~5w/~5w/~5w     | ~5w/~5w/~5w     | ~3w~n",
        [NumClients, PutPs, GetPs, PutPs + GetPs,
         P50p, P95p, P99p, P50g, P95g, P99g, Errors]),
    case get(bench_csv) of
        undefined -> ok;
        Csv ->
            io:fwrite(Csv, "~w,~w,~w,~w,~w,~w,~w,~w,~w,~w,~w~n",
                [NumClients, PutPs, GetPs, PutPs + GetPs,
                 P50p, P95p, P99p, P50g, P95g, P99g, Errors])
    end,
    ets:delete(StatsRef),
    ok.

flush_done() ->
    receive {done, _} -> flush_done() after 0 -> ok end.

%% ============================================================
%% Scenario 5: CRDT type comparison (50 clients × 10s per type)
%% ============================================================

scenario5() ->
    Clients = 50,
    DurationMs = 10000,
    KeySpace = 200,  %% Small so set_members has elements to read against
    CsvFile = "results/scenario5.csv",
    filelib:ensure_dir(CsvFile),
    {ok, Csv} = file:open(CsvFile, [write]),
    io:fwrite(Csv,
        "crdt_type,write_op,read_op,write_per_sec,read_per_sec,"
        "write_p50,write_p99,read_p50,read_p99~n", []),
    io:format("~n--- Scenario 5: CRDT type comparison (~w clients × ~ws per type) ---~n",
              [Clients, DurationMs div 1000]),
    io:format("type    | write/s | read/s  | write p50/p99 us  | read p50/p99 us~n", []),
    io:format("--------+---------+---------+-------------------+----------------~n", []),

    %% LWW
    {LwR, LwW} = run_crdt_workload(lww, Clients, DurationMs, KeySpace),
    write_crdt_csv(Csv, lww, "put", "get", LwW, LwR),

    %% ORSWOT — pre-grow ~100 elements per set so reads have data
    pregrow_sets(KeySpace, 100),
    {SR, SW} = run_crdt_workload(set, Clients, DurationMs, KeySpace),
    write_crdt_csv(Csv, set, "set_add", "set_members", SW, SR),

    %% PN-Counter
    {CR, CW} = run_crdt_workload(counter, Clients, DurationMs, KeySpace),
    write_crdt_csv(Csv, counter, "increment", "value", CW, CR),

    file:close(Csv),
    io:format("~n[results saved to ~s]~n", [CsvFile]),
    ok.

pregrow_sets(KeySpace, ElementsPerKey) ->
    io:format("[pregrow: adding ~w elements to ~w set keys...]~n", [ElementsPerKey, KeySpace]),
    {ok, Sock} = connect(),
    lists:foreach(fun(K) ->
        Key = key(K),
        lists:foreach(fun(E) ->
            Elem = <<"e", (integer_to_binary(E))/binary>>,
            _ = tcp_set_add(Sock, Key, Elem)
        end, lists:seq(1, ElementsPerKey))
    end, lists:seq(1, KeySpace)),
    gen_tcp:close(Sock),
    io:format("[pregrow done]~n", []).

run_crdt_workload(Type, NumClients, DurationMs, KeySpace) ->
    StatsRef = ets:new(crdt_stats, [public, set, {write_concurrency, true}]),
    ets:insert(StatsRef, {{w, lats}, []}),
    ets:insert(StatsRef, {{r, lats}, []}),
    ets:insert(StatsRef, {errors, 0}),
    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Refs = [begin
        {Pid, Ref} = spawn_monitor(fun() ->
            crdt_client(Type, Parent, StatsRef, Deadline, KeySpace)
        end),
        {Pid, Ref}
    end || _ <- lists:seq(1, NumClients)],
    lists:foreach(fun({_, Ref}) ->
        receive {'DOWN', Ref, process, _, _} -> ok end
    end, Refs),
    flush_done(),
    WLats = lists:append(get_lats(StatsRef, w)),
    RLats = lists:append(get_lats(StatsRef, r)),
    [{errors, _Errs}] = ets:lookup(StatsRef, errors),
    ets:delete(StatsRef),
    Secs = DurationMs / 1000,
    WResult = mk_result(WLats, Secs),
    RResult = mk_result(RLats, Secs),
    print_crdt_row(Type, WResult, RResult),
    {RResult, WResult}.

mk_result(Lats, Secs) ->
    Count = length(Lats),
    {P50, _P95, P99} = pcts(Lats),
    #{count => Count, per_sec => trunc(Count / Secs), p50 => P50, p99 => P99}.

print_crdt_row(Type, W, R) ->
    io:format("~-7s | ~7w | ~7w |   ~6w/~6w    |  ~5w/~5w~n",
        [atom_to_list(Type), maps:get(per_sec, W), maps:get(per_sec, R),
         maps:get(p50, W), maps:get(p99, W),
         maps:get(p50, R), maps:get(p99, R)]).

write_crdt_csv(Csv, Type, WOp, ROp, W, R) ->
    io:fwrite(Csv, "~s,~s,~s,~w,~w,~w,~w,~w,~w~n",
        [atom_to_list(Type), WOp, ROp,
         maps:get(per_sec, W), maps:get(per_sec, R),
         maps:get(p50, W), maps:get(p99, W),
         maps:get(p50, R), maps:get(p99, R)]).

crdt_client(Type, Parent, StatsRef, Deadline, KeySpace) ->
    case connect() of
        {ok, Sock} ->
            {WAcc, RAcc} = crdt_loop(Type, Sock, Deadline, KeySpace, [], []),
            gen_tcp:close(Sock),
            append_lats(StatsRef, w, WAcc),
            append_lats(StatsRef, r, RAcc);
        _ ->
            ets:update_counter(StatsRef, errors, 1)
    end,
    Parent ! {done, self()}.

crdt_loop(Type, Sock, Deadline, KeySpace, WAcc, RAcc) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        false -> {WAcc, RAcc};
        true ->
            Op = case rand:uniform(2) of 1 -> w; _ -> r end,
            Key = key(rand:uniform(KeySpace)),
            T0 = erlang:monotonic_time(microsecond),
            R = crdt_call(Type, Op, Sock, Key),
            T1 = erlang:monotonic_time(microsecond),
            D = T1 - T0,
            {WAcc2, RAcc2} = case R of
                {error, _} -> {WAcc, RAcc};
                _ ->
                    case Op of
                        w -> {[D | WAcc], RAcc};
                        r -> {WAcc, [D | RAcc]}
                    end
            end,
            crdt_loop(Type, Sock, Deadline, KeySpace, WAcc2, RAcc2)
    end.

crdt_call(lww, w, Sock, Key)     -> tcp_put(Sock, Key, <<"v">>);
crdt_call(lww, r, Sock, Key)     -> tcp_get(Sock, Key);
crdt_call(set, w, Sock, Key)     -> tcp_set_add(Sock, Key, rand_elem());
crdt_call(set, r, Sock, Key)     -> tcp_set_members(Sock, Key);
crdt_call(counter, w, Sock, Key) -> tcp_counter_inc(Sock, Key, 1);
crdt_call(counter, r, Sock, Key) -> tcp_counter_value(Sock, Key).

rand_elem() ->
    <<"x", (integer_to_binary(rand:uniform(10000)))/binary>>.

%% ============================================================
%% Scenario 9: Partition balance (100k random PUTs)
%% ============================================================

scenario9() ->
    NumKeys = 100000,
    Workers = 20,
    PerWorker = NumKeys div Workers,
    io:format("~n--- Scenario 9: partition balance (~w random PUTs) ---~n", [NumKeys]),
    Value = binary:copy(<<"v">>, ?VALUE_SIZE),
    Parent = self(),
    io:format("[writing ~w keys via ~w concurrent writers...]~n", [NumKeys, Workers]),
    T0 = erlang:monotonic_time(millisecond),
    Refs = [begin
        WN = WorkerNum,
        {Pid, Ref} = spawn_monitor(fun() ->
            balance_writer(WN, PerWorker, Value),
            Parent ! {done, self()}
        end),
        {Pid, Ref}
    end || WorkerNum <- lists:seq(1, Workers)],
    lists:foreach(fun({_, Ref}) ->
        receive {'DOWN', Ref, process, _, _} -> ok end
    end, Refs),
    flush_done(),
    T1 = erlang:monotonic_time(millisecond),
    io:format("[wrote in ~w ms]~n", [T1 - T0]),

    %% Query each owned partition's key count via vordb_ffi
    Parts = lists:sort(vordb_ring_manager:my_partitions()),
    io:format("[counting keys in ~w partitions...]~n", [length(Parts)]),
    Counts = [{P, vordb_ffi:storage_count_partition_keys(P)} || P <- Parts],
    print_balance(NumKeys, Counts),

    CsvFile = "results/scenario9.csv",
    filelib:ensure_dir(CsvFile),
    {ok, Csv} = file:open(CsvFile, [write]),
    io:fwrite(Csv, "partition,keys~n", []),
    lists:foreach(fun({P, C}) -> io:fwrite(Csv, "~w,~w~n", [P, C]) end, Counts),
    file:close(Csv),
    io:format("[results saved to ~s]~n", [CsvFile]),
    ok.

balance_writer(WorkerNum, NumOps, Value) ->
    case connect() of
        {ok, Sock} ->
            lists:foreach(fun(I) ->
                K = <<"rk:",
                      (integer_to_binary(WorkerNum))/binary, ":",
                      (integer_to_binary(I))/binary, ":",
                      (integer_to_binary(rand:uniform(1 bsl 30)))/binary>>,
                _ = tcp_put(Sock, K, Value)
            end, lists:seq(1, NumOps)),
            gen_tcp:close(Sock);
        _ -> ok
    end.

print_balance(Expected, Counts) ->
    Vals = [C || {_, C} <- Counts],
    NumParts = length(Vals),
    Total = lists:sum(Vals),
    Min = lists:min(Vals),
    Max = lists:max(Vals),
    Mean = Total / NumParts,
    %% Stddev
    SqSum = lists:sum([(V - Mean) * (V - Mean) || V <- Vals]),
    StdDev = math:sqrt(SqSum / NumParts),
    Ratio = case Min of 0 -> infinity; _ -> Max / Min end,
    Ideal = Expected div NumParts,
    io:format("~n  partitions:   ~w~n", [NumParts]),
    io:format("  total keys:   ~w (expected ~w; missing ~w)~n",
              [Total, Expected, Expected - Total]),
    io:format("  ideal/part:   ~w~n", [Ideal]),
    io:format("  min:          ~w~n", [Min]),
    io:format("  max:          ~w~n", [Max]),
    io:format("  mean:         ~.1f~n", [Mean]),
    io:format("  stddev:       ~.1f (~.1f%% of mean)~n", [StdDev, 100 * StdDev / Mean]),
    io:format("  max/min:      ~p~n", [Ratio]),
    io:format("~n  per-partition:~n", []),
    lists:foreach(fun({P, C}) ->
        Bar = lists:duplicate(C * 40 div max(1, Max), $#),
        io:format("  P~3..0w | ~6w  ~s~n", [P, C, Bar])
    end, Counts).

%% ============================================================
%% Eprof scenario: 100 clients × 10s, profile server-side procs
%% ============================================================

scenario_eprof() ->
    Clients = 100,
    DurationMs = 10000,
    KeySpace = 100000,
    Value = binary:copy(<<"v">>, ?VALUE_SIZE),
    EprofFile = "results/eprof.txt",
    filelib:ensure_dir(EprofFile),

    io:format("~n--- Scenario eprof: ~w clients × ~ws ---~n", [Clients, DurationMs div 1000]),

    %% Warmup
    io:format("[warmup: prepopulating 5000 keys...]~n", []),
    {ok, WarmSock} = connect(),
    lists:foreach(fun(I) -> _ = tcp_put(WarmSock, key(I), Value) end, lists:seq(1, 5000)),
    gen_tcp:close(WarmSock),
    io:format("[warmup done]~n", []),

    %% Snapshot server procs BEFORE spawning load — these are what we profile.
    ServerProcsA = erlang:processes() -- [self()],
    NumProcsA = length(ServerProcsA),

    %% ---- Phase A: eprof CPU profiling ----
    io:format("~n[Phase A: eprof — profiling ~w server procs]~n", [NumProcsA]),
    {ok, _} = eprof:start(),
    eprof:log(EprofFile),
    profiling = eprof:start_profiling(ServerProcsA),
    run_eprof_load(Clients, DurationMs, KeySpace, Value),
    eprof:stop_profiling(),
    eprof:analyze(total, [{sort, time}]),
    eprof:stop(),
    io:format("[eprof analysis written to ~s]~n", [EprofFile]),
    print_top_eprof_functions(EprofFile, 10),

    %% ---- Phase B: receive message counts ----
    io:format("~n[Phase B: tracing 'receive' on server procs]~n", []),
    %% Re-snapshot — vnode procs survived but ephemeral procs (eprof tracer) are gone
    ServerProcsB = erlang:processes() -- [self()],
    Counter = ets:new(msg_counts, [public, set, {write_concurrency, true}]),
    Tracer = spawn(fun() -> tracer_loop(Counter) end),
    EnabledCount = lists:foldl(fun(P, Acc) ->
        case catch erlang:trace(P, true, ['receive', {tracer, Tracer}]) of
            N when is_integer(N) -> Acc + 1;
            _ -> Acc
        end
    end, 0, ServerProcsB),
    io:format("[traced ~w procs]~n", [EnabledCount]),
    run_eprof_load(Clients, DurationMs, KeySpace, Value),
    %% Disable tracing on the snapshot
    lists:foreach(fun(P) ->
        catch erlang:trace(P, false, ['receive'])
    end, ServerProcsB),
    %% Drain
    timer:sleep(300),
    print_top_procs(Counter, 10),
    Tracer ! stop,
    ets:delete(Counter),
    ok.

run_eprof_load(NumClients, DurationMs, KeySpace, Value) ->
    StatsRef = ets:new(eprof_stats, [public, set, {write_concurrency, true}]),
    ets:insert(StatsRef, {{put, lats}, []}),
    ets:insert(StatsRef, {{get, lats}, []}),
    ets:insert(StatsRef, {errors, 0}),
    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Refs = [begin
        {Pid, Ref} = spawn_monitor(fun() ->
            client_loop(Parent, StatsRef, Deadline, KeySpace, Value)
        end),
        {Pid, Ref}
    end || _ <- lists:seq(1, NumClients)],
    lists:foreach(fun({_, Ref}) ->
        receive {'DOWN', Ref, process, _, _} -> ok end
    end, Refs),
    flush_done(),
    PutCount = length(lists:append(get_lats(StatsRef, put))),
    GetCount = length(lists:append(get_lats(StatsRef, get))),
    [{errors, Errors}] = ets:lookup(StatsRef, errors),
    Secs = DurationMs / 1000,
    io:format("[load: ~w put/s  ~w get/s  ~w total/s  errors=~w]~n",
        [trunc(PutCount/Secs), trunc(GetCount/Secs),
         trunc((PutCount+GetCount)/Secs), Errors]),
    ets:delete(StatsRef),
    ok.

tracer_loop(Counter) ->
    receive
        {trace, Pid, 'receive', _Msg} ->
            ets:update_counter(Counter, Pid, 1, {Pid, 0}),
            tracer_loop(Counter);
        stop -> ok
    end.

print_top_procs(Counter, N) ->
    All = ets:tab2list(Counter),
    Sorted = lists:sort(fun({_, A}, {_, B}) -> A > B end, All),
    Top = lists:sublist(Sorted, N),
    io:format("~n  rank | msg count | registered name        | initial call~n", []),
    io:format("  -----+-----------+------------------------+----------------------~n", []),
    lists:foldl(fun({Pid, Count}, R) ->
        Reg = case process_info(Pid, registered_name) of
            {registered_name, Name} when is_atom(Name) -> atom_to_list(Name);
            _ -> "-"
        end,
        Init = case process_info(Pid, initial_call) of
            {initial_call, IC} -> io_lib:format("~p", [IC]);
            _ -> "(dead)"
        end,
        io:format("  ~4w | ~9w | ~-22s | ~s~n", [R, Count, Reg, lists:flatten(Init)]),
        R + 1
    end, 1, Top),
    ok.

print_top_eprof_functions(File, N) ->
    case file:read_file(File) of
        {ok, Bin} ->
            Lines = binary:split(Bin, <<"\n">>, [global]),
            io:format("~n  TOP ~w functions by cumulative time:~n", [N]),
            io:format("  ~s~n", [string:copies("-", 78)]),
            FunLines = extract_function_lines(Lines, N),
            lists:foreach(fun(L) -> io:format("  ~s~n", [L]) end, FunLines);
        {error, R} ->
            io:format("[failed to read ~s: ~p]~n", [File, R])
    end.

extract_function_lines(Lines, N) ->
    %% eprof:analyze(total, [{sort, time}]) prints functions in ASCENDING order
    %% of cumulative time. We want the largest N — take all function lines,
    %% then keep the last N (right before "Total:" footer).
    All = collect_function_lines(Lines, []),
    Len = length(All),
    Skip = max(0, Len - N),
    lists:nthtail(Skip, All).

collect_function_lines([], Acc) -> lists:reverse(Acc);
collect_function_lines([L | Rest], Acc) ->
    Trimmed = string:trim(binary_to_list(L)),
    case is_function_row(Trimmed) of
        true -> collect_function_lines(Rest, [Trimmed | Acc]);
        false -> collect_function_lines(Rest, Acc)
    end.

is_function_row("") -> false;
is_function_row("Total:" ++ _) -> false;
is_function_row("FUNCTION" ++ _) -> false;
is_function_row("--------" ++ _) -> false;
is_function_row("******" ++ _) -> false;
is_function_row(S) ->
    %% Function rows look like "mod:fun/arity   N  P.PP   T  [  avg]"
    case string:find(S, ":") of
        nomatch -> false;
        _ ->
            case string:find(S, "/") of
                nomatch -> false;
                _ -> true
            end
    end.

client_loop(Parent, StatsRef, Deadline, KeySpace, Value) ->
    case connect() of
        {ok, Sock} ->
            run_client(Parent, StatsRef, Deadline, KeySpace, Value, Sock);
        {error, _} ->
            ets:update_counter(StatsRef, errors, 1),
            Parent ! {done, self()}
    end.

run_client(Parent, StatsRef, Deadline, KeySpace, Value, Sock) ->
    PutAcc = [],
    GetAcc = [],
    {PA, GA, Errs} = client_loop_inner(Sock, StatsRef, Deadline, KeySpace, Value, PutAcc, GetAcc, 0),
    gen_tcp:close(Sock),
    %% Append to ets list (atomic via update_element won't work for lists; just lookup+insert)
    append_lats(StatsRef, put, PA),
    append_lats(StatsRef, get, GA),
    case Errs of
        0 -> ok;
        _ -> ets:update_counter(StatsRef, errors, Errs)
    end,
    Parent ! {done, self()}.

client_loop_inner(Sock, StatsRef, Deadline, KeySpace, Value, PA, GA, Errs) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        false -> {PA, GA, Errs};
        true ->
            Op = case rand:uniform(100) of N when N =< 80 -> get; _ -> put end,
            Key = key(rand:uniform(KeySpace)),
            T0 = erlang:monotonic_time(microsecond),
            R = case Op of
                put -> tcp_put(Sock, Key, Value);
                get -> tcp_get(Sock, Key)
            end,
            T1 = erlang:monotonic_time(microsecond),
            D = T1 - T0,
            {PA2, GA2, Errs2} = case R of
                {error, _} ->
                    {PA, GA, Errs + 1};
                _ ->
                    case Op of
                        put -> {[D | PA], GA, Errs};
                        get -> {PA, [D | GA], Errs}
                    end
            end,
            client_loop_inner(Sock, StatsRef, Deadline, KeySpace, Value, PA2, GA2, Errs2)
    end.

append_lats(StatsRef, Op, Lats) ->
    Key = {Op, lats},
    [{Key, Old}] = ets:lookup(StatsRef, Key),
    ets:insert(StatsRef, {Key, [Lats | Old]}).

get_lats(StatsRef, Op) ->
    [{_, L}] = ets:lookup(StatsRef, {Op, lats}),
    L.

%% ============================================================
%% TCP client (protobuf, packet,4)
%% ============================================================

connect() ->
    gen_tcp:connect("127.0.0.1", ?PORT,
        [binary, {active, false}, {packet, 4}, {nodelay, true}, {send_timeout, 5000}],
        5000).

tcp_put(Sock, Key, Value) ->
    Req = #{request_id => 1, body => {put, #{key => Key, value => Value}}},
    request(Sock, Req).

tcp_get(Sock, Key) ->
    Req = #{request_id => 1, body => {get, #{key => Key}}},
    request(Sock, Req).

tcp_set_add(Sock, Key, Element) ->
    request(Sock, #{request_id => 1, body => {set_add, #{key => Key, element => Element}}}).

tcp_set_members(Sock, Key) ->
    request(Sock, #{request_id => 1, body => {set_members, #{key => Key}}}).

tcp_counter_inc(Sock, Key, Amount) ->
    request(Sock, #{request_id => 1, body => {counter_increment, #{key => Key, amount => Amount}}}).

tcp_counter_value(Sock, Key) ->
    request(Sock, #{request_id => 1, body => {counter_value, #{key => Key}}}).

request(Sock, Msg) ->
    Bin = vordb_pb:encode_msg(Msg, 'Request'),
    case gen_tcp:send(Sock, Bin) of
        ok ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, RespBin} -> vordb_pb:decode_msg(RespBin, 'Response');
                {error, E} -> {error, E}
            end;
        {error, E} -> {error, E}
    end.

warmup(Sock, N, Value) ->
    lists:foreach(fun(I) ->
        _ = tcp_put(Sock, <<"warmup_", (integer_to_binary(I))/binary>>, Value)
    end, lists:seq(1, N)).

%% ============================================================
%% Reporting
%% ============================================================

key(N) when is_integer(N) ->
    <<"bench:", (integer_to_binary(N))/binary>>.

report(Label, Lats) ->
    Sorted = lists:sort(Lats),
    Len = length(Sorted),
    case Len of
        0 -> io:format("~s: no samples~n", [Label]);
        _ ->
            Min = hd(Sorted),
            Max = lists:last(Sorted),
            Mean = lists:sum(Sorted) div Len,
            P50 = pct(Sorted, Len, 0.50),
            P95 = pct(Sorted, Len, 0.95),
            P99 = pct(Sorted, Len, 0.99),
            io:format("~-12s n=~w  min=~wus  mean=~wus  p50=~wus  p95=~wus  p99=~wus  max=~wus~n",
                [Label, Len, Min, Mean, P50, P95, P99, Max])
    end.

pcts([]) -> {0, 0, 0};
pcts(Lats) ->
    Sorted = lists:sort(Lats),
    Len = length(Sorted),
    {pct(Sorted, Len, 0.50), pct(Sorted, Len, 0.95), pct(Sorted, Len, 0.99)}.

pct(Sorted, Len, Q) ->
    Idx = max(1, round(Len * Q)),
    lists:nth(Idx, Sorted).
