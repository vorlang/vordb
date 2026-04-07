-module(vordb_bench_cluster).
-export([scenario7/0]).

%% Scenario 7 — gossip convergence on a 3-node local cluster.
%%
%% Spawns 3 child BEAM nodes via peer:start_link, each running a full VorDB
%% stack (storage gen_server, ring_manager, vnodes, TCP server). All three
%% nodes are members of an 8-partition / N=3 ring, so every node owns every
%% partition. Then drives writes to node 1's TCP and polls node 2 for the
%% value, measuring elapsed time. Repeats with three sync_interval_ms values
%% to show the relationship between gossip cadence and convergence time.

-define(SAMPLES, 200).
-define(WRITE_INTERVAL_MS, 10).
-define(POLL_INTERVAL_MS, 1).
-define(MAX_WAIT_MS, 30000).
-define(RING_SIZE, 8).
-define(NVAL, 3).
-define(BASE_PORT, 25201).

scenario7() ->
    io:format("~n--- Scenario 7: gossip convergence (3-node local cluster) ---~n", []),
    Intervals = [500, 1000, 2000],
    CsvFile = "results/scenario7.csv",
    filelib:ensure_dir(CsvFile),
    {ok, Csv} = file:open(CsvFile, [write]),
    io:fwrite(Csv,
        "sync_interval_ms,samples,timeouts,p50_ms,p95_ms,p99_ms,max_ms,mean_ms~n", []),
    %% Ensure parent BEAM is distributed (peer:start_link requires it)
    ensure_distributed(),
    lists:foreach(fun(IntervalMs) ->
        io:format("~n[sync_interval_ms = ~w | ~w samples]~n", [IntervalMs, ?SAMPLES]),
        Result = run_one_interval(IntervalMs, ?SAMPLES),
        print_result(IntervalMs, Result),
        io:fwrite(Csv, "~w,~w,~w,~w,~w,~w,~w,~w~n",
            [IntervalMs, maps:get(samples, Result), maps:get(timeouts, Result),
             maps:get(p50, Result), maps:get(p95, Result),
             maps:get(p99, Result), maps:get(max, Result),
             maps:get(mean, Result)])
    end, Intervals),
    file:close(Csv),
    io:format("~n[results saved to ~s]~n", [CsvFile]),
    ok.

ensure_distributed() ->
    case node() of
        'nonode@nohost' ->
            Name = list_to_atom("bench_parent_" ++
                                integer_to_list(erlang:unique_integer([positive]))),
            {ok, _} = net_kernel:start([Name, shortnames]),
            erlang:set_cookie(node(), vordb_bench_cookie),
            ok;
        _ -> ok
    end.

%% ============================================================
%% Per-interval test: bring up a fresh 3-node cluster, run convergence loop,
%% tear down. We restart between intervals so each run starts cold.
%% ============================================================

run_one_interval(IntervalMs, NumSamples) ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    NodeNames = [list_to_atom("vordb_n" ++ integer_to_list(I) ++ "_" ++ Suffix)
                 || I <- [1, 2, 3]],
    Ports = [?BASE_PORT, ?BASE_PORT + 1, ?BASE_PORT + 2],

    io:format("[starting 3 peer nodes...]~n", []),
    Peers = lists:map(fun({Name, _Port}) ->
        {ok, Peer, Node} = start_peer(Name),
        {Peer, Node}
    end, lists:zip(NodeNames, Ports)),

    Nodes = [N || {_, N} <- Peers],
    AllNodeBins = [atom_to_binary(N, utf8) || N <- Nodes],

    %% Connect every peer to every other peer (Erlang dist mesh)
    fully_connect(Nodes),

    io:format("[setting up VorDB stack on each peer (sync_interval_ms=~w)...]~n",
              [IntervalMs]),
    lists:foreach(fun({Node, Port}) ->
        ok = setup_peer(Node, Port, IntervalMs, AllNodeBins)
    end, lists:zip(Nodes, Ports)),

    %% Let everything settle
    timer:sleep(500),

    %% Connect TCP from parent to nodes 1 and 2
    [Port1, Port2 | _] = Ports,
    {ok, S1} = gen_tcp:connect("127.0.0.1", Port1,
        [binary, {active, false}, {packet, 4}, {nodelay, true}], 5000),
    {ok, S2} = gen_tcp:connect("127.0.0.1", Port2,
        [binary, {active, false}, {packet, 4}, {nodelay, true}], 5000),

    io:format("[running ~w convergence samples...]~n", [NumSamples]),
    Lats = run_loop(S1, S2, NumSamples, []),

    gen_tcp:close(S1),
    gen_tcp:close(S2),

    %% Tear down peers
    lists:foreach(fun({Peer, _}) -> catch peer:stop(Peer) end, Peers),
    timer:sleep(200),

    compute_pcts(Lats).

start_peer(Name) ->
    %% Use default distribution. Inherit the parent's host so dist names match.
    {ok, Peer, Node} = peer:start_link(#{
        name => Name,
        wait_boot => 15000
    }),
    %% Push the parent's full code path to the child so it can find vordb modules
    Paths = code:get_path(),
    ok = erpc:call(Node, code, add_pathsa, [Paths], 5000),
    {ok, Peer, Node}.

fully_connect(Nodes) ->
    %% Make every peer ping every other peer so the dist mesh is complete
    lists:foreach(fun(A) ->
        lists:foreach(fun(B) ->
            case A =/= B of
                true -> erpc:call(A, net_adm, ping, [B], 5000);
                false -> ok
            end
        end, Nodes)
    end, Nodes).

setup_peer(Node, TcpPort, IntervalMs, AllNodeBins) ->
    %% Spawn a long-lived "holder" proc on the child that owns the links to
    %% storage / ring / vnodes / tcp. The erpc handler proc is ephemeral —
    %% if storage/ring/tcp were linked to it, they'd die when the erpc call
    %% returns. The holder survives until we explicitly stop it.
    Parent = self(),
    Result = erpc:call(Node, fun() ->
        Caller = self(),
        Holder = spawn(fun() -> holder_init(Caller, TcpPort, IntervalMs, AllNodeBins) end),
        receive
            {Holder, Reply} -> Reply
        after 30000 ->
            {error, holder_timeout}
        end
    end, 35000),
    case Result of
        {ok, NumParts} ->
            io:format("  ~p: ok (~w partitions, tcp ~w)~n", [Node, NumParts, TcpPort]),
            ok;
        Other ->
            io:format("  ~p: SETUP FAILED ~p~n", [Node, Other]),
            erlang:error(Other)
    end,
    _ = Parent,
    ok.

%% This function runs INSIDE the child node, in a long-lived holder process.
%% It owns all the linked procs (storage, ring_manager, vnodes, tcp listener)
%% so they survive past the erpc call. After bringing the stack up, it sends
%% the result to the erpc handler and then sits in a receive forever.
holder_init(ReplyTo, TcpPort, IntervalMs, AllNodeBins) ->
    process_flag(trap_exit, true),
    Reply = try
        application:ensure_all_started(telemetry),
        logger:set_primary_config(level, error),
        Suffix = atom_to_list(node()) ++ "_" ++
                 integer_to_list(erlang:unique_integer([positive])),
        DataDir = "/tmp/vordb_conv_" ++ Suffix,
        Dir = list_to_binary(DataDir),
        application:set_env(vordb, data_dir, DataDir),
        catch vordb_ffi:storage_stop(),
        {ok, _} = vordb_ffi:storage_start(Dir),
        lists:foreach(fun(P) ->
            gen_server:call(vordb_storage, {cf_create, P})
        end, lists:seq(0, 7)),
        vordb_registry:start(),
        vordb_cache:init(),
        vordb_metrics:init(),
        catch vordb_metrics:attach_handlers(),
        vordb_dirty_tracker:init(),
        SelfBin = atom_to_binary(node(), utf8),
        catch gen_server:stop(vordb_ring_manager),
        {ok, _} = vordb_ring_manager:start_link(8, 3, AllNodeBins, SelfBin),
        Parts = vordb_ring_manager:my_partitions(),
        OtherPeerBins = [B || B <- AllNodeBins, B =/= SelfBin],
        lists:foreach(fun(P) ->
            {ok, Pid} = gen_server:start_link('Elixir.Vor.Agent.KvStore',
                [{node_id, node()}, {vnode_id, P}, {sync_interval_ms, IntervalMs}], []),
            vordb_registry:register({kv_store, P}, Pid),
            vordb_dirty_tracker:init_partition(P, OtherPeerBins)
        end, Parts),
        catch vordb_tcp:stop(),
        timer:sleep(50),
        {ok, _TcpPid} = vordb_tcp:start_link(TcpPort),
        timer:sleep(100),
        {ok, length(Parts)}
    catch
        Class:Reason:Stack ->
            {error, {Class, Reason, Stack}}
    end,
    ReplyTo ! {self(), Reply},
    holder_loop().

holder_loop() ->
    receive
        stop -> ok;
        {'EXIT', _, _} -> holder_loop();
        _ -> holder_loop()
    end.

%% ============================================================
%% Convergence loop
%% ============================================================

run_loop(_S1, _S2, 0, Acc) -> Acc;
run_loop(S1, S2, N, Acc) ->
    Key = <<"conv:", (integer_to_binary(N))/binary>>,
    Value = <<"convergence_value">>,
    %% Write to node 1
    case tcp_put(S1, Key, Value) of
        {error, _} ->
            timer:sleep(?WRITE_INTERVAL_MS),
            run_loop(S1, S2, N - 1, [-1 | Acc]);
        _ ->
            T0 = erlang:monotonic_time(microsecond),
            Lat = poll_until(S2, Key, T0, ?MAX_WAIT_MS * 1000),
            timer:sleep(?WRITE_INTERVAL_MS),
            run_loop(S1, S2, N - 1, [Lat | Acc])
    end.

poll_until(Sock, Key, T0, MaxUs) ->
    case tcp_get(Sock, Key) of
        #{body := {value, _}} ->
            erlang:monotonic_time(microsecond) - T0;
        _ ->
            Elapsed = erlang:monotonic_time(microsecond) - T0,
            case Elapsed > MaxUs of
                true -> -1;
                false ->
                    timer:sleep(?POLL_INTERVAL_MS),
                    poll_until(Sock, Key, T0, MaxUs)
            end
    end.

tcp_put(Sock, Key, Value) ->
    Req = #{request_id => 1, body => {put, #{key => Key, value => Value}}},
    Bin = vordb_pb:encode_msg(Req, 'Request'),
    case gen_tcp:send(Sock, Bin) of
        ok ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, RespBin} -> vordb_pb:decode_msg(RespBin, 'Response');
                {error, E} -> {error, E}
            end;
        {error, E} -> {error, E}
    end.

tcp_get(Sock, Key) ->
    Req = #{request_id => 1, body => {get, #{key => Key}}},
    Bin = vordb_pb:encode_msg(Req, 'Request'),
    case gen_tcp:send(Sock, Bin) of
        ok ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, RespBin} -> vordb_pb:decode_msg(RespBin, 'Response');
                {error, E} -> {error, E}
            end;
        {error, E} -> {error, E}
    end.

%% ============================================================
%% Stats
%% ============================================================

compute_pcts(Lats) ->
    Total = length(Lats),
    Valid = [L || L <- Lats, L >= 0],
    Timeouts = Total - length(Valid),
    Sorted = lists:sort(Valid),
    Len = length(Sorted),
    case Len of
        0 ->
            #{samples => 0, timeouts => Timeouts,
              p50 => 0, p95 => 0, p99 => 0, max => 0, mean => 0};
        _ ->
            Sum = lists:sum(Sorted),
            #{samples => Len,
              timeouts => Timeouts,
              p50  => us_to_ms_round(pct(Sorted, Len, 0.50)),
              p95  => us_to_ms_round(pct(Sorted, Len, 0.95)),
              p99  => us_to_ms_round(pct(Sorted, Len, 0.99)),
              max  => us_to_ms_round(lists:last(Sorted)),
              mean => us_to_ms_round(Sum div Len)}
    end.

pct(Sorted, Len, Q) ->
    Idx = max(1, round(Len * Q)),
    lists:nth(Idx, Sorted).

us_to_ms_round(Us) ->
    %% Round to nearest ms; values under 1000us still report as 0 or 1
    (Us + 500) div 1000.

print_result(IntervalMs, R) ->
    io:format("  sync_interval=~wms  samples=~w  timeouts=~w~n",
              [IntervalMs, maps:get(samples, R), maps:get(timeouts, R)]),
    io:format("  p50=~wms  p95=~wms  p99=~wms  max=~wms  mean=~wms~n",
              [maps:get(p50, R), maps:get(p95, R),
               maps:get(p99, R), maps:get(max, R), maps:get(mean, R)]).
