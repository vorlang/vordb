# VorDB — Benchmark Suite

**Measure baseline performance, find limits, validate scaling properties.**

---

## Overview

The benchmark suite runs against a live VorDB cluster (local or distributed). It measures latency, throughput, convergence time, and scaling behavior. Results are printed as a summary table and optionally exported as CSV for graphing.

The suite is an Erlang application in the `bench/` directory — not part of the main VorDB build. It connects to VorDB as a client, using both HTTP and TCP protocols.

---

## Project Structure

```
bench/
├── README.md                    # How to run benchmarks
├── rebar.config                 # Erlang project (or gleam.toml if preferred)
├── src/
│   ├── vordb_bench.erl          # Main entry point — CLI + orchestration
│   ├── vordb_bench_client.erl   # Single client process (runs operation loop)
│   ├── vordb_bench_http.erl     # HTTP client (httpc or hackney)
│   ├── vordb_bench_tcp.erl      # TCP client (gen_tcp + protobuf)
│   ├── vordb_bench_stats.erl    # Latency collection + percentile computation
│   ├── vordb_bench_report.erl   # Console + CSV output
│   └── vordb_bench_convergence.erl  # Convergence time measurement
└── results/                     # CSV output directory
```

**Why a separate project:** The benchmark is a client, not part of VorDB itself. It should not share dependencies or compile paths. It exercises the same APIs external users would use.

**Why Erlang:** Benchmarks need low overhead and precise timing. Erlang gives direct access to `:erlang.monotonic_time` with nanosecond precision, `gen_tcp` for the binary protocol, and lightweight process spawning for concurrent clients. No Gleam type overhead needed — this is a measurement tool.

---

## Benchmark Scenarios

### Scenario 1: Single-Operation Latency

Measure per-operation latency at low load (1 client). Establishes the baseline — the best possible latency with no contention.

```
Parameters:
  clients: 1
  operations: 10,000
  workload: sequential PUT then GET for each key
  key pattern: "bench:{sequence_number}"
  value size: 100 bytes
  protocols: HTTP, TCP (run separately)

Output:
  PUT latency: P50, P95, P99, max (microseconds)
  GET latency: P50, P95, P99, max (microseconds)
  TCP vs HTTP comparison
```

### Scenario 2: Throughput Under Load

Measure maximum sustained throughput with increasing concurrency. Find the knee of the curve — where adding more clients stops increasing throughput.

```
Parameters:
  clients: [1, 10, 50, 100, 200, 500, 1000]
  duration: 30 seconds per concurrency level
  workload: 80% GET, 20% PUT (configurable)
  key space: 100,000 unique keys (uniform random selection)
  value size: 100 bytes
  protocol: TCP (higher throughput path)

Output per concurrency level:
  total ops/sec
  PUT ops/sec
  GET ops/sec
  PUT latency: P50, P95, P99
  GET latency: P50, P95, P99
  error rate
```

### Scenario 3: Write-Heavy Throughput

Same as Scenario 2 but with 100% writes. Stresses the write path — gen_server serialization, RocksDB WAL, column family isolation.

```
Parameters:
  clients: [1, 10, 50, 100, 200]
  duration: 30 seconds per level
  workload: 100% PUT
  key space: 1,000,000 unique keys
  value size: 100 bytes
  protocol: TCP

Output:
  write ops/sec at each concurrency
  write latency percentiles
  RocksDB write stall count (from telemetry if available)
```

### Scenario 4: Read-Heavy Throughput (ETS Validation)

Validate that the ETS read path delivers high throughput. Pre-populate data, then benchmark reads only.

```
Parameters:
  setup: pre-populate 100,000 keys
  clients: [1, 10, 50, 100, 500, 1000]
  duration: 30 seconds per level
  workload: 100% GET (keys from pre-populated set)
  protocol: TCP

Output:
  read ops/sec at each concurrency
  read latency percentiles
  Comparison note: gen_server reads would be ~X ops/sec (measure by temporarily bypassing ETS)
```

### Scenario 5: CRDT Type Comparison

Compare throughput across LWW, ORSWOT, and PN-Counter operations. Different CRDT types have different merge costs.

```
Parameters:
  clients: 50
  duration: 30 seconds per type
  workloads:
    LWW: PUT/GET
    Set: add/members (set size grows to ~100 elements)
    Counter: increment/value

Output:
  ops/sec per CRDT type
  latency percentiles per type
  merge cost comparison (from telemetry: vordb.merge.duration)
```

### Scenario 6: Value Size Impact

Measure how value size affects throughput and latency.

```
Parameters:
  clients: 50
  duration: 30 seconds per size
  workload: 80% GET, 20% PUT
  value sizes: [100B, 1KB, 10KB, 100KB, 1MB]
  protocol: TCP

Output:
  ops/sec at each value size
  latency percentiles at each value size
  bytes/sec throughput
```

### Scenario 7: Gossip Convergence

Measure how long it takes for a write on one node to become readable on another.

```
Parameters:
  cluster: 3 nodes
  method: write on node 1, poll node 2 until value appears
  sample size: 1,000 writes
  write interval: 10ms between writes (avoid overwhelming gossip)

Output:
  convergence time: P50, P95, P99, max (milliseconds)
  convergence time vs. sync_interval_ms setting
```

### Scenario 8: Cluster Scaling

Measure throughput at different cluster sizes. Validates that adding nodes adds capacity.

```
Parameters:
  cluster sizes: [1, 3, 5]  (limited by local machine for dev benchmarks)
  clients: 100 per cluster
  duration: 30 seconds
  workload: 80% GET, 20% PUT
  ring_size: 64 (smaller for local testing)
  replication_n: 3

Output:
  total cluster ops/sec at each size
  per-node ops/sec at each size
  storage per node (should decrease with more nodes)
```

### Scenario 9: Partition Balance

Verify keys are evenly distributed across partitions under load.

```
Parameters:
  operations: 100,000 PUTs with random keys
  cluster: 3 nodes, ring_size=64

Output:
  keys per partition: min, max, mean, stddev
  keys per node: min, max, mean, stddev
  histogram of partition fill levels
```

### Scenario 10: Handoff Performance

Measure time and bandwidth to transfer a partition during node join.

```
Parameters:
  cluster: 3 nodes
  pre-populate: 100,000 keys
  action: add 4th node, measure handoff

Output:
  handoff duration per partition
  total handoff time for all affected partitions
  keys transferred per second
  bytes transferred per second
  impact on read/write latency during handoff
```

---

## Implementation

### Client Process

Each benchmark client is an Erlang process running a tight loop:

```erlang
-module(vordb_bench_client).

run(Config) ->
    Protocol = maps:get(protocol, Config, tcp),
    Conn = connect(Protocol, Config),
    Workload = maps:get(workload, Config),
    Duration = maps:get(duration_ms, Config, 30000),
    KeySpace = maps:get(key_space, Config, 100000),
    ValueSize = maps:get(value_size, Config, 100),
    StatsRef = maps:get(stats_ref, Config),

    Deadline = erlang:monotonic_time(millisecond) + Duration,
    loop(Conn, Protocol, Workload, KeySpace, ValueSize, StatsRef, Deadline, 0).

loop(Conn, Protocol, Workload, KeySpace, ValueSize, StatsRef, Deadline, Count) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        true ->
            Op = pick_operation(Workload),
            Key = random_key(KeySpace),
            Start = erlang:monotonic_time(microsecond),
            Result = execute(Conn, Protocol, Op, Key, ValueSize),
            Duration = erlang:monotonic_time(microsecond) - Start,
            vordb_bench_stats:record(StatsRef, Op, Duration, Result),
            loop(Conn, Protocol, Workload, KeySpace, ValueSize, StatsRef, Deadline, Count + 1);
        false ->
            {ok, Count}
    end.

pick_operation(Workload) ->
    %% Workload = [{put, 20}, {get, 80}] — percentages
    R = rand:uniform(100),
    pick_from_distribution(Workload, R).

random_key(KeySpace) ->
    N = rand:uniform(KeySpace),
    <<"bench:", (integer_to_binary(N))/binary>>.
```

### TCP Client

```erlang
-module(vordb_bench_tcp).

connect(Host, Port) ->
    {ok, Sock} = gen_tcp:connect(Host, Port, [
        binary, {active, false}, {packet, 4},
        {send_timeout, 5000}
    ]),
    Sock.

put(Sock, Key, Value) ->
    Req = vordb_pb:encode_msg(#'Request'{
        request_id = rand:uniform(1000000),
        body = {put, #'PutRequest'{key = Key, value = Value}}
    }),
    ok = gen_tcp:send(Sock, Req),
    {ok, RespBin} = gen_tcp:recv(Sock, 0, 5000),
    vordb_pb:decode_msg(RespBin, 'Response').

get(Sock, Key) ->
    Req = vordb_pb:encode_msg(#'Request'{
        request_id = rand:uniform(1000000),
        body = {get, #'GetRequest'{key = Key}}
    }),
    ok = gen_tcp:send(Sock, Req),
    {ok, RespBin} = gen_tcp:recv(Sock, 0, 5000),
    vordb_pb:decode_msg(RespBin, 'Response').
```

### HTTP Client

```erlang
-module(vordb_bench_http).

connect(Host, Port) ->
    {Host, Port}.  %% Stateless — HTTP is request/response

put({Host, Port}, Key, Value) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/kv/~s", [Host, Port, Key])),
    Body = jsx:encode(#{<<"value">> => Value}),
    {ok, {{_, Status, _}, _, RespBody}} = httpc:request(put, {Url, [], "application/json", Body}, [], []),
    {Status, RespBody}.

get({Host, Port}, Key) ->
    Url = lists:flatten(io_lib:format("http://~s:~p/kv/~s", [Host, Port, Key])),
    {ok, {{_, Status, _}, _, RespBody}} = httpc:request(get, {Url, [], [], []}, [], []),
    {Status, RespBody}.
```

### Statistics Collection

ETS-based latency collection with histogram buckets. Lock-free — each client process writes directly.

```erlang
-module(vordb_bench_stats).

new() ->
    Ref = ets:new(bench_stats, [public, set, {write_concurrency, true}]),
    %% Initialize counters for each operation type
    lists:foreach(fun(Op) ->
        ets:insert(Ref, {{Op, count}, 0}),
        ets:insert(Ref, {{Op, errors}, 0}),
        ets:insert(Ref, {{Op, sum}, 0})
    end, [put, get, delete, set_add, set_members, counter_inc, counter_val]),
    Ref.

record(Ref, Op, DurationUs, Result) ->
    ets:update_counter(Ref, {Op, count}, 1),
    ets:update_counter(Ref, {Op, sum}, DurationUs),
    case Result of
        {error, _} -> ets:update_counter(Ref, {Op, errors}, 1);
        _ -> ok
    end,
    %% Store individual latencies for percentile calculation
    Seq = ets:update_counter(Ref, {Op, count}, 0),
    ets:insert(Ref, {{Op, latency, Seq}, DurationUs}).

percentiles(Ref, Op) ->
    %% Collect all latencies, sort, compute percentiles
    Latencies = collect_latencies(Ref, Op),
    Sorted = lists:sort(Latencies),
    Len = length(Sorted),
    case Len of
        0 -> #{p50 => 0, p95 => 0, p99 => 0, max => 0, count => 0};
        _ -> #{
            p50 => lists:nth(max(1, round(Len * 0.50)), Sorted),
            p95 => lists:nth(max(1, round(Len * 0.95)), Sorted),
            p99 => lists:nth(max(1, round(Len * 0.99)), Sorted),
            max => lists:last(Sorted),
            min => hd(Sorted),
            count => Len,
            mean => lists:sum(Sorted) div Len
        }
    end.
```

**Note on memory:** Storing every individual latency works for benchmarks up to ~1M operations. Beyond that, switch to a streaming histogram (HDR Histogram or fixed-bucket approach). For initial benchmarks, raw storage is fine.

### Report Output

```erlang
-module(vordb_bench_report).

print_summary(StatsRef, DurationSec) ->
    io:format("~n=== VorDB Benchmark Results ===~n"),
    io:format("Duration: ~p seconds~n~n", [DurationSec]),

    lists:foreach(fun(Op) ->
        Stats = vordb_bench_stats:percentiles(StatsRef, Op),
        Count = maps:get(count, Stats, 0),
        case Count > 0 of
            true ->
                OpsPerSec = Count / DurationSec,
                io:format("~-15s  ops/sec: ~.0f  P50: ~pus  P95: ~pus  P99: ~pus  max: ~pus~n",
                    [Op, OpsPerSec,
                     maps:get(p50, Stats), maps:get(p95, Stats),
                     maps:get(p99, Stats), maps:get(max, Stats)]);
            false ->
                ok
        end
    end, [put, get, delete, set_add, set_members, counter_inc, counter_val]),

    io:format("~n").

write_csv(StatsRef, DurationSec, Filename) ->
    {ok, F} = file:open(Filename, [write]),
    io:fwrite(F, "operation,count,ops_per_sec,p50_us,p95_us,p99_us,max_us,mean_us~n", []),
    lists:foreach(fun(Op) ->
        Stats = vordb_bench_stats:percentiles(StatsRef, Op),
        Count = maps:get(count, Stats, 0),
        case Count > 0 of
            true ->
                io:fwrite(F, "~s,~p,~.0f,~p,~p,~p,~p,~p~n",
                    [Op, Count, Count / DurationSec,
                     maps:get(p50, Stats), maps:get(p95, Stats),
                     maps:get(p99, Stats), maps:get(max, Stats),
                     maps:get(mean, Stats)]);
            false -> ok
        end
    end, [put, get, delete, set_add, set_members, counter_inc, counter_val]),
    file:close(F).
```

### Convergence Measurement

```erlang
-module(vordb_bench_convergence).

measure(WriteNode, ReadNode, NumSamples, WriteIntervalMs) ->
    Latencies = lists:map(fun(I) ->
        Key = <<"conv_test:", (integer_to_binary(I))/binary>>,
        Value = <<"convergence_value">>,

        %% Write to node 1
        vordb_bench_tcp:put(WriteNode, Key, Value),
        WriteTime = erlang:monotonic_time(microsecond),

        %% Poll node 2 until value appears
        ConvergeTime = poll_until_found(ReadNode, Key, WriteTime, 10000000),
        %% 10 second max wait

        timer:sleep(WriteIntervalMs),
        ConvergeTime
    end, lists:seq(1, NumSamples)),

    %% Compute percentiles
    Sorted = lists:sort([L || L <- Latencies, L > 0]),
    #{
        p50 => lists:nth(max(1, round(length(Sorted) * 0.50)), Sorted),
        p95 => lists:nth(max(1, round(length(Sorted) * 0.95)), Sorted),
        p99 => lists:nth(max(1, round(length(Sorted) * 0.99)), Sorted),
        max => lists:last(Sorted),
        samples => length(Sorted)
    }.

poll_until_found(Conn, Key, StartTime, MaxWaitUs) ->
    case vordb_bench_tcp:get(Conn, Key) of
        {ok, #'Response'{body = {value, _}}} ->
            erlang:monotonic_time(microsecond) - StartTime;
        _ ->
            Elapsed = erlang:monotonic_time(microsecond) - StartTime,
            case Elapsed > MaxWaitUs of
                true -> -1;  %% Timeout
                false ->
                    timer:sleep(1),  %% 1ms poll interval
                    poll_until_found(Conn, Key, StartTime, MaxWaitUs)
            end
    end.
```

---

## CLI Interface

```bash
# Run default benchmark (Scenario 2: throughput under load)
cd bench
make run NODES=localhost:5001

# Run against a 3-node cluster
make run NODES=localhost:5001,localhost:5002,localhost:5003

# Run specific scenario
make run SCENARIO=latency NODES=localhost:5001
make run SCENARIO=throughput NODES=localhost:5001
make run SCENARIO=write_heavy NODES=localhost:5001
make run SCENARIO=read_heavy NODES=localhost:5001
make run SCENARIO=convergence NODES=localhost:5001,localhost:5002
make run SCENARIO=scaling NODES=localhost:5001,localhost:5002,localhost:5003
make run SCENARIO=value_size NODES=localhost:5001
make run SCENARIO=crdt_types NODES=localhost:5001
make run SCENARIO=partition_balance NODES=localhost:5001
make run SCENARIO=all NODES=localhost:5001

# Custom parameters
make run SCENARIO=throughput CLIENTS=200 DURATION=60 KEY_SPACE=1000000 VALUE_SIZE=1024

# Export results to CSV
make run SCENARIO=throughput CSV=results/throughput_2026_04.csv
```

### Main Entry Point

```erlang
-module(vordb_bench).

main(Args) ->
    Config = parse_args(Args),
    Scenario = maps:get(scenario, Config, throughput),
    Nodes = maps:get(nodes, Config),

    io:format("VorDB Benchmark~n"),
    io:format("Scenario: ~p~n", [Scenario]),
    io:format("Nodes: ~p~n~n", [Nodes]),

    Results = run_scenario(Scenario, Config),
    vordb_bench_report:print_summary(Results),

    case maps:get(csv, Config, undefined) of
        undefined -> ok;
        File -> vordb_bench_report:write_csv(Results, File)
    end.

run_scenario(latency, Config) ->
    %% Scenario 1: single client, sequential ops
    run_with_clients(1, Config#{workload => [{put, 50}, {get, 50}]});

run_scenario(throughput, Config) ->
    %% Scenario 2: increasing concurrency
    ClientLevels = maps:get(client_levels, Config, [1, 10, 50, 100, 200, 500]),
    lists:foreach(fun(N) ->
        io:format("~n--- ~p clients ---~n", [N]),
        Results = run_with_clients(N, Config#{workload => [{put, 20}, {get, 80}]}),
        vordb_bench_report:print_summary(Results)
    end, ClientLevels);

run_scenario(write_heavy, Config) ->
    run_scenario(throughput, Config#{workload => [{put, 100}]});

run_scenario(read_heavy, Config) ->
    %% Pre-populate, then read-only
    io:format("Pre-populating ~p keys...~n", [maps:get(key_space, Config, 100000)]),
    populate(Config),
    run_scenario(throughput, Config#{workload => [{get, 100}]});

run_scenario(convergence, Config) ->
    [Node1, Node2 | _] = maps:get(nodes, Config),
    Conn1 = vordb_bench_tcp:connect(Node1),
    Conn2 = vordb_bench_tcp:connect(Node2),
    Results = vordb_bench_convergence:measure(Conn1, Conn2, 1000, 10),
    io:format("Convergence: P50=~pus P95=~pus P99=~pus max=~pus~n",
        [maps:get(p50, Results), maps:get(p95, Results),
         maps:get(p99, Results), maps:get(max, Results)]);

%% ... other scenarios
```

### Running Multiple Clients

```erlang
run_with_clients(NumClients, Config) ->
    StatsRef = vordb_bench_stats:new(),
    Duration = maps:get(duration_ms, Config, 30000),
    Nodes = maps:get(nodes, Config),

    %% Spawn client processes, distributed across nodes
    Pids = lists:map(fun(I) ->
        Node = lists:nth((I rem length(Nodes)) + 1, Nodes),
        spawn_monitor(fun() ->
            Conn = vordb_bench_tcp:connect(Node),
            vordb_bench_client:run(Config#{
                connection => Conn,
                stats_ref => StatsRef
            })
        end)
    end, lists:seq(1, NumClients)),

    %% Wait for all clients to finish
    lists:foreach(fun({Pid, MonRef}) ->
        receive
            {'DOWN', MonRef, process, Pid, _} -> ok
        end
    end, Pids),

    {StatsRef, Duration / 1000}.
```

---

## Expected Baseline Numbers

These are rough targets based on BEAM + RocksDB + ETS characteristics. Actual numbers will vary with hardware.

| Metric | Expected Range | Notes |
|---|---|---|
| Single PUT latency (TCP) | 50-200 μs | gen_server call + RocksDB write + ETS write |
| Single GET latency (TCP) | 10-50 μs | ETS lookup only, no gen_server |
| Single PUT latency (HTTP) | 200-1000 μs | HTTP parsing overhead |
| Single GET latency (HTTP) | 100-500 μs | HTTP overhead |
| Write throughput (1 node, TCP) | 10,000-50,000 ops/sec | Bounded by gen_server serialization per partition |
| Read throughput (1 node, TCP) | 100,000-500,000 ops/sec | ETS concurrent reads, no serialization |
| Gossip convergence (3 nodes) | 1-3 seconds | Default 1s gossip interval + processing |
| Handoff speed | 50,000-200,000 keys/sec | Bounded by Erlang distribution + RocksDB read |

**Key insight:** Read throughput should be 10-50x write throughput because reads bypass the gen_server. If the ratio is lower than 10x, the ETS read path may not be working correctly.

---

## Telemetry Integration

The benchmark should correlate its own measurements with VorDB's telemetry. After a benchmark run, scrape `/metrics` from each node and compare:

- Benchmark-measured ops/sec vs. telemetry `vordb.request.count`
- Benchmark-measured latency vs. telemetry `vordb.request.duration` histogram
- Cache hit rate from telemetry (should be ~100% for read benchmarks after warmup)
- Gossip delta sizes from telemetry during write benchmarks
- Merge duration from telemetry during convergence benchmarks

This cross-validation confirms both the benchmark and the telemetry are measuring correctly.

---

## Implementation Order

```
1. Stats module — ETS-based latency collection + percentile computation
   - Unit test: record 1000 latencies, verify percentiles are correct

2. TCP client — connect, put, get, delete
   - Verify against running VorDB instance

3. HTTP client — put, get, delete
   - Verify against running VorDB instance

4. Client process — operation loop with timing
   - Single client, fixed duration, reports stats

5. Orchestrator — spawn N clients, collect results
   - Multi-client benchmark runs

6. Report — console + CSV output
   - Formatted summary table

7. Convergence benchmark
   - Write-then-poll measurement

8. CLI — scenario selection, parameter parsing
   - make run interface

9. Run baseline benchmarks and document results
   - Record numbers for current hardware
   - Identify bottlenecks
```

---

## After First Benchmark Run

The numbers will tell you where to focus next:

- **If write throughput is low:** the gen_server per partition is the bottleneck. Options: batch writes, write-ahead buffering, or accepting eventual consistency for writes (fire-and-forget cast instead of call).
- **If read throughput is low:** ETS reads may not be working correctly, or the benchmark is bottlenecked on TCP connection handling.
- **If convergence is slow:** gossip interval too long, or merge processing is expensive. Check telemetry merge duration.
- **If TCP is not much faster than HTTP:** protobuf encoding/decoding overhead may dominate. Profile with `eprof` or `fprof`.
- **If scaling is sublinear:** coordinator forwarding overhead, or gossip bandwidth growing faster than expected. Check scoped gossip is working.

The benchmark suite is a diagnostic tool, not just a scorecard. The first run establishes baseline; subsequent runs after optimizations prove the improvement.

---

*Benchmark suite for VorDB. Separate project, exercises public APIs, measures what matters.*
