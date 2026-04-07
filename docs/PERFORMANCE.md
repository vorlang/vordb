# VorDB — Performance

This document captures measured performance characteristics of VorDB. It's
intended for someone evaluating whether VorDB fits their workload, and for
maintainers reasoning about where to spend optimization effort. Every number
here is reproducible from the bench suite under `bench/` (`make run-latency`,
`make run-throughput`, `make run-crdt`, `make run-balance`,
`make run-convergence`, `make run-eprof`).

All measurements are from a single Apple Silicon laptop. Real hardware will
likely be faster on raw throughput and similar on the architectural shapes
(convergence latency, distribution uniformity, CRDT cost ratios).

---

## Headline Result: ~2 ms Cross-Node Convergence

The most architecturally important measurement in this document is also the
most counter-intuitive. **A write on node 1 is readable on node 2 in ~2 ms,
and that latency does not change when you change the gossip interval.**

| sync_interval_ms | samples | timeouts | p50 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|---|
| 500 | 200 | 0 | **2 ms** | **2 ms** | **2 ms** | 7 ms | 2 ms |
| 1000 | 200 | 0 | **2 ms** | **2 ms** | **2 ms** | 8 ms | 2 ms |
| 2000 | 200 | 0 | **2 ms** | **2 ms** | **2 ms** | 7 ms | 2 ms |

*Setup: 3 local BEAM nodes via `peer:start_link`, ring_size=8, N=3 (full
replication). Test writes a key to node 1's TCP port, then polls node 2's
TCP port every 1 ms until the value appears, repeats 200 times with 10 ms
spacing between writes. Run separately for each `sync_interval_ms`.*

### Why is convergence independent of the gossip interval?

Because gossip is not the replication path. Look at `vordb_coordinator:write/2`:
when a write arrives, the coordinator finds the preference list for the key
(N replicas), executes the write on the local replica, and **fan-outs the
same write to the other N-1 replicas via `erpc:cast` over Erlang
distribution**. Those casts go out before the client gets its 200 OK. On a
healthy cluster they land in the remote replica's mailbox within ~1 ms.

The Vor agent's `every sync_interval_ms` block fires gossip on a timer, but
by the time gossip checks the dirty tracker, the keys have usually already
been ACKed by the receiving replica and cleared. Gossip is the **safety
net**: it propagates writes that missed the fan-out cast — node was
unreachable, message queue was full, agent crashed and restarted from disk
and needs catch-up. Gossip is not on the steady-state hot path.

This is the right shape for an AP system. Convergence is dominated by
"write happens → other replicas hear about it" latency, which over loopback
Erlang dist is sub-millisecond. The gossip interval is the **time-to-recovery
after a missed fan-out**, not the time-to-convergence of a normal write.

### Practical implications

- **Read-your-write across nodes is essentially immediate** on a healthy
  cluster. You can write to one node and read from another a millisecond
  later and expect to see the value.
- **`sync_interval_ms` is a recovery dial**, not a freshness dial. Lower it
  to recover faster from network blips; raise it to reduce background
  bandwidth on healthy clusters. The default of 1000 ms is a safe middle.
- **Network partitions** still produce divergence — that's the expected
  CRDT semantics. When the partition heals, gossip catches replicas up.
  The 2 ms number assumes both nodes are reachable.

---

## Single-Op Latency

*Setup: 1 client, 10,000 sequential operations over TCP, 100-byte values,
single in-VM node, ring_size=64.*

| operation | min | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| **PUT (TCP)** | 32 μs | **41 μs** | 94 μs | 146 μs | 479 μs |
| **GET (TCP)** | 21 μs | **31 μs** | 38 μs | 51 μs | 193 μs |

GET is roughly 25% faster than PUT at p50, dropping further at p99. The gap
exists because GET hits the ETS read cache directly (no gen_server hop, no
RocksDB) while PUT goes through the vnode gen_server, the storage gen_server,
and the RocksDB NIF including a WAL fsync. TCP framing and protobuf encode
costs are roughly equal on both paths.

A client that needs sub-millisecond p99s for both reads and writes against
small values on local storage will get them.

---

## Throughput Under Load

*Setup: variable concurrency, 80% GET / 20% PUT, 100,000-key uniform random
key space, 100-byte values, 10 seconds per concurrency level, single in-VM
node, ring_size=64.*

| clients | put/s | get/s | **total/s** | put p50 (μs) | put p99 (μs) | get p50 (μs) | get p99 (μs) | conn errors |
|---|---|---|---|---|---|---|---|---|
| 1 | 5,561 | 21,995 | **27,556** | 51 | 158 | 31 | 64 | 0 |
| 10 | 6,051 | 24,267 | **30,318** | 220 | 544 | 99 | 207 | 0 |
| 50 | 6,195 | 11,575 | **17,770** | 1,924 | 8,855 | 104 | 299 | 0 |
| 100 | 6,949 | 10,823 | **17,772** | 2,996 | 13,410 | 105 | 280 | 22 |
| 200 | 6,170 | 10,234 | **16,404** | 3,859 | 19,131 | 106 | 273 | 100 |

### What this shows

- **Read throughput is unaffected by concurrency.** GET p50 stays at ~100 μs
  from 10 clients to 200 clients. The ETS read fast-path scales horizontally
  on a single node — readers do not contend with each other or with writers.
- **Write throughput plateaus around 6–7k PUTs/sec.** Adding more clients
  past ~10 does not increase write throughput; it just increases write
  latency as the vnode and storage gen_server mailboxes deepen.
- **The knee is at ~10 concurrent clients.** Aggregate throughput peaks
  at ~30k ops/sec (6k writes + 24k reads) with 10 clients and gradually
  declines to ~16k ops/sec at 200 clients. The decline is GC and gen_server
  mailbox contention, not RocksDB stalls.
- **Connection errors** at 100+ clients are macOS's TCP listen backlog
  (`SOMAXCONN`=128) being exceeded by simultaneous `gen_tcp:connect` calls
  from the bench. This is a bench-side artifact, not a server limit.

### Why writes plateau

The eprof scenario (`make run-eprof`) at 100 concurrent clients shows that
the busiest gen_server is `vordb_storage` (~155k receives over 10 s), with
the kvstore vnodes at ~40k receives each. RocksDB itself uses only ~6% of
CPU time; the bulk of the work is gen_server message dispatch, monitor
setup/teardown, and BEAM↔NIF port commands.

A persistent_term + direct-rocksdb experiment (see Optimization History
below) attempted to remove the storage gen_server. It made things **worse**:
RocksDB's internal WAL/memtable mutexes contend badly under fan-out from
many vnode processes. The gen_server is acting as a contention buffer; it
makes the path serial, which is faster than parallel-with-internal-locks
in this configuration.

### How VorDB scales past the per-node ceiling

By adding nodes, not by saturating one. Each node has its own
`vordb_storage` gen_server and its own RocksDB instance with its own WAL.
With N=3 replication, aggregate cluster effective write throughput is
roughly `nodes × per_node × 1/N`. On 5 nodes that's ~28k effective
writes/sec; on 10 nodes ~57k. The ring distributes load uniformly (see
Partition Balance below), so per-node load is even. The right answer to
"I need more write throughput" is "add a node," not "make this node
faster."

---

## CRDT Type Cost Comparison

*Setup: 50 clients × 10 seconds per type, 50/50 read/write split, 200-key
space (small enough that ORSWOT sets reach realistic sizes; pre-grown to
100 elements per key before measurement), single in-VM node.*

| CRDT type | write op | read op | write/s | read/s | write p50 | write p99 | read p50 | read p99 |
|---|---|---|---|---|---|---|---|---|
| **LWW-Register** | put | get | **9,293** | **9,340** | 465 μs | 8,684 μs | **77 μs** | 174 μs |
| **PN-Counter** | increment | value | **8,245** | **8,733** | 472 μs | 8,905 μs | **80 μs** | 177 μs |
| **ORSWOT (Set)** | set_add | set_members | **5,105** | **4,054** | 967 μs | **43,334 μs** | **229 μs** | 509 μs |

### User guidance

**LWW and PN-Counter are operationally equivalent.** Throughput, write
latency, and read latency are within noise of each other across both types.
PN-Counter's merge (per-node max on the increment and decrement maps) is
cheap and bounded by the number of nodes that have ever touched the
counter, not the number of operations. Pick PN-Counter for any
"how many things happened" workload without performance concern.

**ORSWOT is roughly 2× slower on writes and 5× higher tail latency.**
The cost is real and worth understanding before choosing it:

- Each `set_add` walks the existing dot map, allocates a new dot, updates
  the version vector, and re-serializes the whole CRDT state. With 100
  elements per key the state is meaningfully larger than an LWW entry, so
  every mutation copies more memory and triggers more GC.
- `set_members` walks the dot map to materialize the present elements;
  reads scale with element count.
- The 43 ms write p99 outlier suggests occasional GC pauses on large set
  state. Larger sets will have larger pauses.

**When to use which:**
- **LWW-Register** — strings, JSON blobs, opaque values, configuration,
  user profile fields, anything where "last writer wins" is correct
  semantics. Cheapest CRDT type. Use this by default.
- **PN-Counter** — counters of any kind: page views, likes, retries,
  inventory deltas. Effectively the same cost as LWW. Use freely.
- **ORSWOT (Set)** — when you need set semantics with concurrent
  add/remove and add-wins resolution. Tag clouds, group memberships,
  follower lists, dedup buckets. Plan for 2× the write cost and watch
  the maximum element count per key — keep sets bounded (a few hundred
  elements is fine, tens of thousands will hurt).

---

## Partition Balance

*Setup: 100,000 PUTs with random keys via TCP, ring_size=8, single node,
20 concurrent writers.*

| metric | value |
|---|---|
| total keys | 100,000 |
| ideal/partition | 12,500 |
| min | 12,230 |
| max | 12,595 |
| mean | 12,500.0 |
| stddev | **109.4** |
| stddev as % of mean | **0.9%** |
| max/min ratio | **1.030** |

```
P000 |  12574  #######################################
P001 |  12483  #######################################
P002 |  12230  ######################################
P003 |  12519  #######################################
P004 |  12495  #######################################
P005 |  12518  #######################################
P006 |  12595  ########################################
P007 |  12586  #######################################
```

The hash function (`erlang:phash2/2`) distributes random keys across
partitions almost perfectly. Stddev is under 1% of the mean and the
max/min ratio is 1.03. For comparison, a uniform Bernoulli over 100k
trials and 8 buckets has expected stddev `sqrt(12500) ≈ 112` — measured
stddev is 109.4, indistinguishable from ideal random distribution.

**Implication:** under uniform random key access, no partition is hot.
Hot partitions can still arise from non-uniform key access patterns
(everyone hammering the same key) — that is workload-dependent and not
something the ring can fix. But the hash itself is not introducing skew.

---

## Optimization History

VorDB's performance characteristics are not accidental. This section
records what was tried, what worked, and what was reverted, so future
contributors don't repeat the dead-end experiments.

### Win: ring state in `persistent_term`

**Before:** `vordb_ring_manager` was a gen_server. Every read operation
called `vordb_ring_manager:key_partition/1`,
`vordb_ring_manager:key_nodes/1`, `vordb_ring_manager:is_local/1`,
`vordb_ring_manager:self_node/0` — all gen_server calls. eprof at 100
concurrent clients showed `vordb_ring_manager` receiving **1.86 million
messages in 10 seconds** (~186k/s) while the system was serving only
~17k requests/sec — meaning every request was making ~12 ring_manager
calls, each a gen_server round-trip.

**Change:** the ring data structure, the self-node binary, and the
"my partitions" list now live in `persistent_term`. The gen_server
still owns *writes* (membership changes write the new ring through
`gen_server:call`) but readers call `persistent_term:get/1` and compute
the partition or preference list locally. No IPC, no serialization,
no contention. See `src/vordb_ring_manager.erl`.

**Result:**
- `vordb_ring_manager` dropped out of the top 10 of both functions-by-time
  and processes-by-message-count in eprof.
- `gen:do_call/4` calls dropped from 2.0M to 0.31M — a 6.6× reduction in
  total gen_server round-trips.
- GET p50 at 100 clients dropped from 175 μs to 110 μs (-37%).
- GET p50 at 200 clients dropped from 175 μs to 112 μs (-36%).
- 10-client throughput improved from ~26k to ~30k ops/sec (+15%).
- Write throughput plateau did *not* lift — it had been masked by the
  ring_manager bottleneck and was actually limited by the next layer down
  (`vordb_storage`), which is what motivated the next experiment.

### Loss (reverted): storage handle in `persistent_term`

**Hypothesis:** if removing the gen_server in front of ring state was a
win, removing the gen_server in front of storage state should also be a
win. Each vnode would call `rocksdb:put/get/delete` directly via handles
read from `persistent_term`, instead of routing through the
`vordb_storage` gen_server.

**Implementation:** the storage gen_server still owned init/terminate
and CF lifecycle (cf_create, cf_drop), and republished the CF handle map
to `persistent_term` on mutation. All read/write operations bypassed it.

**Result:** Throughput **regressed** at every concurrency level above 1:

| clients | total/s before | total/s after PT-storage | delta |
|---|---|---|---|
| 1 | 26,340 | 30,502 | +16% (good) |
| 10 | 30,510 | 24,886 | **−18%** |
| 50 | 19,136 | 15,754 | **−18%** |
| 100 | 16,551 | 10,596 | **−36%** |
| 200 | 16,579 | 10,912 | **−34%** |

**Why:** RocksDB has its own internal mutexes — on the WAL, on the
memtable insert path, on the column family handle map. Under fan-out
from 64 concurrent vnode processes, these mutexes contend badly. The
`vordb_storage` gen_server, by serializing writes through a single
mailbox, was acting as **an unintentional contention buffer**. It made
the path serial, but serial RocksDB writes turned out to be faster than
parallel-with-internal-locks at this fan-out factor.

This is a class of performance gotcha worth remembering: **a queue can
be faster than no queue when the resource you are queueing in front of
has its own internal contention.**

**Decision:** reverted. The storage gen_server stays. The single-node
write ceiling at ~17k ops/sec is the fundamental constraint of this
architecture on this hardware, and lifting it requires either:

1. Per-vnode RocksDB instances (each vnode opens its own DB so there
   is no shared WAL or shared mutex). Higher disk overhead, more
   file handles, isolated failure domains. The TiKV approach.
2. A small writer pool (e.g. 4–8 worker procs sharded by partition).
   Reduces fan-out from 64-wide to pool-wide, may sit between the
   current ceiling and the unbounded-concurrency regression.
3. Group commit / batched writes. Coalesce concurrent `rocksdb:put`
   calls into a single `rocksdb:write` batch within a short window.
   Amortizes WAL fsync cost.

None of these are needed today. The horizontal scaling story
(add nodes, each with its own storage stack) makes the per-node
ceiling much less interesting than it sounds.

### Win: precomputed atom in metrics handler

**Before:** `vordb_metrics:handle_counter_event/4` constructed a counter
name on every event by:
```erlang
list_to_atom(lists:flatten(lists:join("_", [atom_to_list(E) || E <- Event])))
```

For a `[vordb, cache, hit]` event, this builds the per-element atom
strings, joins them with underscore, flattens the result, and atomizes
it — *every cache hit*. eprof showed `lists:do_flatten/2` at **18.99
million calls in 10 seconds**, consuming 4.67% of CPU time.

**Change:** the precomputed atom name (`vordb_cache_hit`,
`vordb_cache_miss`, `vordb_gossip_delta_sent`) is passed in the
`telemetry:attach/4` config map and read from there in the handler.
Zero runtime atom construction.

**Result:** small but real reduction in per-request overhead. The
optimization is preserved because the cost was unjustified — the
counter name is constant per attached handler, there is no reason to
recompute it.

---

## Reproducing These Numbers

```bash
cd bench

# Single-op latency (~1 minute)
make run-latency

# Throughput sweep across concurrency levels (~1.5 minutes)
make run-throughput

# CRDT type comparison, 30 seconds per type plus pre-grow (~45 seconds)
make run-crdt

# Partition balance, 100k random PUTs and per-partition counts (~10 seconds)
make run-balance

# 3-node convergence, takes ~3 minutes total because it boots a fresh
# 3-peer cluster for each sync_interval_ms value
make run-convergence

# CPU profiling and process message-count tracing (~30 seconds)
make run-eprof
```

CSVs land in `bench/results/`. The bench is intentionally a separate
project (not in `src/`) so it shares no compile path with VorDB itself —
it exercises the same TCP and HTTP APIs an external client would use.

---

## What This Document Doesn't Cover

These are real questions VorDB has not measured yet. Listing them so
nobody mistakes the present document for the whole story:

- **Multi-region latency.** All measurements are on a single laptop with
  loopback networking. Real cross-region replication will have wider
  fan-out and convergence numbers — bounded below by network RTT.
- **Sustained workloads beyond 30 seconds.** All scenarios are short
  bursts. Compaction stalls, RocksDB write amplification, and memory
  steady-state behavior are not exercised here.
- **Cluster scaling on real hardware.** The "scales horizontally by
  adding nodes" claim is architectural reasoning, not measurement. A
  real multi-node throughput sweep is the next benchmark to add.
- **Handoff under load.** Partition transfer during node join was not
  exercised in this round.
- **Failure scenarios.** Convergence after a node restart, after a
  network partition, after a process crash. These are integration tests
  but not benchmarks yet.
- **Large value sizes.** All tests use 100-byte values. Throughput will
  shift toward bandwidth and away from per-op overhead at larger sizes.

The bench suite at `bench/README.md` lists scenarios 6, 8, and 10
as "to do" — those cover most of the gaps above.
