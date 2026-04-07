# VorDB — Project Overview

**A CRDT-based distributed key-value store with verified coordination.**

This document describes VorDB as it exists today. It is the single reference for understanding the complete system — architecture, data flow, module responsibilities, deployment model, and current status. Written for new contributors (human or AI).

---

## What VorDB Is

VorDB is a distributed database where:

- Every node accepts writes — no leader, no election, no consensus protocol
- Conflicts resolve automatically using CRDTs (Conflict-Free Replicated Data Types)
- The coordination layer is formally verified at compile time via Vor
- Data is partitioned across nodes via a consistent hashing ring
- Each key is replicated to N nodes for fault tolerance
- Writes fan out synchronously to all replicas at request time; convergence is typically ~2 ms across a 3-node loopback cluster (see [PERFORMANCE.md](./PERFORMANCE.md)). Gossip is the safety net that catches up replicas when a fan-out is dropped or a node was unreachable, not the primary replication path.
- Availability is prioritized over immediate consistency: a write succeeds as soon as the local (or primary) replica acknowledges; remote replicas are reconciled by fan-out cast and, if needed, by gossip.

VorDB is the spiritual successor to Riak. Riak proved that CRDT-based distributed storage on the BEAM works at production scale. VorDB adds what Riak never had: compile-time verification of the coordination layer.

---

## Language Stack

| Layer | Language | Role |
|---|---|---|
| Coordination | Vor | State machine, CRDT merge, gossip timing. Compile-time verified. |
| Application | Gleam | All application logic. Statically typed, exhaustive pattern matching. |
| FFI bridges | Erlang | Thin bridges between Gleam, Vor agent, RocksDB, and BEAM primitives. |
| Persistence | RocksDB (C++) | Local storage via NIF bindings. |
| Runtime | BEAM/OTP | Process isolation, Erlang distribution, supervision, fault tolerance. |

**Why three languages:** Each covers a different safety concern. Vor verifies coordination correctness (the most dangerous layer for bugs). Gleam catches type errors in application logic at compile time (prevents the class of bugs AI coding agents most commonly produce). Erlang provides direct access to BEAM primitives where typed wrappers add friction without value.

---

## Architecture

```
Client
  │
  ├── HTTP REST (mist) ──────────────────────────┐
  │                                               │
  └── TCP Binary (protobuf, gen_tcp {packet,4}) ──┤
                                                   │
                                                   ▼
                                            Coordinator
                                     (routes by ring partition)
                                                   │
                        ┌──────────────────────────┼──────────────────────────┐
                        ▼                          ▼                          ▼
                   Local vnode              Remote vnode               Remote vnode
                   (if replica)          (async fan-out)           (async fan-out)
                        │
                        ▼
              Vor Agent (KvStore)
              ┌─────────────────────┐
              │ LWW merge (:lww)    │ ← compile-time verified
              │ ORSWOT merge        │ ← property-tested extern
              │ PN-Counter merge    │ ← property-tested extern
              │ Gossip timing       │ ← every block fires extern
              │ State recovery      │ ← on :init loads from storage
              └─────────────────────┘
                   │           │
                   ▼           ▼
              ETS Cache    RocksDB
              (reads)    (persistence)
                         per-partition
                        column families
```

### Request Flow — Write

1. Client sends PUT to any node (HTTP or TCP)
2. Coordinator computes partition: `hash(key) rem 256`
3. Coordinator looks up preference list from ring: N nodes that hold this key
4. If this node is a replica: synchronous local write to Vor agent
5. Fan out to remaining N-1 replicas via `erpc:cast` over Erlang distribution. The casts go out before the client response — they are **the actual replication mechanism**, not gossip. On a healthy cluster, replicas receive the write within ~1 ms of the local commit.
6. Vor agent processes write: creates LWW entry with timestamp, transitions state
7. Agent persists to RocksDB (column family for this partition)
8. Agent writes through to ETS cache
9. Agent marks key dirty in ETS DirtyTracker. If the fan-out cast was delivered (the common case) the key will be cleared from the dirty set on the next ACK. The dirty mark exists so gossip can catch up replicas that missed the cast.
10. Response returned to client after local (or primary) write succeeds

### Request Flow — Read

1. Client sends GET to any node
2. Coordinator computes partition
3. If this node is a replica: read directly from ETS cache (gen_server not involved)
4. If not a replica: forward to a replica node, which reads from its ETS cache
5. Filter tombstones (deleted keys return not_found)
6. Return value to client

### Gossip Flow

Gossip is the **safety net** that catches up replicas which missed a fan-out cast. On a healthy cluster, fan-out delivers the write to all replicas within ~1 ms; gossip's job is the long tail — node was unreachable, cast was dropped, message queue was overloaded, agent crashed and restarted from disk and is now stale. Gossip is *not* on the steady-state hot path.

1. Each vnode's Vor agent fires its `every sync_interval_ms` timer
2. Timer calls gossip extern: `send_vnode_deltas(partition, node_id)`
3. Gossip module queries ETS DirtyTracker: which keys are still dirty (un-ACKed) for each replica peer?
4. For each peer with dirty keys: fetch entries from Vor agent, send delta via Erlang distribution
5. Receiving vnode's Vor agent processes `{:lww_sync, ...}` (or set/counter sync): merges via CRDT
6. Agent persists merged state, updates ETS cache, marks dirty for further propagation
7. Peer sends ACK; sender clears confirmed dirty entries
8. Unacknowledged deltas retry on next gossip tick

This means `sync_interval_ms` controls **how fast missed fan-outs are reconciled**, not how fast typical writes propagate. Lowering it speeds up convergence after failures; raising it reduces background bandwidth. Measured convergence on a 3-node loopback cluster is ~2 ms p99 across 500 ms / 1000 ms / 2000 ms intervals — flat, because almost no writes need gossip to converge. See [PERFORMANCE.md](./PERFORMANCE.md) for the full numbers.

### Ring Change Flow

1. Node join or leave triggers membership change
2. Membership module computes new ring: `Ring.add_node` or `Ring.remove_node`
3. Ring manager updates local ring, persists to RocksDB default column family
4. Ring broadcast to all cluster members (immediate) + periodic ring gossip (backup)
5. Each node computes diff: which partitions gained, which lost
6. Gained partitions: create column family, start new vnode
7. Lost partitions: initiate handoff (stream data to new owner), then stop vnode and drop column family
8. DirtyTracker updates peer sets for affected partitions
9. All nodes converge on same ring version

---

## CRDT Types

### LWW-Register (Last-Writer-Wins)

Simple key-value. Each entry: `{value, timestamp, node_id}`. Concurrent writes resolved by highest timestamp; ties broken by node_id. Deletes are tombstones.

**Merge:** Vor-native `map_merge(:lww)` — compile-time verified.

**API:** `PUT /kv/:key`, `GET /kv/:key`, `DELETE /kv/:key`

### ORSWOT (Optimized OR-Set Without Tombstones)

Set with add/remove. Concurrent add and remove resolves in favor of add (add-wins). Uses vector clock (dot-based tracking) instead of tombstones — space is O(elements × nodes) regardless of operation count.

**Merge:** Gleam extern — property-tested (commutativity, associativity, idempotency).

**API:** `POST /set/:key/add`, `POST /set/:key/remove`, `GET /set/:key`

### PN-Counter (Positive-Negative Counter)

Increment and decrement counter. Implemented as pair of G-Counters (positive and negative). Value = sum(P) - sum(N). Each node maintains its own counts.

**Merge:** Gleam extern — property-tested. Inner operation is max per node per side.

**API:** `POST /counter/:key/increment`, `POST /counter/:key/decrement`, `GET /counter/:key`

---

## Data Distribution

### Consistent Hashing Ring

- Fixed ring size (default 256 partitions, configurable, immutable after cluster creation)
- Each partition assigned to a node via sorted node list modulo
- Ring shared across all nodes via ring gossip (periodic + event-driven)
- Ring versioned with monotonic integer; higher version wins on conflict

### Replication

- Each key replicated to N nodes (default 3)
- Preference list: primary owner + N-1 clockwise neighbors on ring
- With 3 nodes and N=3: full replication (every node has everything)
- With 4+ nodes and N=3: partial replication (each node holds ~3/cluster_size of data)

### Vnodes

- Each node runs one Vor agent process per owned partition
- A node owning 64 partitions runs 64 vnode processes
- Each vnode is an independent failure domain — crash affects only its partition
- Vnodes register in OTP Registry keyed by partition number

---

## Storage

### RocksDB

One RocksDB database per node. Per-partition column families provide write isolation:

```
Column families:
  "default"          → cluster metadata (ring, node config)
  "partition_000"    → all data for partition 0
  "partition_042"    → all data for partition 42
  ...
```

Keys within a column family: `lww:mykey`, `set:mykey`, `counter:mykey` (type prefix only, no partition prefix).

Column family lifecycle tied to partition ownership — created on partition gain, dropped after handoff complete.

### ETS Cache

One ETS table (`vordb_cache`) per node. Write-through from Vor agent on every mutation. Reads served directly from ETS — gen_server not involved.

```
Key: {partition, type_atom, key_binary}
Value: CRDT state
```

Populated on vnode startup from RocksDB. Cleared on vnode stop. Not persisted — RocksDB is source of truth.

### Serialization

- RocksDB values: `term_to_binary` (Erlang term format). Protobuf storage migration deferred.
- TCP wire protocol: Protocol Buffers with length-prefixed framing.
- Internal gossip: Erlang distribution (native term passing).

---

## The Vor Agent

`src/vor/kv_store.vor` — the heart of VorDB. Unchanged from Phase 0 through Phase 3.

The agent is a gen_server parameterized with `node_id`, `vnode_id`, and `sync_interval_ms`. It manages three state fields (`lww_store`, `set_store`, `counter_store`), each a map of keys to CRDT state.

**What the agent handles:**
- Client operations (put, get, delete, set_add, set_remove, counter_increment, counter_decrement)
- Sync messages from gossip (lww_sync, set_sync, counter_sync) — triggers CRDT merge
- Periodic gossip timer (`every` block calls extern to send deltas)
- State recovery (`on :init` loads from RocksDB and populates ETS cache)

**What the agent delegates via externs:**
- RocksDB persistence (storage module)
- ETS cache writes (cache module)
- Dirty tracking (DirtyTracker ETS)
- Gossip dispatch (gossip module)
- Entry construction (Entry helpers)
- ORSWOT and PN-Counter merge (Gleam CRDT modules)
- Timestamp generation (`:erlang.system_time`)

**What the agent doesn't know about:**
- The ring, partition assignment, or cluster topology
- Which node is primary, which are replicas
- Handoff, ring gossip, or membership changes
- HTTP or TCP protocols
- ETS reads (reads bypass the agent entirely)

The agent is a pure CRDT state machine. Everything distributed is built around it without its knowledge.

---

## Modules

### Vor (compiled to BEAM)

| Module | File | Role |
|---|---|---|
| Vor.Agent.KvStore | `src/vor/kv_store.vor` | CRDT coordination — merge, state, gossip timing |

### Gleam (statically typed application code)

| Module | File | Role |
|---|---|---|
| vordb | `src/vordb.gleam` | Public API |
| vordb/types | `src/vordb/types.gleam` | Core type definitions |
| vordb/entry | `src/vordb/entry.gleam` | LWW entry construction |
| vordb/or_set | `src/vordb/or_set.gleam` | ORSWOT implementation |
| vordb/counter | `src/vordb/counter.gleam` | PN-Counter implementation |
| vordb/ring | `src/vordb/ring.gleam` | Ring data structure — partition assignment, preference lists, diffing |
| vordb/protobuf | `src/vordb/protobuf.gleam` | Protobuf encode/decode wrappers |
| vordb/serializer | `src/vordb/serializer.gleam` | Storage serialization |
| vordb/map_utils | `src/vordb/map_utils.gleam` | Map key selection helpers |
| vordb/storage | `src/vordb/storage.gleam` | RocksDB typed interface |
| vordb/vnode_router | `src/vordb/vnode_router.gleam` | Key-to-partition routing |
| vordb/cluster | `src/vordb/cluster.gleam` | Peer discovery |
| vordb/gossip | `src/vordb/gossip.gleam` | Gossip dispatch interface |
| vordb/dirty_tracker | `src/vordb/dirty_tracker.gleam` | ETS dirty tracking interface |
| vordb/http_router | `src/vordb/http_router.gleam` | HTTP REST API |
| vordb/metrics | `src/vordb/metrics.gleam` | Telemetry emission helpers |
| vordb/handoff | `src/vordb/handoff.gleam` | Handoff typed interface |

### Erlang (FFI bridges and services)

| Module | File | Role |
|---|---|---|
| vordb_ffi | `src/vordb_ffi.erl` | Main FFI: storage, CRDT helpers, Vor agent calls |
| vordb_cache | `src/vordb_cache.erl` | ETS read cache |
| vordb_dirty_ffi | `src/vordb_dirty_ffi.erl` | ETS dirty tracking operations |
| vordb_ring_manager | `src/vordb_ring_manager.erl` | Ring state GenServer |
| vordb_ring_gossip | `src/vordb_ring_gossip.erl` | Ring distribution (periodic + broadcast) |
| vordb_coordinator | `src/vordb_coordinator.erl` | Write fan-out, read routing |
| vordb_handoff | `src/vordb_handoff.erl` | Partition data transfer |
| vordb_tcp | `src/vordb_tcp.erl` | Binary TCP server |
| vordb_metrics | `src/vordb_metrics.erl` | Telemetry handlers, Prometheus formatter |
| vordb_pb | `src/vordb_pb.erl` | Generated protobuf encode/decode |

### Proto

| File | Role |
|---|---|
| `proto/vordb.proto` | Protocol Buffer schema for CRDT types and client protocol |

---

## Supervision Tree

```
VorDB.Application
  └── VorDB.Supervisor (one_for_one)
        ├── VorDB.Metrics             # Telemetry ETS + handler registration
        ├── VorDB.Storage             # RocksDB (column families)
        ├── VorDB.Cluster             # Peer discovery
        ├── VorDB.RingManager         # Ring state + persistence
        ├── VorDB.DirtyTracker (ETS)  # Init ETS tables (no GenServer)
        ├── VorDB.VnodeRegistry       # OTP Registry for vnode lookup
        ├── VorDB.VnodeSupervisor     # Dynamic — starts/stops vnodes per ring
        │     ├── {:kv_store, 0}      # Vor agent instances
        │     ├── {:kv_store, 3}
        │     ├── ...
        │     └── {:kv_store, 251}
        ├── VorDB.Handoff             # Partition transfer manager
        ├── VorDB.RingGossip          # Ring distribution
        ├── VorDB.Gossip              # Full-state fallback timer
        ├── VorDB.Membership          # Join/leave + ring cascade
        ├── VorDB.TcpServer           # Binary protocol (protobuf)
        └── HTTP (mist)               # REST API + admin + metrics
```

Start order matters — each child depends on earlier children being available.

---

## Client Protocols

### HTTP REST

| Method | Endpoint | Description |
|---|---|---|
| `PUT /kv/:key` | Write LWW value | Body: `{"value": "..."}` |
| `GET /kv/:key` | Read LWW value | Returns value or 404 |
| `DELETE /kv/:key` | Delete LWW key | Tombstone write |
| `POST /set/:key/add` | Add set element | Body: `{"element": "..."}` |
| `POST /set/:key/remove` | Remove set element | Body: `{"element": "..."}` |
| `GET /set/:key` | List set members | Returns member list or 404 |
| `POST /counter/:key/increment` | Increment counter | Body: `{"amount": 1}` (optional) |
| `POST /counter/:key/decrement` | Decrement counter | Body: `{"amount": 1}` (optional) |
| `GET /counter/:key` | Get counter value | Returns integer value or 404 |
| `GET /status` | Node status | Node info, peers, vnodes |
| `POST /admin/join` | Join cluster | Body: `{"seed_node": "..."}` |
| `POST /admin/leave` | Leave cluster | |
| `GET /admin/members` | View membership | |
| `POST /admin/full-sync` | Trigger full-state sync | On-demand recovery |
| `GET /metrics` | Prometheus metrics | Scrape endpoint |

### TCP Binary Protocol

Length-prefixed protobuf over TCP. Port configurable (default 5001).

```
[4 bytes BE length][protobuf-encoded Request]
→
[4 bytes BE length][protobuf-encoded Response]
```

Supports all CRDT operations. `request_id` field enables request-response correlation. Erlang's `{packet, 4}` handles framing.

---

## Deployment Model

### Single Node (Development)

```bash
gleam build
elixir --sname node1 -S mix run --no-halt
# Or via Makefile: make run
```

One node runs all 256 partitions. HTTP on 4001, TCP on 5001.

### Multi-Node Cluster

```bash
# Node 1 (seed)
VORDB_NODE_ID=node1 VORDB_HTTP_PORT=4001 VORDB_TCP_PORT=5001 \
  elixir --sname node1 -S mix run --no-halt

# Node 2 (joins via seed)
VORDB_NODE_ID=node2 VORDB_HTTP_PORT=4002 VORDB_TCP_PORT=5002 \
  VORDB_PEERS=node1@localhost elixir --sname node2 -S mix run --no-halt

# Or join dynamically:
curl -X POST http://localhost:4002/admin/join \
  -H "Content-Type: application/json" \
  -d '{"seed_node": "node1@localhost"}'
```

### Multi-Region

```
Azure East US        Azure West US       Azure Europe
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ VorDB Node 1 │    │ VorDB Node 2 │    │ VorDB Node 3 │
│ BEAM + RocksDB│◄──►│ BEAM + RocksDB│◄──►│ BEAM + RocksDB│
└──────────────┘    └──────────────┘    └──────────────┘
```

Each node is an independent BEAM application with its own RocksDB. The ring distributes partitions across nodes. Gossip propagates via Erlang distribution. RocksDB instances never communicate directly.

Region-aware replica placement (ensuring replicas span regions) is a future feature.

---

## Configuration

```toml
# Node identity
node_id = "node1"

# Networking
http_port = 4001
tcp_port = 5001

# Cluster
peers = []                      # Static peers for bootstrap
seed_nodes = []                 # Seed nodes for dynamic join

# Ring
ring_size = 256                 # Partitions (immutable after cluster creation)
replication_n = 3               # Replicas per key

# Gossip
sync_interval_ms = 1000         # Delta gossip interval per vnode
ring_gossip_interval_ms = 5000  # Ring gossip interval

# Storage
data_dir = "data/{node_id}"
cf_write_buffer_size_mb = 4     # Memtable per column family
block_cache_size_mb = 512       # Shared block cache

# TCP
tcp_max_connections = 1000
tcp_max_message_size_mb = 16

# Operations
fresh_start = false             # Delete data on startup (dev only)
metrics_enabled = true
```

---

## Testing

```bash
# Unit + property tests
make test

# Include integration tests
make test-integration
```

### Test Coverage

| Category | Tests | What's verified |
|---|---|---|
| Pure CRDT modules | ~30 | ORSWOT, PN-Counter, LWW entry logic |
| Storage | ~6 | RocksDB round-trip, column families, partition isolation |
| KvStore agent | ~9 | CRUD through Vor agent, sync/merge, state recovery |
| HTTP endpoints | ~16 | All REST routes, error handling |
| TCP protocol | ~8 | All binary protocol operations, framing, error handling |
| DirtyTracker | ~7 | ETS dirty tracking, ACKs, partition lifecycle |
| Ring | ~26 | Construction, partitioning, preference lists, diffing, serialization |
| Metrics | TBD | Telemetry emission, Prometheus format |
| Property suites | 15 | CRDT merge commutativity, associativity, idempotency (×3 types, ×5 properties) |
| Integration | ~11 | Multi-node gossip, convergence, restart, partition recovery |

### Property Tests

The most critical tests. Each CRDT merge function is verified for three mathematical properties that every CRDT must satisfy:

- **Commutativity:** `merge(a, b) == merge(b, a)` — order doesn't matter
- **Associativity:** `merge(merge(a, b), c) == merge(a, merge(b, c))` — grouping doesn't matter
- **Idempotency:** `merge(a, a) == a` — merging with self is a no-op

These run 30-50 random iterations each, generating random CRDT states and verifying the properties hold.

---

## Build System

```makefile
# Compile protobuf schema
make proto

# Compile Vor agent + Gleam + Erlang
make build

# Run tests
make test

# Run with default config
make run
```

Build order: proto → Vor compilation → Gleam build (compiles .gleam + .erl files).

---

## Outstanding Issues

### Vor Language Gaps (VOR_GAPS.md)

| Gap | Status | Impact |
|---|---|---|
| GAP-005 | Open (Low) | Map literal syntax — cosmetic |
| GAP-010 | Open (Low) | Emit field name collision — cosmetic |
| GAP-013 | Open (Medium) | OR-Set merge not Vor-native — property-tested workaround |
| GAP-014 | Open (Medium) | PN-Counter merge not Vor-native — property-tested workaround |

GAP-013 and GAP-014 are blocked on Vor gaining map iteration capabilities. The verification story is: LWW merge is compile-time verified, OR-Set and PN-Counter merge are property-tested. All three are correct; two are verified at different levels.

### Scaling Concerns (SCALING_DEBT.md)

| Item | Status | Severity |
|---|---|---|
| SCALE-001 | Open (Medium) | Merge in externs — same as GAP-013/014 |
| SCALE-006 | Open (Low) | term_to_binary for storage — blocks cross-language RocksDB tools |
| SCALE-007 | Open (Low) | No type enforcement per key — Redis model, arguably correct |
| SCALE-011 | Open (Low) | Hot partitions — operational tuning |
| SCALE-012 | Open (Low) | Ring churn — rate-limit ring changes |
| SCALE-013 | Open (Low) | Handoff bandwidth — throttle when needed |

No architectural blockers. All remaining items are operational tuning or Vor language maturity.

---

## Project History

| Phase | Milestone | Key Deliverable |
|---|---|---|
| Phase 0 | "It works on my laptop" | LWW-Register, 3 nodes, full-state gossip, REST API, RocksDB |
| Phase 1 | "It's a real database" | OR-Set, PN-Counter, delta gossip with ACKs, vnode sharding, cluster membership |
| Gleam migration | Type safety for AI contributors | All Elixir replaced with Gleam + Erlang FFI |
| Phase 2 | "It scales" | Consistent hashing ring, partial replication, replicated writes, handoff, ring gossip |
| Phase 3 | "It's production-ready" | ORSWOT, ETS reads, column families, ETS DirtyTracker, protobuf TCP, telemetry |

The Vor agent at the core is unchanged from Phase 0 through Phase 3. Every phase added infrastructure around it.

---

## Intellectual Heritage

- **Riak** — CRDT-based distributed storage on BEAM. Proved the model works at scale. Business failure, not technology failure. VorDB fills the empty chair.
- **Amazon Dynamo** — consistent hashing, preference lists, sloppy quorum, vector clocks. VorDB's ring and replication model.
- **Vor** — BEAM-native language with compile-time verification. VorDB's coordination layer.
- **RocksDB** — LSM-tree storage engine. Write-optimized, battle-tested at petabyte scale (TiKV, CockroachDB).
- **BEAM/OTP** — process isolation, fault tolerance, Erlang distribution. The runtime that makes all of this possible.

---

*Repository: vorlang/vordb · License: MIT*
