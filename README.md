# VorDB

**A CRDT-based distributed key-value store with verified coordination**

VorDB is a distributed database where every node accepts writes, conflicts resolve automatically using CRDTs, and the coordination layer is formally verified at compile time. Built on [Vor](https://github.com/vorlang/vor), Gleam, RocksDB, and the BEAM.

## Why

Distributed databases have their most dangerous bugs in the coordination layer — merge functions, gossip protocols, state machine transitions. A bug in a CRDT merge function silently corrupts data across the entire cluster. These bugs are hard to find with testing because they only manifest under specific timing and network conditions.

VorDB's coordination layer is written in [Vor](https://github.com/vorlang/vor), a BEAM-native language with compile-time verification. The merge function that resolves conflicts between nodes is verified to be correct before the code ever runs. The safety properties are proven, not tested.

The application layer is written in Gleam for static type safety, with Erlang FFI modules bridging to Vor agents and RocksDB. Three layers of defense: Vor verifies coordination, Gleam's type system catches application bugs at compile time, and property tests verify CRDT merge correctness.

Riak proved that CRDT-based distributed storage on the BEAM works at production scale. Companies like Comcast, bet365, and NHS ran it for real workloads. Riak's failure was business execution, not technology. VorDB picks up where Riak left off — with formal verification that Riak never had.

## Architecture

```
Client (HTTP)
      │
      ▼
HTTP API (mist — Gleam)
      │
      ▼
Vnode Router (consistent hashing — Gleam)
      │
      ├──→ Vnode 0:  KvStore (Vor agent)
      ├──→ Vnode 1:  KvStore (Vor agent)
      ├──→ ...
      └──→ Vnode 15: KvStore (Vor agent)
                │
            ┌───┴───┐
            ▼       ▼
    CRDT Merge    Extern calls
    (verified)    via Erlang FFI
                    │
                    ▼
              RocksDB (persistence)
```

Each node runs N virtual nodes (vnodes), each owning a portion of the keyspace. Every vnode is an independent Vor agent with its own state, its own gossip timer, and its own failure domain. A crashed vnode restarts from RocksDB without affecting other vnodes.

| Layer | Language | Verified? | Role |
|---|---|---|---|
| Coordination | Vor | Compile-time proven | CRDT merge, gossip timing, state recovery |
| Application | Gleam | Statically typed | CRDT types, vnode routing, HTTP, gossip dispatch |
| FFI Bridge | Erlang | Tested | Vor agent ↔ Gleam, RocksDB NIF, process registry |
| Storage | RocksDB (C++) | Battle-tested | Persistence, crash recovery, compression |
| Runtime | BEAM/OTP | Battle-tested | Process isolation, distribution, fault tolerance |

## Quick Start

### Prerequisites

- Erlang/OTP 25+
- Gleam 1.0+
- Elixir (for Vor compiler only)
- [Vor compiler](https://github.com/vorlang/vor) (cloned alongside this repo)

### Build

```bash
git clone https://github.com/vorlang/vordb.git
cd vordb
make build    # Compiles Vor agent, then runs gleam build
```

`make build` first compiles `kv_store.vor` using the Vor compiler (requires the Vor repo at `../vor`), then runs `gleam build` to compile all Gleam and Erlang modules.

### Run a single node

```bash
erl -pa build/dev/erlang/*/ebin -eval 'vordb_app:start(normal, []).'
```

The HTTP API is available at `http://localhost:4001`.

### Run a 3-node cluster

```bash
# Terminal 1
erl -sname node1 -pa build/dev/erlang/*/ebin -eval 'vordb_app:start(normal, []).'

# Terminal 2
erl -sname node2 -pa build/dev/erlang/*/ebin -eval 'vordb_app:start(normal, []).'

# Terminal 3
erl -sname node3 -pa build/dev/erlang/*/ebin -eval 'vordb_app:start(normal, []).'
```

### Dynamic cluster membership

```bash
# Start a 4th node and join the cluster
curl -X POST http://localhost:4004/admin/join \
  -H "Content-Type: application/json" \
  -d '{"seed_node": "node1@localhost"}'

# Leave cluster gracefully
curl -X POST http://localhost:4004/admin/leave

# View cluster membership
curl http://localhost:4001/admin/members
```

### Write and read

```bash
# Write to node 1
curl -X PUT http://localhost:4001/kv/greeting \
  -H "Content-Type: application/json" \
  -d '{"value": "hello world"}'

# Read from node 2 (after gossip converges)
curl http://localhost:4002/kv/greeting

# Delete from node 3
curl -X DELETE http://localhost:4003/kv/greeting
```

## API

### Key-Value (LWW-Register)

| Method | Endpoint | Description |
|---|---|---|
| `PUT /kv/:key` | Write a value | Body: `{"value": "..."}` → `{"ok": true, "timestamp": ...}` |
| `GET /kv/:key` | Read a value | → `{"key": "...", "value": "..."}` or 404 |
| `DELETE /kv/:key` | Delete a value | → `{"deleted": true, "timestamp": ...}` |

### Sets (OR-Set)

| Method | Endpoint | Description |
|---|---|---|
| `POST /set/:key/add` | Add element | Body: `{"element": "..."}` → `{"ok": true}` |
| `POST /set/:key/remove` | Remove element | Body: `{"element": "..."}` → `{"ok": true}` |
| `GET /set/:key` | List members | → `{"key": "...", "members": [...]}` or 404 |

### Counters (PN-Counter)

| Method | Endpoint | Description |
|---|---|---|
| `POST /counter/:key/increment` | Increment | Body: `{"amount": 1}` (optional) → `{"ok": true}` |
| `POST /counter/:key/decrement` | Decrement | Body: `{"amount": 1}` (optional) → `{"ok": true}` |
| `GET /counter/:key` | Get value | → `{"key": "...", "value": N}` or 404 |

### Cluster

| Method | Endpoint | Description |
|---|---|---|
| `GET /status` | Node status | → `{"status": "running", "num_vnodes": N}` |
| `POST /admin/join` | Join cluster | Body: `{"seed_node": "..."}` |
| `POST /admin/leave` | Leave cluster | |
| `GET /admin/members` | View membership | → `{"members": [...]}` |
| `POST /admin/full-sync` | Trigger full-state sync | For recovery |

## CRDT Types

### LWW-Register (Last-Writer-Wins)

Simple key-value storage. Each entry carries a timestamp and node ID. When two nodes write to the same key concurrently, the write with the higher timestamp wins. Equal timestamps are broken by node ID comparison. Deletes are tombstones.

Merge is implemented in Vor using native `map_merge(:lww)` and verified at compile time.

### OR-Set (Observed-Remove Set)

A set where elements can be added and removed. Concurrent add and remove of the same element resolves in favor of the add (add-wins semantics). Each add is tagged with a unique identifier; a remove only affects tags it has observed.

Merge is set union on tag maps — property-tested for commutativity, associativity, and idempotency.

### PN-Counter (Positive-Negative Counter)

A counter supporting both increment and decrement. Implemented as a pair of G-Counters: one for increments, one for decrements. The value is the difference. Each node maintains its own counts; merge takes the max per node per side.

Merge is property-tested for commutativity, associativity, and idempotency.

## Gossip Protocol

VorDB uses delta-state gossip with ACK-based delivery. Each vnode independently tracks which keys changed since the last sync to each peer. On its gossip interval, a vnode sends only the changed entries (deltas) to its peer vnodes on other nodes. Peers ACK received deltas; unACKed deltas are retried.

Full-state sync is available as an on-demand operation for node joins and disaster recovery, but is not required for normal operation.

## Vnode Sharding

Each node divides its keyspace into N vnodes (default 16) using consistent hashing. Each vnode is an independent Vor agent process with its own state, gossip timer, and RocksDB key range. Benefits:

- Multi-core parallelism — operations on different keys run concurrently
- Failure isolation — a crashed vnode only affects its key range
- Granular gossip — each vnode tracks and sends only its own changes

## Persistence

RocksDB provides crash recovery. When a vnode starts, its Vor agent's `on :init` handler loads persisted state from RocksDB before accepting any messages — no race window, no stale reads.

## Tests

```bash
# Build and run all tests (compiles Vor agent first)
make test

# Or separately:
make vor         # Compile Vor agent
gleam build      # Compile Gleam + Erlang modules
gleam test       # Run tests
```

108 test cases and 15 property-based suites covering CRDT merge correctness (commutativity, associativity, idempotency for all three types), HTTP REST endpoints, vnode routing, delta gossip with ACK tracking, persistence across restarts, and multi-node cluster convergence.

## Project Structure

```
vordb/
├── gleam.toml                        # Gleam project config
├── Makefile                          # Build: Vor pre-compilation + gleam build
├── src/
│   ├── vor/
│   │   └── kv_store.vor              # Vor agent — verified coordination layer
│   ├── vordb.gleam                   # Public API (typed)
│   ├── vordb/
│   │   ├── types.gleam               # Core type definitions
│   │   ├── entry.gleam               # LWW entry types and helpers
│   │   ├── or_set.gleam              # OR-Set CRDT (typed, pure)
│   │   ├── counter.gleam             # PN-Counter CRDT (typed, pure)
│   │   ├── serializer.gleam          # Term serialization
│   │   ├── map_utils.gleam           # Map utilities
│   │   ├── storage.gleam             # RocksDB typed wrapper
│   │   ├── vnode_router.gleam        # Consistent hash routing
│   │   ├── cluster.gleam             # Peer discovery
│   │   ├── gossip.gleam              # Gossip dispatch
│   │   └── http_router.gleam         # HTTP API (mist)
│   ├── vordb_ffi.erl                 # Erlang FFI — Vor agent bridge, RocksDB, storage gen_server
│   ├── vordb_http_ffi.erl            # HTTP JSON parsing and response formatting
│   ├── vordb_dirty_tracker.erl       # Per-vnode per-peer delta tracking with ACKs
│   ├── vordb_membership.erl          # Dynamic cluster membership
│   ├── vordb_vnode_sup.erl           # Vnode supervisor (starts N agents)
│   ├── vordb_vnode_starter.erl       # Agent start + registry
│   ├── vordb_registry.erl            # ETS-based process registry
│   └── vordb_app.erl                 # OTP application + supervision tree
└── test/
    ├── *_test.gleam                  # Gleam unit tests (entry, counter, or_set, map_utils)
    ├── dirty_tracker_test.gleam      # DirtyTracker ACK tests
    ├── storage_test.gleam            # RocksDB round-trip tests
    ├── kv_store_test.gleam           # Vor agent CRUD + sync tests
    ├── http_test.gleam               # HTTP endpoint tests (starts mist)
    ├── cluster_test.gleam            # Multi-instance gossip convergence tests
    └── property_test.gleam           # CRDT property tests (15 suites)
```

## Status

**Phase 1 — complete. Gleam migration — complete.**

- Three CRDT types: LWW-Register (compile-time verified merge), OR-Set, PN-Counter (property-tested merge)
- Delta-state gossip with ACK-based delivery and per-vnode timers
- Vnode sharding (16 vnodes per node, consistent hashing)
- Dynamic cluster membership (join/leave)
- RocksDB persistence with vnode-aware key prefixing
- REST API for all CRDT types plus cluster administration
- Application layer in Gleam with static types; Erlang FFI bridge to Vor agents
- 108 test cases + 15 property-based suites

Roadmap includes consistent hashing ring with partial replication, multi-datacenter gossip, TTL, and anti-entropy.

## License

MIT
