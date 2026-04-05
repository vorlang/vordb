# VorDB

**A CRDT-based distributed key-value store with verified coordination**

VorDB is a distributed database where every node accepts writes, conflicts resolve automatically using CRDTs, and the coordination layer is formally verified at compile time. Built on [Vor](https://github.com/vorlang/vor), Elixir, RocksDB, and the BEAM.

## Why

Distributed databases have their most dangerous bugs in the coordination layer — merge functions, gossip protocols, state machine transitions. A bug in a CRDT merge function silently corrupts data across the entire cluster. These bugs are hard to find with testing because they only manifest under specific timing and network conditions.

VorDB's coordination layer is written in [Vor](https://github.com/vorlang/vor), a BEAM-native language with compile-time verification. The merge function that resolves conflicts between nodes is verified to be correct before the code ever runs. The safety properties are proven, not tested.

Riak proved that CRDT-based distributed storage on the BEAM works at production scale. Companies like Comcast, bet365, and NHS ran it for real workloads. Riak's failure was business execution, not technology. VorDB picks up where Riak left off — with formal verification that Riak never had.

## Architecture

```
Client (HTTP)
      │
      ▼
HTTP API (Plug/Cowboy)
      │
      ▼
Vnode Router (consistent hashing)
      │
      ├──→ Vnode 0:  KvStore (Vor agent)
      ├──→ Vnode 1:  KvStore (Vor agent)
      ├──→ ...
      └──→ Vnode 15: KvStore (Vor agent)
                │
            ┌───┴───┐
            ▼       ▼
    CRDT Merge    Extern calls
    (verified)    to storage
                    │
                    ▼
              RocksDB (persistence)
```

Each node runs N virtual nodes (vnodes), each owning a portion of the keyspace. Every vnode is an independent Vor agent with its own state, its own gossip timer, and its own failure domain. A crashed vnode restarts from RocksDB without affecting other vnodes.

| Layer | Language | Verified? | Role |
|---|---|---|---|
| Coordination | Vor | Compile-time proven | CRDT merge, gossip timing, state recovery |
| Vnode routing | Elixir | Tested | Consistent hashing, process registry |
| Gossip | Elixir | Tested | Delta tracking, ACK protocol, peer delivery |
| HTTP API | Elixir | Tested | REST endpoints, request routing |
| Storage | Elixir + RocksDB | Battle-tested | Persistence, crash recovery |
| Runtime | BEAM/OTP | Battle-tested | Process isolation, distribution, fault tolerance |

## Quick Start

### Prerequisites

- Erlang/OTP 25+
- Elixir 1.15+
- [Vor compiler](https://github.com/vorlang/vor) (cloned alongside this repo)
- RocksDB (installed via the `rocksdb` Hex package NIF)

### Run a single node

```bash
git clone https://github.com/vorlang/vordb.git
cd vordb
mix deps.get
mix compile

# Start node 1
elixir --sname node1 -S mix run --no-halt
```

The HTTP API is available at `http://localhost:4001`.

### Run a 3-node cluster

```bash
# Terminal 1
VORDB_NODE_ID=node1 VORDB_HTTP_PORT=4001 elixir --sname node1 -S mix run --no-halt

# Terminal 2
VORDB_NODE_ID=node2 VORDB_HTTP_PORT=4002 VORDB_PEERS=node1@localhost elixir --sname node2 -S mix run --no-halt

# Terminal 3
VORDB_NODE_ID=node3 VORDB_HTTP_PORT=4003 VORDB_PEERS=node1@localhost,node2@localhost elixir --sname node3 -S mix run --no-halt
```

### Dynamic cluster membership

```bash
# Start a 4th node and join the cluster
VORDB_NODE_ID=node4 VORDB_HTTP_PORT=4004 elixir --sname node4 -S mix run --no-halt

# Join via admin API
curl -X POST http://localhost:4004/admin/join \
  -H "Content-Type: application/json" \
  -d '{"seed_node": "node1@localhost"}'

# Leave cluster gracefully
curl -X POST http://localhost:4004/admin/leave

# View cluster membership
curl http://localhost:4001/admin/members
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
| `GET /status` | Node status | → `{"node": "...", "peers": [...], "vnodes": [...]}` |
| `POST /admin/join` | Join cluster | Body: `{"seed_node": "..."}` |
| `POST /admin/leave` | Leave cluster | |
| `GET /admin/members` | View membership | → `{"members": [...]}` |

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
# Unit and property tests
mix test

# Include integration tests (multi-node)
mix test --include integration
```

146 tests and 15 property-based suites covering CRDT merge correctness (commutativity, associativity, idempotency), vnode routing, delta gossip with ACK tracking, persistence across restarts, and multi-node cluster convergence.

## Project Structure

```
vordb/
├── src/vor/
│   └── kv_store.vor              # Vor agent — verified coordination layer
├── lib/vordb/
│   ├── application.ex            # OTP supervision tree
│   ├── vnode_supervisor.ex       # Starts N vnode agents
│   ├── vnode_router.ex           # Consistent hash routing
│   ├── storage.ex                # RocksDB wrapper
│   ├── serializer.ex             # Term serialization
│   ├── entry.ex                  # LWW entry helpers
│   ├── or_set.ex                 # OR-Set CRDT logic
│   ├── counter.ex                # PN-Counter CRDT logic
│   ├── dirty_tracker.ex          # Per-vnode per-peer delta tracking with ACKs
│   ├── gossip.ex                 # Delta and full-state sync
│   ├── membership.ex             # Dynamic cluster membership
│   ├── cluster.ex                # Peer discovery
│   └── http/router.ex            # REST + admin API
└── test/
    ├── vordb/                    # Unit + property tests
    └── integration/              # Multi-node cluster tests
```

## Status

**Phase 1 — complete.**

- Three CRDT types: LWW-Register (compile-time verified merge), OR-Set, PN-Counter (property-tested merge)
- Delta-state gossip with ACK-based delivery and per-vnode timers
- Vnode sharding (16 vnodes per node, consistent hashing)
- Dynamic cluster membership (join/leave)
- RocksDB persistence with vnode-aware key prefixing
- REST API for all CRDT types plus cluster administration
- 146 tests + 15 property-based suites

Roadmap includes consistent hashing ring with partial replication, multi-datacenter gossip, TTL, and anti-entropy.

## License

MIT
