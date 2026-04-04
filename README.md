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
Vor Coordination Layer (compile-time verified)
  ├── LWW-Register merge — map_merge with :lww strategy
  ├── State recovery — on :init loads from RocksDB
  └── Extern calls to storage layer
      │
      ▼
Elixir Storage Layer
  └── RocksDB (persistence, crash recovery, compression)
```

| Layer | Language | Verified? | Role |
|---|---|---|---|
| Coordination | Vor | Compile-time proven | CRDT merge, gossip protocol, conflict resolution |
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

| Method | Endpoint | Description |
|---|---|---|
| `PUT /kv/:key` | Write a value | Body: `{"value": "..."}` → `{"ok": true, "timestamp": ...}` |
| `GET /kv/:key` | Read a value | → `{"key": "...", "value": "..."}` or 404 |
| `DELETE /kv/:key` | Delete a value | → `{"deleted": true, "timestamp": ...}` |
| `GET /status` | Node status | → `{"node": "...", "peers": [...], "connected": [...]}` |

## How It Works

### LWW-Register CRDT

Each key-value pair is stored as a Last-Writer-Wins Register: `{value, timestamp, node_id}`. When two nodes write to the same key, the write with the higher timestamp wins. Equal timestamps are broken by node ID comparison. Deletes are tombstones that participate in the same merge logic.

The merge function is implemented in Vor using native `map_merge(:lww)` and verified at compile time. This guarantees commutativity, associativity, and idempotency — the three properties a CRDT merge must satisfy.

### Gossip

Nodes periodically broadcast their full state to all peers via Erlang distribution. Each receiving node merges the incoming state with its local state using the verified LWW merge. Merged state is persisted to RocksDB.

### Persistence

RocksDB provides crash recovery. When a node restarts, the Vor agent's `on :init` handler loads persisted state from RocksDB before accepting any messages — no race window, no stale reads.

## Tests

```bash
# Unit and property tests
mix test

# Include integration tests (multi-node)
mix test --include integration
```

48 tests including 5 property-based suites verifying CRDT merge correctness (commutativity, associativity, idempotency, monotonic timestamps, key preservation).

## Project Structure

```
vordb/
├── src/vor/
│   └── kv_store.vor              # Vor agent — the verified coordination layer
├── lib/vordb/
│   ├── application.ex            # OTP supervision tree
│   ├── storage.ex                # RocksDB wrapper
│   ├── serializer.ex             # Term serialization
│   ├── entry.ex                  # LWW entry helpers
│   ├── gossip.ex                 # Periodic state broadcast
│   ├── cluster.ex                # Static peer discovery
│   └── http/router.ex            # REST API
└── test/
    ├── vordb/                    # Unit + property tests
    └── integration/              # Multi-node cluster tests
```

## Status

**Phase 0 — complete.** Single CRDT type (LWW-Register), static cluster topology, full-state gossip, REST API, RocksDB persistence, compile-time verified merge.

Roadmap includes additional CRDT types, delta-state gossip, vnode sharding, consistent hashing, and multi-datacenter support.

## License

MIT
