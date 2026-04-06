# VorDB

**A CRDT-based distributed key-value store with verified coordination.**

Every node accepts writes. Conflicts resolve automatically via CRDTs. The coordination layer is formally verified at compile time using [Vor](https://github.com/vorlang/vor). Built on Gleam, Erlang, RocksDB, and the BEAM.

VorDB is the spiritual successor to Riak — with the compile-time verification Riak never had.

For the full architecture reference, see [docs/PROJECT_OVERVIEW.md](./docs/PROJECT_OVERVIEW.md).

## Features

- **Three CRDT types** — LWW-Register (Vor-native verified merge), ORSWOT set, PN-Counter
- **Consistent hashing ring** — 256 partitions, N-way replication, dynamic membership
- **Replicated writes** — any node coordinates; sloppy quorum fan-out to N replicas
- **Delta gossip with ACKs** — bandwidth proportional to replication factor, not cluster size
- **Partition handoff** — data streams to new owners on ring changes
- **ETS read path** — reads bypass gen_server via write-through cache
- **Per-partition column families** — write isolation, independent compaction
- **Lock-free ETS dirty tracker** — no GenServer bottleneck on the write path
- **Protobuf TCP protocol** — length-prefixed binary protocol alongside HTTP REST
- **Telemetry + Prometheus** — `/metrics` endpoint with low-cardinality tags

## Quick Start

### Prerequisites

- Erlang/OTP 25+
- Gleam 1.0+
- [Vor compiler](https://github.com/vorlang/vor) cloned at `../vor`

### Build & Run

```bash
git clone https://github.com/vorlang/vordb.git
cd vordb
make build      # Compile proto + Vor agent + Gleam + Erlang
make test       # Run all tests
make run        # Start a single node (HTTP :4001, TCP :5001)
```

### Multi-node cluster

```bash
# Node 1 (seed)
VORDB_NODE_ID=node1 VORDB_HTTP_PORT=4001 VORDB_TCP_PORT=5001 make run

# Node 2 — join via admin API
VORDB_NODE_ID=node2 VORDB_HTTP_PORT=4002 VORDB_TCP_PORT=5002 make run
curl -X POST http://localhost:4002/admin/join \
  -H "Content-Type: application/json" \
  -d '{"seed_node": "node1@localhost"}'
```

### Write and read

```bash
curl -X PUT http://localhost:4001/kv/greeting \
  -H "Content-Type: application/json" \
  -d '{"value": "hello"}'

curl http://localhost:4002/kv/greeting   # reads after gossip converges
```

## API

### Key-Value (LWW-Register)

| Method | Endpoint | Description |
|---|---|---|
| `PUT /kv/:key` | Write a value (`{"value": "..."}`) |
| `GET /kv/:key` | Read a value (404 if missing) |
| `DELETE /kv/:key` | Tombstone delete |

### Sets (ORSWOT)

| Method | Endpoint | Description |
|---|---|---|
| `POST /set/:key/add` | Add element (`{"element": "..."}`) |
| `POST /set/:key/remove` | Remove element |
| `GET /set/:key` | List members |

### Counters (PN-Counter)

| Method | Endpoint | Description |
|---|---|---|
| `POST /counter/:key/increment` | Increment (`{"amount": N}` optional) |
| `POST /counter/:key/decrement` | Decrement |
| `GET /counter/:key` | Get value |

### Cluster & Operations

| Method | Endpoint | Description |
|---|---|---|
| `GET /status` | Node status |
| `GET /metrics` | Prometheus scrape endpoint |
| `POST /admin/join` | Join cluster (`{"seed_node": "..."}`) |
| `POST /admin/leave` | Leave cluster gracefully |
| `GET /admin/members` | View membership |
| `POST /admin/full-sync` | Trigger on-demand full-state sync |

A binary TCP protocol (length-prefixed protobuf) is also exposed on `tcp_port` (default 5001) and supports all CRDT operations.

## Status

**Phase 3 — complete. Production-ready.**

| Phase | Milestone |
|---|---|
| Phase 0 | LWW + 3 nodes + REST + RocksDB |
| Phase 1 | OR-Set, PN-Counter, delta gossip with ACKs, vnode sharding, membership |
| Phase 2 | Consistent hashing ring, replicated writes, handoff, ring gossip |
| Phase 3 | ORSWOT, ETS reads, column families, ETS DirtyTracker, protobuf/TCP, telemetry |

72 tests, 0 failures. CRDT merge correctness verified by 15 property-based suites (commutativity, associativity, idempotency × 3 types).

See [docs/PROJECT_OVERVIEW.md](./docs/PROJECT_OVERVIEW.md) for architecture, request flow, module reference, supervision tree, configuration, and outstanding issues.

## License

MIT
