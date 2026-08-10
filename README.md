# VorDB

**A CRDT-based distributed key-value store on the BEAM.**

Every node accepts writes. Conflicts resolve automatically via CRDTs. Built on Gleam, Erlang, RocksDB, and the BEAM, with the coordination vnode compiled from [Vor](https://github.com/vorlang/vor).

**On verification claims:** earlier versions of this README advertised "five-layer verification", "512 states proven", and "0 violations under chaos". A re-derivation against current Vor (2026-08-09) found those figures unsupportable — the model checker explores **one state**, and the declared invariants do not catch a deliberately broken replica-merge. What holds up is chaos simulation, CRDT property testing, and auto-generated telemetry. The full audit, including the mutation tests that failed to go red, is in
[docs/VERIFICATION_AUDIT_2026-08.md](./docs/VERIFICATION_AUDIT_2026-08.md). Numbers below are reproducible with `make verify`.

For the full architecture reference, see [docs/PROJECT_OVERVIEW.md](./docs/PROJECT_OVERVIEW.md).

## Features

- **Three CRDT types** — LWW-Register (Vor-native `map_merge(:lww)`), ORSWOT set, PN-Counter
- **Buckets** — named collections with per-bucket CRDT type, TTL, replication factor, and consistency level
- **TTL** — automatic key expiration per bucket, reset on mutation (active data doesn't expire)
- **Tunable consistency** — per-bucket W/R quorum with presets: `eventual`, `session`, `consistent`, `paranoid`
- **Read repair** — quorum reads detect and fix stale replicas automatically
- **Consistent hashing ring** — 256 partitions, N-way replication, dynamic membership
- **Replicated writes** — any node coordinates; fan-out to N replicas with configurable write quorum
- **Delta gossip with ACKs** — bandwidth proportional to replication factor, not cluster size
- **Partition handoff** — data streams to new owners on ring changes
- **ETS read path** — reads bypass gen_server via write-through cache
- **Per-partition column families** — write isolation, independent compaction
- **Lock-free ETS dirty tracker** — no GenServer bottleneck on the gossip path
- **Protobuf TCP protocol** — length-prefixed binary protocol alongside HTTP REST
- **Telemetry + Prometheus** — `/metrics` endpoint with low-cardinality tags; Vor auto-generates agent telemetry (transitions, messages, constraint violations)
- **Chaos simulation** — real BEAM processes under kills, partitions, and message delays, with declared-vs-observed coverage reported per run
- **CRDT property testing** — commutativity, associativity, idempotency across all three merge functions
- **Auto-generated telemetry** — transitions, messages, and constraint violations instrumented by the Vor compiler, not by hand

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

### Create a bucket and write data

```bash
# Create a bucket with ORSWOT type, 1-hour TTL, session consistency
curl -X POST http://localhost:4001/buckets \
  -H "Content-Type: application/json" \
  -d '{"name": "user_tags", "type": "orswot", "ttl_seconds": 3600, "consistency": "session"}'

# Add elements to a set
curl -X POST http://localhost:4001/bucket/user_tags/user_123/add \
  -H "Content-Type: application/json" \
  -d '{"element": "premium"}'

# Read set members (from any node)
curl http://localhost:4002/bucket/user_tags/user_123
```

### Legacy API (default buckets)

```bash
# These still work — route to auto-created default buckets
curl -X PUT http://localhost:4001/kv/greeting \
  -H "Content-Type: application/json" \
  -d '{"value": "hello"}'

curl http://localhost:4002/kv/greeting
```

## API

### Bucket Management

| Method | Endpoint | Description |
|---|---|---|
| `POST /buckets` | Create bucket | `{"name": "...", "type": "lww\|orswot\|pn_counter", "ttl_seconds": N, "consistency": "eventual\|session\|consistent\|paranoid"}` |
| `GET /buckets` | List all buckets | |
| `GET /buckets/:name` | Get bucket config | |
| `DELETE /buckets/:name` | Delete bucket and data | |

### Data Operations (Bucket API)

| Method | Endpoint | Description |
|---|---|---|
| `PUT /bucket/:bucket/:key` | Write value (LWW) | `{"value": "..."}` |
| `GET /bucket/:bucket/:key` | Read value | Returns value or 404 |
| `DELETE /bucket/:bucket/:key` | Delete value | Tombstone |
| `POST /bucket/:bucket/:key/add` | Add set element (ORSWOT) | `{"element": "..."}` |
| `POST /bucket/:bucket/:key/remove` | Remove set element | `{"element": "..."}` |
| `POST /bucket/:bucket/:key/increment` | Increment counter | `{"amount": N}` (optional) |
| `POST /bucket/:bucket/:key/decrement` | Decrement counter | `{"amount": N}` (optional) |

### Legacy Endpoints (Default Buckets)

| Method | Endpoint | Description |
|---|---|---|
| `PUT /kv/:key` | Write LWW value | Routes to `__default_lww__` bucket |
| `GET /kv/:key` | Read LWW value | |
| `DELETE /kv/:key` | Delete LWW value | |
| `POST /set/:key/add` | Add set element | Routes to `__default_set__` bucket |
| `POST /set/:key/remove` | Remove set element | |
| `GET /set/:key` | List set members | |
| `POST /counter/:key/increment` | Increment counter | Routes to `__default_counter__` bucket |
| `POST /counter/:key/decrement` | Decrement counter | |
| `GET /counter/:key` | Get counter value | |

### Cluster & Operations

| Method | Endpoint | Description |
|---|---|---|
| `GET /status` | Node status | |
| `GET /metrics` | Prometheus scrape endpoint | |
| `POST /admin/join` | Join cluster | `{"seed_node": "..."}` |
| `POST /admin/leave` | Leave cluster gracefully | |
| `GET /admin/members` | View membership | |
| `POST /admin/full-sync` | Trigger on-demand full-state sync | |

A binary TCP protocol (length-prefixed protobuf) is also exposed on `tcp_port` (default 5001) and supports all CRDT and bucket operations.

## Consistency Model

VorDB offers tunable consistency per bucket via W (write quorum) and R (read quorum):

| Preset | W | R | Behavior |
|---|---|---|---|
| `eventual` | 1 | 1 | Fastest. Writes confirmed locally, reads from one replica. Default. |
| `session` | 2 | 1 | Writes confirmed by majority. Reads may lag. |
| `consistent` | 2 | 2 | Strong consistency (W+R > N). Higher latency. |
| `paranoid` | 3 | 3 | All replicas must agree. Any failure blocks operations. |

When R > 1, quorum reads automatically trigger **read repair** — stale replicas are updated with the merged value asynchronously.

W=1 and R=1 use optimized fast paths with zero quorum overhead.

## Status

**Phase 3+ — production-hardened with core database features.**

| Phase | Milestone |
|---|---|
| Phase 0 | LWW + 3 nodes + REST + RocksDB |
| Phase 1 | OR-Set, PN-Counter, delta gossip with ACKs, vnode sharding, membership |
| Phase 2 | Consistent hashing ring, replicated writes, handoff, ring gossip |
| Phase 3 | ORSWOT, ETS reads, column families, ETS DirtyTracker, protobuf/TCP, telemetry |
| Core features | Buckets, TTL, tunable W/R quorum, read repair |
| Verification | Chaos simulation, auto-telemetry, protocol constraints — audited 2026-08 |

**75 tests, 0 failures.** That suite is the load-bearing safety net: it is what
catches a broken quorum, and it is what tests replica convergence.

What the Vor tiers add, measured with `make verify` on 2026-08-09:

| layer | what it produced | worth |
|---|---|---|
| Chaos simulation | 3 scenarios (seeds 42/123/777), 29–59 invariant checks, 3–7 injected faults, 0 violations, 50–51 of 51 declared handlers reached, integrity clean | **Real.** Exercises live code under kills, partitions, and delays. |
| Auto-generated telemetry | every transition, message, and constraint violation instrumented by the compiler; `lww_store` redacted as `sensitive` | **Real.** |
| Protocol constraints | `where amount > 0` on counter operations, enforced before handlers run | **Real.** |
| CRDT property testing | 15 suites × 3 CRDT types (this is plain gleeunit, not Vor) | **Real.** |
| Multi-agent model checking | 1 state, depth 0 — the agent's state is three maps, which the explorer abstracts to `:unknown`, and gossip leaves through an extern so no inter-agent message is ever modelled | **Nothing.** Two of five invariants are reported vacuous; a planted replica-merge bug does not turn it red. |
| Compile-time safety proofs | none — VorDB declares zero agent-level safety invariants | **Nothing.** The old "LWW merge is proven at compile time" claim was unsupported. |

See [docs/VERIFICATION_AUDIT_2026-08.md](./docs/VERIFICATION_AUDIT_2026-08.md)
for the findings, the mutation tests, and what would have to change for model
checking to be worth running. See [verify/README.md](./verify/README.md) for how
each figure is produced.

See [docs/PROJECT_OVERVIEW.md](./docs/PROJECT_OVERVIEW.md) for architecture, request flow, module reference, supervision tree, configuration, and outstanding issues.

## License

MIT
