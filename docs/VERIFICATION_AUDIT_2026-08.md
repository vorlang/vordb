# VorDB — Vor migration + deferred verification audit

**Date:** 2026-08-09
**Vor main at:** `d64b949` ("Remove dead multi-agent liveness duplicate (F10 follow-up)")
**VorDB at:** `9f2aa40` ("Vor verification integration: auto-telemetry, enhanced model checking, docs"), last touched 2026-04-28

Two jobs in one pass: bring VorDB onto current Vor main, and re-derive every
verification figure VorDB has published. The prior going in was that all of them
were vacuous. That prior was right, and the honest numbers are worse than
"vacuous" in one place — the model checker explores **one state**.

Findings are numbered `F-nnn` (new series for verification-claim findings).
Vor-side gaps to report upstream continue VorDB's existing `GAP-nnn` ledger
(previous high-water mark: GAP-014).

---

## Headline

| Claim as published | Status after re-derivation |
|---|---|
| "model checking (512 states proven across 5 invariants)" | **False.** 1 state, depth 0, 0 transitions. 2 of the 5 invariants are refused as vacuous; the other 3 are `for_all` sentinels that Vor's relevance axis cannot flag (GAP-016). |
| "6× symmetry reduction for 3 identical agents" | **Impossible.** Symmetry reduction was deleted from Vor as unsound (Vor KNOWN_ISSUES §2). The claim only ever appeared as planned guidance in `directions/VOR_VERIFICATION_INTEGRATION.md` (internal); it was never wired into a build target or a published figure. |
| "chaos simulation (0 violations under kills + partitions + delays)" | **Reproduces, and means less than it reads.** The fault/check counts replay exactly (29/4, 29/3, 59/7). As published — no workload, no infrastructure — the run reached **0 of 51 declared handlers** and every store held an extern-error tuple for the whole run. The current simulator prints that; the old one did not. |
| "Five-layer verification" | **Two and a half layers.** Layers 1 and 2 (compile-time proof, model checking) verify nothing about VorDB's coordination. Layers 3–5 (chaos, property tests, telemetry) are real. |
| "compile-time proofs (LWW merge)" | **Unsupported.** `map_merge(:lww)` is a compiler builtin; nothing in VorDB's build produces a proof obligation about it. VorDB declares zero agent-level safety invariants. |

The one thing the audit did *not* find: a regression. VorDB builds and passes its
75 tests against current Vor with no source change to the agent. That is itself
finding F-001.

---

## Phase 1 — Migration log

`make build` and `make test` are green against current Vor main with **zero
changes** to `src/vor/kv_store.vor`. 75 tests, 0 failures. The only migration
edit anywhere is the tier downgrade in `src/vor/kv_cluster.vor` required by
F-002.

### F-001 — Nothing broke, and that is the finding

Every refusal Vor added in the last four months is a refusal VorDB cannot
trigger, because VorDB never used the construct:

| Vor change | Would break VorDB if… | VorDB's actual usage |
|---|---|---|
| `proven` value invariants on gen_server agents now refuse `:unsupported_invariant` (KNOWN_ISSUES §8, F3) | any agent declared a `safety … proven` | **0** agent-level safety invariants in `KvStore` |
| system-tier `liveness … proven` now fails closed (§9, F10) | any system declared liveness | **0** liveness invariants, agent or system |
| `emit` in a periodic/`:*_fired`/resilience handler now a compile error `:caller_less_emit` (§7) | the `every` block emitted | the `every` block contains one extern call, no `emit` |
| `:*_fired` handlers now run their full body | any resilience handler existed | **0** resilience handlers |
| symmetry flags removed (§2) | any build target passed them | no build target ever invoked `mix vor.check` |
| gen_server post-terminal actions now threaded (DP0) | a handler had actions after `emit` | every handler ends on its `emit` |

VorDB uses Vor as a gen_server code generator with telemetry, and nothing more.
It had no local proofs to lose because it never claimed any. The `proven` label
in VorDB's docs refers exclusively to the five *system*-tier invariants in
`kv_cluster.vor`, which `mix compile` never runs.

Two constructs VorDB *does* use are now supported properly rather than silently:
branch-local `emit` (`on {:set_members}` emits from both arms of an `if`) and
`on :init` state loading. Both are observed working — the chaos run reaches both
`:set_members` and `:set_not_found` emits, and the explorer now applies `on :init`
(§8, F4). No behavioural difference was detectable from VorDB's side, so these
are recorded as "now covered by the conformance matrix", not as fixes to VorDB.

### F-002 — Two `proven` system invariants refused as VACUOUS PROOF

```
✗ VACUOUS PROOF: safety "v1 and v2 lww_store don't diverge to error" is declared
  `proven`, but its subject was never reachable in the explored state space.
✗ VACUOUS PROOF: safety "v2 and v3 lww_store don't diverge to error" …
```

Both are `never(vX.lww_store == :error and vY.lww_store != :error)`. The
antecedent `vX.lww_store == :error` is never true in any reachable state, so the
implication holds by emptiness. This was equally true under the old toolchain —
the old toolchain reported it as part of "512 states proven".

The source comment above them claimed they detected cross-replica divergence.
They never compared the replicas to each other; they compared each to the atom
`:error`. That comment was wrong when written.

**Action taken:** honest downgrade to `checked`, the routing the refusal
recommends, with the reason recorded inline. Not deleted, not restructured, not
`--allow-vacuous`'d.

### F-003 — The 6× symmetry claim

`directions/VOR_VERIFICATION_INTEGRATION.md` (internal) §227 recommends "Use symmetry
reduction: … Vor's symmetry reduction reduces the state space by 6× for 3
identical agents." Symmetry reduction no longer exists (deleted as unsound —
it could map states in different Sₙ orbits to one fingerprint and prune
reachable states). The 6× figure was never measured on VorDB and never reached
README or PROJECT_OVERVIEW; it was aspirational text in a spec. Annotated in
place rather than deleted, so the spec's history stays readable.

### F-005 — Five of eight generated telemetry events are unattached

`vordb_metrics:init/0` attaches `[:vor, :message, :received]`,
`[:vor, :transition]`, `[:vor, :constraint, :violated]`. Current Vor codegen
also emits:

- `[:vor, :agent, :start]`
- `[:vor, :message, :emitted]` — now carries the *declared* tag straight from
  the compiler, which is what makes the coverage axis exact
- `[:vor, :backpressure, :rejected]`
- `[:vor, :monitored, :deadline_exceeded]` — new (§9, F13)
- `[:vor, :monitored, :resilience_fired]` — new (§9, F13)

The two `:monitored` events would never fire today (VorDB declares no monitored
invariants), but `:message, :emitted` and `:agent, :start` carry information the
Prometheus endpoint currently drops. Not fixed in this pass — logged as a gap,
because attaching them is a metrics-surface change, not a migration fix.

---

## Phase 2 — The audit

### 2a. Is anti-entropy timer-driven?

**Yes — and worse than the G-Counter case.** `src/vor/kv_store.vor`:

```vor
every sync_interval_ms do
  Erlang.vordb_ffi.gossip_send_vnode_deltas(node_id: node_id, vnode_id: vnode_id)
end
```

The G-Counter's gossip was at least a `broadcast` the explorer could model once
timers fired. VorDB's is an **extern call**. `KvStore` contains no `send` and no
`broadcast` anywhere — grep confirms zero. Every inter-vnode message VorDB
exchanges (`lww_sync`, `set_sync`, `counter_sync`) is constructed inside
`vordb_ffi:gossip_send_vnode_deltas/2` and delivered by `erpc` to a real peer
node.

Consequences:

- The six `connect` edges in `kv_cluster.vor` are **decorative**. No agent ever
  puts a message on the wire, so the topology carries nothing at either tier.
- Firing timers (the Phase 3a fix that rescued Raft) buys VorDB nothing: the
  timer fires, calls an extern, the extern evaluates to `:unknown`, no message
  is queued, the state is unchanged.
- The second replication path — the coordinator's synchronous quorum fan-out in
  `vordb_coordinator:fan_out_async/3` → `vordb_quorum:quorum_write/4` — is pure
  Erlang and lives entirely outside the agent. Vor cannot see it at all.

So the pre-fix figures were measured over a space in which replication never
happened, exactly as the brief anticipated. The post-fix figures are measured
over the same space, because the mechanism is an extern rather than a timer.

**GAP-015 (report to Vor):** an agent whose inter-agent messaging goes through an
extern is indistinguishable, to the explorer, from an agent with no peers. The
`connect` topology is accepted and then never exercised. A `mix vor.check`
warning along the lines of "system declares N connections; no agent contains a
`send`/`broadcast`; topology is unexercised" would have caught this in April.

### 2b. Invariant table — strength × relevance × coverage

Configuration: `mix vor.check --deep` (queue 4, integer-bound 3, depth 50,
max-states 200k), current Vor main. **Result: 1 state explored, max depth 0.**

| # | Invariant | Declared | Relevance (checker) | Relevance (sim) | Substantive? |
|---|---|---|---|---|---|
| 1 | `lww_store never becomes the :error sentinel` | `proven` | `substantive` 1/1 | `substantive` 29/29 | **No** — see below |
| 2 | `set_store never becomes the :error sentinel` | `proven` | `substantive` 1/1 | `substantive` 29/29 | **No** |
| 3 | `counter_store never becomes the :error sentinel` | `proven` | `substantive` 1/1 | `substantive` 29/29 | **No** |
| 4 | `v1/v2 lww_store don't diverge to error` | ~~`proven`~~ → `checked` | **VACUOUS** 0/1 | **VACUOUS** 0/29 | No |
| 5 | `v2/v3 lww_store don't diverge to error` | ~~`proven`~~ → `checked` | **VACUOUS** 0/1 | **VACUOUS** 0/29 | No |

Invariants 1–3 carry a green `substantive` badge that should not be read as
evidence. Two independent reasons:

- **GAP-016 (report to Vor).** Vor's relevance axis treats a `for_all`
  invariant's subject as its quantification domain, which is non-empty whenever
  the system has agents (`Invariant.subject_active?/2`, the `{:for_all, _}`
  clause). A `for_all` invariant is therefore `substantive` **by construction**
  and can never be flagged vacuous, however empty the state space. Here it is
  reported substantive over a state space of size 1. The axis works as designed
  for `never(...)` shapes, where subject ≠ property; it is blind for `for_all`.
- **The property is far weaker than its old name.** It was called
  "lww_store is always a map"; it tests `!= :error`. An extern-error tuple, a
  stale map, an empty map, and a map that has diverged from every peer all
  satisfy it. Renamed in this pass to what it actually checks.

### 2c. Why 1 state

All three state fields are maps, and the explorer abstracts map operations to
`:unknown` (Vor KNOWN_ISSUES §5). `on :init` sets all three from externs →
`:unknown`. Every handler then writes a map operation over them → `:unknown`.
No handler emits an inter-agent message, so the pending queue never grows. Every
successor is byte-identical to its parent, is dropped by the same-as-parent
filter, and BFS terminates at the root.

The old "512 states, depth 9" figure came from the pre-fix explorer over the
same source. Whatever produced it, it was not this model.

### F-004 — The declared-vs-observed coverage axis reads green over a 1-state space

```
unreached_states: []   unfired_handlers: []   unfired_timers: []   unfired_resilience: []
```

Nothing is unreached — every one of the 17 handlers and the `every` timer is
*entered* from the root state. Coverage measures handler entry, and handler
entry genuinely happens; it is the *effect* that is invisible. Read alone, the
coverage axis would suggest full exploration of a system that never took a
transition. The two axes have to be read together: `1 state / depth 0` is the
number that carries the information.

### F-008 — VorDB declares no state machine

`states: {0, 0}` on both tiers. `KvStore` has no enum-typed state field, so it
compiles to a `gen_server` with no state graph. There is no declared-state
coverage to report because there are no declared states. This is also the root
cause of F-001: `verify_safety` has no graph to walk, which is precisely the
situation Vor's F3 fix made fail closed — VorDB simply never declared the
invariant that would have hit it.

### 2d. Simulation with integrity

Three scenarios, current simulator, replaying the seeds from the published
table. Run two ways: **as published** (`mix vor.simulate <file> --seed N`, no
workload, no VorDB infrastructure on the code path), and **full-stack**
(`make verify-chaos` — VorDB beams prepended, RocksDB + ETS cache + dirty
tracker started, workload 10/s).

| scenario | seed | dur | checks | faults | violations | integrity | handlers reached | emits reached |
|---|---|---|---|---|---|---|---|---|
| as published (kill) | 42 | 30s | 29 | 4 | 0 | not degraded | **0 / 51** | **0 / 39** |
| full-stack (kill) | 42 | 30s | 29 | 4 | 0 | not degraded | 50 / 51 | 35 / 39 |
| full-stack (partition + delay) | 123 | 30s | 29 | 3 | 0 | not degraded | 51 / 51 | 36 / 39 |
| full-stack (combined) | 777 | 60s | 59 | 7 | 0 | not degraded | 51 / 51 | 35 / 39 |

The published check and fault counts reproduce exactly. Seeding is real: fault
schedule and workload replay; workload totals drift by a few operations because
BEAM scheduling is not seeded.

**Would the old "0 violations" run have survived integrity checking?** Yes — and
that is the uncomfortable part of the answer. `assess_integrity/3` reports
`degraded?: false, reasons: []` for every run above, including the as-published
one that executed nothing. The integrity axis asks whether the *harness*
degraded (did fault-injection tasks crash, did the planned checks run); it does
not ask whether the agents did anything. The axis that catches this run is
**coverage**, which the old simulator did not report. No `UNDER-TESTED` verdict
was produced by any scenario.

### F-009 — The published chaos run reached zero handlers

`mix vor.simulate` defaults to `--workload 0`. The published table's workload
column (147/2, 145/0, 563/7) means it was run with a workload, so the real
published run was closer to row 2 than row 1. But the default invocation — the
one in the docs' command line — is row 1: 29 checks, 4 faults, 0 violations,
**0 of 51 handlers entered**. A pass over a run in which no agent processed a
single message.

### F-006 — The invariants cannot detect total store failure

Running the simulator without VorDB's beams on the code path (the documented
"known limitation"), every extern raises `:undef` and each store field holds

```erlang
{vor_extern_error, error, undef, [{vordb_ffi, storage_get_all_lww, [2], []}, …]}
```

for the entire run. All CRDT operations fail. Agents crash with `BadMapError`
and are restarted by the supervisor. **All five invariants pass, three of them
reported `substantive`.** The verdict is `✓ PASS — 29 checks, 4 faults, 0
violations`.

An error tuple is not the atom `:error`, so `lww_store != :error` holds. This is
the sharpest available demonstration that the invariant set is not measuring
store health.

### F-007 — Live replica divergence goes unreported

In the M0 counterexample dump (below), the harness printed live agent state at
the moment of violation:

```
v1: set_store: %{"test" => {:orswot, %{"test" => %{node1: 2}}, %{node1: 2}}}
v2: set_store: %{"test" => {:orswot, %{}, %{}}}
```

v1 and v2 hold different values for the same key, in a run reported as passing
on every convergence-adjacent invariant. There is no anti-entropy in the
simulator — gossip leaves through an extern aimed at cluster peers the harness
does not start — so replicas *cannot* converge there. A real convergence
invariant is therefore not addable at the simulation tier without also
modelling gossip inside the agent; and at the checker tier it is trivially true,
because both sides are `:unknown`.

Convergence is tested nowhere in the Vor tiers. It is tested by
`vordb_cluster_tests` and `vordb_integration_tests` in `make test`.

### 2e. Mutation tests

Three plants. All run against both tiers where the tier can see the code at all.

| id | mutation | checker | simulator | `make test` |
|---|---|---|---|---|
| **M0** | positive control — `on {:put}` sets `transition lww_store: :error` | **RED** — counterexample at depth 1, 2 states | **RED** — violation reported with full agent-state dump | n/a |
| **M1** | **`on {:lww_sync}` drops the remote store** (`transition lww_store: lww_store`) — replicas can never converge | **GREEN** — 1 state, depth 0, byte-identical output to unmutated | **GREEN** — pass, 19 checks, 3 faults, integrity clean | n/a |
| **M2** | `vordb_quorum:quorum_write/4` acks after 1 replica regardless of W | not visible | not visible | **RED** — 1 failure (`quorum_test`: W=2 write succeeds on single node) |

### F-010 — The headline mutation is not caught

M1 is the plant the brief asked for: break replica convergence. **Neither Vor
tier goes red.** The checker's output is not merely still-green, it is
character-for-character identical to the unmutated run, because the mutated
handler and the original both write `:unknown` into `lww_store`. The simulator
is green because no invariant compares replicas, and could not usefully do so
(F-007).

M0 confirms the harness is capable of going red, so this is a property of
VorDB's invariant set and model, not a broken toolchain.

### F-011 — Quorum is caught, but not by Vor

M2 is caught in under a minute — by a hand-written gleeunit test, not by
verification. `vordb_quorum` is pure Erlang invoked from the coordinator, which
runs in the HTTP handler's process. It is not inside any Vor agent, so no Vor
tier has visibility into it by construction. VorDB's tunable-consistency feature
— arguably its most safety-critical logic — is defended entirely by
`vordb_quorum_tests`.

---

## Phase 3 — Docs updated

- `README.md` — the "512 states proven", "0 violations", and "five-layer
  verification" claims removed; replaced with the Phase 2 numbers and their
  caveats. Feature bullet rewritten.
- `docs/PROJECT_OVERVIEW.md` — §Verification rewritten end to end: the
  compile-time-proof claim withdrawn, the 512-state figure replaced with
  "1 state, depth 0" and why, the chaos table given coverage and integrity
  columns, and a new "What each level does not catch" table added alongside the
  existing one.
- `directions/VOR_VERIFICATION_INTEGRATION.md` (internal) — symmetry recommendation
  annotated as removed-from-Vor (F-003); the "1,001 states" Raft reference
  annotated as the vacuous pre-fix figure.
- `src/vor/kv_cluster.vor` — tier downgrade (F-002), invariant renames, and the
  decorative-topology / no-convergence-invariant facts recorded inline.
- `verify/` + `make verify` — new. Every figure the docs now quote is produced
  by a target in this repo.

---

## Phase 4 — Separability of the ring layer (assessment only)

**Verdict: cleanly separable from Vor; moderately entangled with the storage
engine through two seams — the registry and the CRDT-aware read-repair path.**
Extraction is a real project, not a rename, but nothing is architecturally
blocking.

### Coupling to Vor: one line

```erlang
%% vordb_vnode_starter.erl
gen_server:start_link('Elixir.Vor.Agent.KvStore', [{node_id, …}, …], [])
```

That is the entire ring-layer dependency on Vor. `vordb_ffi` is the extern
*target* (Vor calls into it, not the reverse). No ring-layer module references
`Vor.*`. A standalone ring library would take a `{Module, Args}` vnode child
spec and never know Vor exists.

### Tier 1 — moves as-is (zero VorDB-internal dependencies)

| module | lines | notes |
|---|---|---|
| `src/vordb/ring.gleam` | 232 | Pure consistent-hash ring: `new/3`, `key_to_partition/2`, `preference_list/2`, `node_all_partitions/2`, `add_node/2`, `remove_node/2`, `diff/2`, `to_binary/1`, `from_binary/1`. Imports only `gleam/*`. This is the library. |
| `src/vordb_ring_manager.erl` | 99 | `persistent_term`-backed ring holder. No `vordb_*` calls at all. |
| `src/vordb_registry.erl` | 27 | Generic `{key → pid}` ETS registry. Not ring-specific; a dependency of everything. |

### Tier 2 — moves with callbacks abstracted

| module | lines | what has to be parameterised |
|---|---|---|
| `vordb_vnode_sup.erl` + `vordb_vnode_starter.erl` | 81 | The vnode child spec (currently hardcoded to the Vor agent). |
| `vordb_ring_gossip.erl` | 127 | Calls `vordb_vnode_sup:apply_ring_change/2`, `vordb_handoff`, `vordb_dirty_tracker`. Needs a "ring changed" callback instead of three direct calls. |
| `vordb_membership.erl` | 85 | Only touches `vordb_dirty_tracker` (to reset peer state). One callback. |

### Tier 3 — would have to be rewritten or left behind

| module | lines | why it does not move |
|---|---|---|
| `vordb_handoff.erl` | 165 | Streams partition data. Calls `vordb_ffi:storage_iterate_partition/3`, `storage_delete_partition/1` — a storage-engine contract. Extractable only behind a `partition_store` behaviour. |
| `vordb_quorum.erl` | 300 | Generic in shape (fan out, collect W/R, merge, repair) but the merge and repair steps know LWW vs ORSWOT vs PN-counter and call `vordb_cache` and `vordb_ffi` directly. Needs a `crdt` behaviour to become generic. |
| `vordb_coordinator.erl` | 334 | Bucket lookup, key prefixing, ETS-cache fast path, metrics. VorDB application logic that happens to route. Stays. |
| `vordb_ffi.erl` | 560 | God module: storage, CRDT helpers, gossip, ring lookups, agent start. Would have to be split before anything above it moves cleanly. **This is the main obstacle**, not the ring code itself. |

### Storage engine — stays put

`vordb_cache`, `vordb_dirty_tracker`, `vordb_bucket_registry`, the RocksDB half
of `vordb_ffi`, and the Gleam CRDT modules (`or_set`, `counter`, `entry`,
`types`, `map_utils`).

### Dependency sketch

```
                 ┌──────────────────────────────────────────┐
   HTTP / TCP ──▶│ vordb_coordinator  ──▶ vordb_quorum       │  VorDB app
                 │      │                    │               │  (stays)
                 └──────┼────────────────────┼───────────────┘
                        │                    │
                 ┌──────▼────────────────────▼───────────────┐
                 │ vordb_ring_manager ◀── vordb_ring_gossip  │  ring layer
                 │        │                   │              │  (would move)
                 │        │            vordb_vnode_sup       │
                 │        ▼                   │              │
                 │  vordb@ring (pure)   vordb_membership     │
                 └────────────────────────────┼──────────────┘
                                              │  ← only Vor coupling
                                       vordb_vnode_starter
                                              │
                                    Elixir.Vor.Agent.KvStore
                                              │  externs
                 ┌────────────────────────────▼──────────────┐
                 │ vordb_ffi ─ vordb_cache ─ dirty_tracker    │  storage
                 │ RocksDB · or_set · counter · entry         │  (stays)
                 └───────────────────────────────────────────┘
                                    ▲
                     vordb_handoff ─┘  (straddles: ring-driven, storage-coupled)
```

Rough size: ~440 lines move cleanly, ~290 move with callbacks, ~1,300 need a
`vordb_ffi` split first.

### Bearing on the audit

Extraction would not change VorDB's verification posture in either direction.
The ring layer is 100% outside every Vor tier today — no ring code is inside a
Vor agent, so nothing that moves would stop being verified, because none of it
is verified now.

---

## Findings index

| id | finding | severity |
|---|---|---|
| F-001 | Migration surfaced no break; VorDB uses none of the constructs Vor made fail-closed | informational, load-bearing |
| F-002 | Two `proven` system invariants refused as VACUOUS PROOF; downgraded to `checked` | high |
| F-003 | "6× symmetry reduction" recommendation refers to a feature deleted as unsound | medium (doc) |
| F-004 | Model checker explores 1 state at depth 0; coverage axis nonetheless reads green | **critical** |
| F-005 | 5 of 8 generated telemetry events unattached, including both new `:monitored` events | medium |
| F-006 | All five invariants pass while every store holds an extern-error tuple | **critical** |
| F-007 | Live replica divergence observed in a passing run; no invariant compares replicas | high |
| F-008 | `KvStore` declares no enum state — no state machine, no declared-state coverage | informational |
| F-009 | The published chaos invocation reaches 0/51 handlers and is not flagged UNDER-TESTED | high |
| F-010 | **Mutation M1 (broken replica convergence) is caught by nothing** | **critical** |
| F-011 | Mutation M2 (broken quorum) caught only by hand-written tests; Vor cannot see quorum | high |
| GAP-015 | Vor: extern-mediated messaging makes a declared `connect` topology silently unexercised | report upstream |
| GAP-016 | Vor: `for_all` invariants are `substantive` by construction and can never be flagged vacuous | report upstream |

## To report back to Vor (not fixed here)

Neither is a bug in the sense of incorrect output — both are cases where a true
report reads as stronger evidence than it is.

**GAP-015 — unexercised topology.** `mix vor.check` accepts a system with N
`connect` edges and no `send`/`broadcast` in any agent, and reports a clean
verdict over a state space with no inter-agent message. VorDB is the worst case:
6 declared edges, 0 possible messages, 1 state. A warning at check time
("topology declared but no agent can produce a message") costs one AST pass and
would have flagged this in April.

**GAP-016 — `for_all` relevance.** `Vor.Explorer.Invariant.subject_active?/2`
returns `map_size(agents) > 0` for `{:for_all, _}`, documented as "universal
vacuity is an empty domain, which never happens". Sound as stated, but the
resulting `✓ substantive` badge is indistinguishable in the output from the
`never(...)` case where it carries real information. Over VorDB's 1-state space
three invariants print `substantive (subject held in 1/1 states)`. Suggestion:
report `for_all` relevance as `n/a` or `trivial`, or qualify it by the size of
the explored space, rather than as `substantive`.

## Reproducing

```bash
make verify-check     # 1 state, depth 0, relevance verdicts, coverage
make verify-chaos     # the three scenarios, full-stack, with coverage + integrity
make test             # 75 tests
```

Mutation sources are not committed — the diffs are three lines and are quoted
in §2e.
