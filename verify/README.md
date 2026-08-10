# verify/ — reproducing VorDB's verification numbers

Every verification figure in [README.md](../README.md) and
[docs/PROJECT_OVERVIEW.md](../docs/PROJECT_OVERVIEW.md) is produced by one of
these. Nothing is quoted from memory or from a previous toolchain.

Run everything:

```bash
make verify           # check + chaos, prints the numbers the docs cite
```

Or individually:

```bash
make verify-check     # mix vor.check --deep on the combined system block
make verify-chaos     # full-stack chaos: RocksDB + ETS started, then vor.simulate
```

## What each one does

**`make verify-check`** concatenates `src/vor/kv_store.vor` and
`src/vor/kv_cluster.vor` into `build/verify/kv_combined.vor` (Vor's system block
needs the agent definition in the same source) and runs the multi-agent checker
from `../vor`. `verify/check_stats.exs` dumps the raw stats map — relevance
verdicts, declared-vs-observed coverage, state count — rather than the
task's prose summary.

**`make verify-chaos`** is the part the old numbers got wrong. Plain
`mix vor.simulate` starts the KvStore agents with none of VorDB's
infrastructure on the code path, so every extern (`vordb_ffi.storage_*`,
`vordb_cache.*`, the Gleam CRDT modules) raises `:undef` and every store field
holds a `{:vor_extern_error, ...}` tuple for the whole run. The declared
invariants pass anyway. `verify/chaos.exs` prepends VorDB's `build/dev/erlang/*/ebin`
to the code path and starts `vordb_storage` (RocksDB, temp dir),
`vordb_cache`, and `vordb_dirty_tracker` before handing off to
`Vor.Simulator.run_file/2`, so the agents run real CRDT code against real
storage.

Both report the two axes that matter more than the verdict:

- **relevance** — `substantive` / `vacuous` / `unexercised` per invariant
- **coverage** — declared-vs-observed handlers, accepts, emits, states

A `✓ PASS` with 0/51 handlers reached is not a pass. Read the axes.

## Known limits of these runs

- The chaos harness still has **no anti-entropy**: gossip leaves the agent
  through an extern aimed at real cluster peers that the harness does not
  start, so replicas never reconcile. Convergence is not tested here — it is
  tested by `vordb_cluster_tests` / `vordb_integration_tests` in `make test`.
- The model checker explores **1 state**. See finding F-004 in
  [docs/VERIFICATION_AUDIT_2026-08.md](../docs/VERIFICATION_AUDIT_2026-08.md).
- Seeds fix the fault schedule and workload, not BEAM scheduling; check and
  fault counts reproduce, workload counts drift by a few operations.
