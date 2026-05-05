# Catalog Mutex Granularization Plan

## Current State (2026-05-05)

The Catalog uses a single `sync.RWMutex` (`c.mu`) protecting all tables, indexes, views, triggers, CTE results, undo logs, savepoints, and statistics. An audit of all 70+ lock sites in `pkg/catalog/` found:

- **Read-only paths already use `RLock()`**: `Select`, `SearchFTS`, `GetTable`, `ListTables`, `GetIndex`, `ApplyRLSFilter`, and ~30 others correctly acquire read locks.
- **Write paths correctly use `Lock()`**: `CreateTable`, `DropTable`, `Insert`, `Update`, `Delete`, `CreateIndex`, `Vacuum`, `Save`, `Load`, etc.
- **No simple `Lock()` -> `RLock()` conversions remain**.
- **Deadlock risk fixed**: `evaluateMatchExpr` no longer re-acquires `RLock()` while callers hold it (see commit `41b532a`).

## Problem

Under high concurrency:
1. A single `Lock()` blocks all readers and writers.
2. DDL (`CREATE TABLE`, `ALTER TABLE`, `VACUUM`) serializes everything.
3. Even read-heavy workloads can stall when a writer is waiting (Go `RWMutex` writer starvation prevention blocks new readers).

## Proposed Migration Path

### Phase 1: Table-Level Locking (Target: v2.4)
Replace the single catalog mutex with a hierarchy:

```
Catalog.mu         sync.RWMutex   // Protects table registry (map[string]*Table)
Table.mu           sync.RWMutex   // Protects table metadata + row data
tree.Mu            sync.RWMutex   // Already exists in btree
```

**Changes needed:**
- Add `sync.RWMutex` to `Table` struct.
- `GetTable` returns a pointer; callers acquire `Table.mu` for reads/writes.
- `scanTableRows` acquires `Table.mu.RLock()` instead of `Catalog.mu.RLock()`.
- DDL (`CreateTable`, `DropTable`) still acquires `Catalog.mu.Lock()` to mutate the registry.
- DML (`Insert`, `Update`, `Delete`) acquires `Catalog.mu.RLock()` to find the table, then `Table.mu.Lock()` for the mutation.

**Risk**: `Table` is currently a value type stored in `Catalog.tables`. Changing it to a pointer or adding a mutex to the value requires careful refactoring of all sites that copy `Table`.

### Phase 2: Index-Level Locking (Target: v2.5)
Move index trees under per-index mutexes so index maintenance during DML doesn't block scans on other indexes of the same table.

### Phase 3: Partition-Level Locking (Target: v2.6)
If table partitioning is heavily used, lock individual partition trees instead of the whole table.

## Acceptance Criteria
- `go test ./pkg/... -race` passes after each phase.
- Benchmark `BenchmarkStress_ConcurrentReadsWrites` shows <50% reduction in p99 latency under mixed workloads.
- No deadlock regressions (verified by existing `TestDeadlockDetection` and stress tests).

## Blockers
- `Table` is currently copied by value in several places (e.g., `GetTable` returns a copy). Need to audit all `Table` copies before adding a mutex.
- `Vacuum` rebuilds trees while holding `Catalog.mu.Lock()`; needs to acquire per-table locks in a deterministic order to avoid deadlocks.
