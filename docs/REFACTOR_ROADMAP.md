# CobaltDB Multi-Writer MVCC Refactor Roadmap

**Date:** 2026-05-12
**Status:** In Progress — 2 critical bugs fixed today, infrastructure largely in place
**Target:** SQLite WAL-mode level concurrency within 2-3 months

---

## Executive Summary

CobaltDB already has ~80% of the multi-writer MVCC infrastructure built:
- `goroutineTxnShards[16]` — sharded goroutine-to-transaction mapping
- `commitMu[64]` — sharded commit locks by (table, key) hash
- `txnManager` bridge — `*txn.Manager` with OCC conflict detection
- `pendingWrites` / `pendingWriteMap` — buffered DML for commit-time apply
- `enableBufferedWrites` — **already enabled by `engine.Open()`**

The real bottleneck is the outer `Catalog.mu` (`sync.RWMutex`) which serializes:
1. **All SELECT queries** — `cat.mu.RLock()` held for entire scan
2. **All DML metadata lookups** — table/column resolution under `cat.mu`
3. **All DDL** — `cat.mu.Lock()` blocks everything

Additionally, the `[]interface{}` row representation forces ~2 allocs/row (Go `staticuint64s` limit + string boxing).

---

## Critical Bugs Fixed (2026-05-12)

| Commit | File | Bug | Impact |
|--------|------|-----|--------|
| `8afa28a` | `pkg/txn/manager.go:992` | WAL pooled buffer returned before WAL append completed | Data race → possible WAL corruption under concurrent writes |
| `11912eb` | `pkg/txn/manager.go:865,928` | Version shard array `[8]int` silently dropped shards > 8 | `fatal error: concurrent map read and map write` for large transactions |

Both were found by `go test -race ./pkg/...` after enabling stress benchmarks.

---

## Current Benchmark Baseline (Apple M4, 1-row txns)

| Workers | ops/sec | Scaling |
|---------|---------|---------|
| 1 | 98K | 1.0x |
| 2 | 148K | 1.5x |
| 4 | 168K | 1.7x |
| 8 | 135K | 1.4x |
| 16 | 118K | 1.2x |

**Interpretation:** Small txns scale poorly. The system is lock-bound, not CPU-bound.

---

## Phase 1: SELECT Without Catalog.mu (Week 1-2)

### Goal
Remove `cat.mu.RLock()` from the SELECT hot path so reads no longer block writes.

### Current State
```go
// pkg/catalog/catalog_select.go:22
func (cat *Catalog) Select(...) {
    cat.mu.RLock()              // <-- blocks all writers
    defer cat.mu.RUnlock()
    return cat.selectLocked(...)
}
```

### Proposed Change
Snapshot table metadata before scanning, then release the lock:

```go
// NEW: pkg/catalog/catalog_core.go
 type TableSnapshot struct {
     Def        *TableDef
     Tree       btree.TreeStore
     IdxTrees   map[string]btree.TreeStore
     SchemaVer  uint64
 }

 func (c *Catalog) getTableSnapshot(name string) TableSnapshot {
     c.mu.RLock()
     def := c.tables[name]
     tree := c.tableTrees[name]
     // copy index tree references
     idxTrees := make(map[string]btree.TreeStore)
     for _, idx := range c.indexes {
         if idx.TableName == name {
             idxTrees[idx.Name] = c.indexTrees[idx.Name]
         }
     }
     c.mu.RUnlock()
     return TableSnapshot{Def: def, Tree: tree, IdxTrees: idxTrees, SchemaVer: c.schemaVersion}
 }
```

Then `selectLocked` receives `TableSnapshot` instead of looking up under the lock.

### Files to Change
1. `pkg/catalog/catalog_core.go` — Add `TableSnapshot`, `schemaVersion atomic.Uint64`, `getTableSnapshot()`
2. `pkg/catalog/catalog_select.go:22` — Remove `cat.mu.RLock()`; snapshot metadata, call `selectLockedWithSnapshot()`
3. `pkg/catalog/catalog_select_helpers.go` — Any helper that touches `cat.tables` or `cat.indexes`
4. `pkg/catalog/catalog_eval.go` — `evaluateWhere`, `evaluateExpression` must accept snapshot or work on resolved columns

### Risk
- Stale tree reference: if DDL drops the table during scan, the `btree.TreeStore` pointer remains valid (BTree is refcounted/pinned by buffer pool). The scan completes safely; the table is just orphaned until vacuum.
- Schema change mid-scan: embed `SchemaVer` in row data (future); for now, accept that DDL + DML races are already undefined behavior.

### Acceptance Criteria
- `go test ./pkg/catalog/ -race` passes
- `BenchmarkSelectWithScan` allocs/op unchanged or improved
- `TestStress_ConcurrentReadsWrites` p99 latency drops >30%

---

## Phase 2: Schema Cache + Per-Connection Metadata (Week 2-3)

### Goal
Eliminate `cat.mu.RLock()` from INSERT/UPDATE/DELETE metadata resolution.

### Current State
Every INSERT does:
```go
c.mu.RLock()
table := c.tables[stmt.Table]
c.mu.RUnlock()
// ... validation, column mapping, encoding ...
```

### Proposed Change
Add a per-connection (or per-prepared-statement) schema cache:

```go
// pkg/engine/database.go
 type schemaCacheEntry struct {
     def       *catalog.TableDef
     version   uint64   // Catalog.schemaVersion at cache time
     cols      []string // resolved column names
 }

 type DB struct {
     // ... existing fields ...
     schemaCache   map[string]schemaCacheEntry  // keyed by "db.table"
     schemaCacheMu sync.RWMutex
 }
```

On DDL, `Catalog.schemaVersion` increments. The engine invalidates its cache on the next statement.

### Files to Change
1. `pkg/catalog/catalog_core.go` — `schemaVersion atomic.Uint64`, increment in all DDL funcs
2. `pkg/engine/database.go` — Add `schemaCache`, `getCachedTableDef(sql string)`
3. `pkg/catalog/catalog_insert.go` — Use cached `TableDef` for column resolution when cache hit
4. `pkg/catalog/catalog_update.go` — Same
5. `pkg/catalog/catalog_delete.go` — Same

### Acceptance Criteria
- Autocommit INSERT p99 latency < 200μs (currently ~500μs estimated)
- Cache hit rate > 95% in OLTP workload (same table, same columns)
- `go test -race ./pkg/engine/...` passes

---

## Phase 3: Online DDL (Week 3-4)

### Goal
`CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX` no longer block reads/writes for seconds.

### Current State
All DDL acquires `cat.mu.Lock()` for the entire operation:
```go
func (c *Catalog) CreateTable(...) {
    c.mu.Lock()
    defer c.mu.Unlock()
    // ... allocates pages, writes metadata, builds indexes ...
}
```

### Proposed Change: Schema-Versioned Online DDL

1. **Schema versioning**: Every DDL increments `schemaVersion` atomically.
2. **Lazy index builds**: `CREATE INDEX` marks the index as `building` in metadata, returns immediately. A background job populates the index.
3. **Two-phase DDL**:
   - Phase A (under `cat.mu.Lock()`, <1ms): Update metadata only (maps, defs). Do NOT touch B-trees.
   - Phase B (lock-free): Background goroutine builds/drops physical structures.
4. **Ghost table pattern for ALTER TABLE** (like MySQL pt-online-schema-change):
   - Create new table with new schema
   - Background job copies rows in batches
   - Brief `cat.mu.Lock()` (<10ms) to swap table pointers atomically

### Files to Change
1. `pkg/catalog/catalog_ddl.go` — All `CreateTable`, `DropTable`, `CreateIndex`, `AlterTable`
2. `pkg/catalog/catalog_core.go` — `IndexDef.Status` enum: `IndexActive`, `IndexBuilding`, `IndexDropping`
3. `pkg/scheduler/scheduler.go` — Register `buildIndexJob`, `dropTableCleanupJob`
4. `pkg/catalog/catalog_index.go` — `scanTableRows` skips `IndexBuilding` indexes

### Acceptance Criteria
- `CREATE INDEX` on 1M-row table completes in <100ms (main thread), background finishes in <10s
- `ALTER TABLE ADD COLUMN` does not block concurrent SELECT/INSERT
- `go test ./pkg/catalog/ -run TestDDL` passes

---

## Phase 4: BTree Page-Level Latching (Week 4-6)

### Goal
Multiple transactions can write to different keys in the same table concurrently.

### Current State
BTree has 256 shards with per-shard `sync.RWMutex`, but within a shard, all operations are serialized. For tables with sequential PKs (e.g., auto-increment), all writes hit the same shard.

### Proposed Change: Fine-Grained Page Latching (SQLite B-tree model)

Replace shard mutex with per-page read/write latches:
- **Read latch**: `sync.RWMutex` on each `BTreeNode`
- **Write latch**: Exclusive lock on leaf node + intent locks on ancestors
- **Crabbing**: Lock parent, find child, lock child, unlock parent (top-down)
- **Leaf page split**: Lock parent exclusively, then split

```go
// pkg/btree/node.go
type BTreeNode struct {
    // ... existing fields ...
    latch sync.RWMutex
}

// pkg/btree/btree.go
func (t *BTree) Put(key, value []byte) error {
    // crabbing traversal
    // lock root -> read
    // find child -> lock child -> unlock root
    // ... until leaf
    // lock leaf -> write
    // insert
    // if split: lock parent exclusively, propagate
}
```

### Files to Change
1. `pkg/btree/node.go` — Add `latch sync.RWMutex` to `BTreeNode`
2. `pkg/btree/btree.go` — Rewrite `Put`, `Get`, `Delete` with crabbing protocol
3. `pkg/btree/btree.go` — `PutBatch` / `DeleteBatch` must acquire all leaf latches in sorted order to avoid deadlock
4. `pkg/btree/btree_test.go` — Concurrent stress test with `runtime.GOMAXPROCS` goroutines

### Risk
- Deadlock if latch acquisition order is not strictly defined
- Memory overhead: 256 shards × ~100 nodes = ~25K mutexes (acceptable)

### Acceptance Criteria
- `BenchmarkConcurrentWritersScaleSmallTxn/workers=16` scales to >4x (currently 1.2x)
- `go test -race ./pkg/btree/...` passes
- No deadlock in 60-second stress test

---

## Phase 5: Flat Row Format (Week 6-8)

### Goal
Eliminate the ~2 allocs/row caused by `[]interface{}` boxing.

### Current State
```go
// Every row decoded:
rowData := make([]interface{}, 0, numCols)  // 1 alloc
vrow, _ := decodeVersionedRowFast(data, numCols, rowData)
// If string: stringStruct alloc (16 bytes)
// If int64 > 255: 8-byte heap alloc
```

### Proposed Change: Type-Specific Row Storage

**Option A: FlatBuffer (minimal change)**
```go
type FlatRow struct {
    NumCols    uint16
    NullBitmap [4]uint64  // up to 256 columns
    Int64s     []int64    // slice header (3 words), data on heap only if > 0
    Float64s   []float64
    Strings    []string   // each string still boxes, but fewer allocs overall
    Blobs      [][]byte
}
```

**Option B: Columnar Store (max performance)**
```go
type ColumnBlock struct {
    Name     string
    Type     ColumnType
    Int64s   []int64
    Strings  []string
    Validity []uint64  // bitmap
}

type TableStore struct {
    Columns []ColumnBlock
    Rows    int
}
```
Columnar is 10-50x faster for analytical scans but harder for OLTP updates.

### Recommended: Hybrid Approach
- **OLTP tables**: FlatBuffer row format (simpler updates)
- **Analytical / read-heavy tables**: Columnar (opt-in via `CREATE TABLE ... STORE=columnar`)

### Files to Change
1. `pkg/catalog/temporal.go` — `decodeVersionedRowFast` → `decodeVersionedRowFlat`
2. `pkg/catalog/temporal.go` — `encodeVersionedRowFast` → `encodeVersionedRowFlat`
3. `pkg/catalog/catalog_insert.go` — Build `FlatRow` directly from args
4. `pkg/catalog/catalog_select.go` — Return `[][]interface{}` for backward compat, but internally use `FlatRow`
5. `pkg/engine/database.go` — `Rows.Scan` must handle `FlatRow` without reflection

### Acceptance Criteria
- `BenchmarkDecodeScanRow` < 50ns/row (currently 204ns)
- `BenchmarkSelectWithScan` allocs/op < 100 for 1000 rows (currently ~1750)
- All existing tests pass without API change

---

## Phase 6: Async WAL + Epoch Commit (Week 8-10)

### Goal
Remove fsync from the commit critical path.

### Current State
```go
// pkg/txn/manager.go commitWithConflictDetection
// 1. Lock version shards
// 2. Validate reads
// 3. Update versions
// 4. Unlock version shards  <-- good: allows concurrent commits
// 5. WAL Append + fsync      <-- bad: still serializes
```

### Proposed Change: Group Commit Leader

```go
// pkg/storage/wal.go
 type WAL struct {
     // ... existing ...
     commitQueue   chan *commitRequest
     epochTicker   *time.Ticker  // 1ms
     epochLeader   sync.Mutex    // only leader fsyncs
 }

 func (w *WAL) AsyncAppend(records []*WALRecord) (commitToken, error) {
     // Write to bufio, return token immediately
     // Do NOT fsync
 }

 func (w *WAL) CommitEpoch() {
     // Leader: flush bufio, fsync once
     // All transactions in this epoch are now durable
     // Notify waiters
 }
```

Transactions append to WAL buffer without fsync. A background goroutine (or the first committer) becomes the "epoch leader" and fsyncs once per millisecond for all transactions in that epoch.

### Files to Change
1. `pkg/storage/wal.go` — Add `commitQueue`, `epochTicker`, `AsyncAppend()`, `CommitEpoch()`
2. `pkg/txn/manager.go` — Replace `wal.AppendBatchWithoutSync` + implicit fsync with `AsyncAppend` + wait on epoch
3. `pkg/engine/database.go` — `Options.GroupCommitInterval` (default 1ms, `0` = sync)

### Acceptance Criteria
- `BenchmarkConcurrentWritersScaleSmallTxn` throughput > 500K ops/sec at 16 workers
- No data loss on simulated crash (power failure test with `kill -9`)
- `SyncMode=SyncFull` still fsyncs immediately (backward compat)

---

## Cross-Cutting Concerns

### Testing Strategy
Every phase must include:
1. **Race tests**: `CGO_ENABLED=1 go test -race ./pkg/...`
2. **Stress tests**: `TestStress_ConcurrentReadsWrites` with 10+ workers, 10+ seconds
3. **ACID tests**: `TestACID_RollbackConsistency`, `TestACID_CommitDurability`
4. **Benchmark regression**: `benchstat` against `main` baseline

### Memory Model
- Go 1.26 `atomic` types preferred over `sync/atomic` funcs
- `atomic.Pointer[T]` for lock-free snapshots
- `sync.Map` only where key set is unbounded (e.g., connection tracking)

### Backward Compatibility
- `engine.Open(path, opts)` API unchanged
- MySQL wire protocol unchanged
- On-disk format: Phase 5 (FlatRow) requires migration or dual-read path

---

## Timeline & Milestones

| Phase | Duration | Deliverable | Owner |
|-------|----------|-------------|-------|
| 1 | Week 1-2 | SELECT without cat.mu | TBD |
| 2 | Week 2-3 | Schema cache | TBD |
| 3 | Week 3-4 | Online DDL | TBD |
| 4 | Week 4-6 | Page-level latching | TBD |
| 5 | Week 6-8 | Flat row format | TBD |
| 6 | Week 8-10 | Async WAL | TBD |

**Total: ~2.5 months for full SQLite-level concurrency.**

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-05-12 | Do NOT replace `[]interface{}` with generics | Generic `Row[T any]` breaks MySQL wire protocol `interface{}` contract. FlatRow is the pragmatic middle ground. |
| 2026-05-12 | Keep `commitMu[64]` + version shards | Already scales well for large txns. Page latching is the next bottleneck. |
| 2026-05-12 | Skip SSI (Serializable Snapshot Isolation) for now | Snapshot Isolation is sufficient for 95% of apps. SSI adds ~20% commit overhead. |

---

## References

- `docs/MUTEX_GRANULARIZATION_PLAN.md` — Earlier mutex audit (2026-05-05)
- `docs/MVCC_MULTIWRITER_PLAN.md` — MVCC architecture design (2026-05-05)
- `pkg/txn/manager.go` — OCC commit path (`commitWithConflictDetection`)
- `pkg/catalog/catalog_txn.go` — Buffered write mode (`isBufferedMode`, `pendingWrites`)
- `pkg/btree/btree.go` — Current 256-shard locking
- SQLite B-tree latching: https://sqlite.org/lockingv3.html
- CockroachDB parallel commits: https://www.cockroachlabs.com/blog/parallel-commits/
