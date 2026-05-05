# Single-Writer to Multi-Writer MVCC Plan

## Current State (2026-05-05)

CobaltDB enforces a **single-writer model**:
- `Catalog.mu.Lock()` is held for the entire duration of `Insert`, `Update`, `Delete`.
- Only one write transaction can proceed at a time.
- Long-running `SELECT` statements (with `RLock()`) block all writes.

**Important finding**: `pkg/txn/manager.go` already contains substantial MVCC infrastructure:
- `Transaction.ReadSet` and `Transaction.WriteSet` track per-transaction reads/writes
- `Manager.detectConflicts()` validates snapshot isolation at commit time
- `Manager.applyWrites()` commits buffered writes to the `VersionStore` and WAL
- `VersionStore` (`pkg/txn/version_store.go`) maintains per-key version chains

The limitation is **not** missing MVCC infrastructure but rather the **Catalog layer** forcing single-writer serialization via `Catalog.mu.Lock()`. All DML operations (`Insert`, `Update`, `Delete`) mutate B-trees directly under the catalog lock instead of buffering writes in `Transaction.WriteSet` and delegating application to `Manager.applyWrites()`.

## Problem

1. **Write throughput ceiling**: Even on multi-core machines, write TPS is capped by a single goroutine.
2. **Read-write interference**: A `SELECT COUNT(*)` over a large table blocks all INSERTs/UPDATEs.
3. **Not production-ready for high-concurrency OLTP**: The single mutex becomes the dominant bottleneck.

## Proposed Architecture

### Phase 1: Catalog Refactor to Use Existing MVCC (Target: v3.0)

The `txn.Manager` already supports OCC. The work is to refactor the `Catalog` to use it.

**Catalog changes needed:**

1. **Per-transaction undo logs**: Move `undoLog`, `savepoints`, `txnID`, `txnActive` from `Catalog` fields into a `catalogTxnState` struct. `Catalog` tracks `activeTxns map[uint64]*catalogTxnState`.
2. **Buffer writes in DML**: 
   - `Insert`/`Update`/`Delete` should build the new row/value, then add it to `txn.WriteSet` keyed by `"table:pk"`.
   - Record undo information in the per-txn undo log.
   - Do NOT call `tree.Put()` or `tree.Delete()` directly.
3. **Snapshot reads in SELECT**:
   - `scanTableRows` should read from `versionStore.GetAtSnapshot(key, txn.StartTS)` when inside a transaction.
   - Fall back to `tree.Get()` for committed data.
4. **Commit through Manager**:
   - `Catalog.CommitTransaction(txnID)` calls `txn.Commit()` on the `txn.Manager`.
   - `Manager.detectConflicts()` validates `ReadSet` against `m.versions`.
   - `Manager.applyWrites()` applies `WriteSet` to B-trees and WAL.
   - On success, replay per-txn undo log on rollback.

**Conflict detection** (already implemented in `txn.Manager`):
- Write-Write: `detectConflicts` checks if `ReadSet` versions changed.
- Write-Read: No conflict (readers use snapshot isolation via `VersionStore`).
- Phantom reads: Requires tracking read ranges in `ReadSet` (enhancement needed).

### Phase 2: Lock-Free Version Store (Target: v3.1)

Replace the current mutex-protected version store with a lock-free structure:
- Per-key version chains stored in `sync.Map` or sharded maps.
- Reduces contention on the central version store during heavy write workloads.

### Phase 3: Serializable Snapshot Isolation (SSI) (Target: v3.2)

Add SSI for true serializable transactions:
- Track read sets and write sets per transaction.
- On commit, verify that no committed transaction wrote keys in our read set (rw-conflict) or read keys in our write set (wr-conflict).
- Abort on serializable violation.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Deadlocks between intent acquisition | Enforce key ordering during intent phase |
| Starvation of long-running writers | Add max retry count; fall back to single-writer queue after N retries |
| Garbage accumulation in version store | Run `Prune` more aggressively; add background GC goroutine |
| Breaking existing transaction semantics | Maintain `BEGIN`/`COMMIT`/`ROLLBACK` API unchanged; internal refactor only |
| WAL ordering changes | Ensure `commitTS` monotonicity matches WAL append order |

## Acceptance Criteria
- `go test ./pkg/... -race` passes.
- `TestDeadlockDetection` continues to pass (or is replaced with multi-writer deadlock tests).
- New benchmark `BenchmarkConcurrentWriters` shows linear scaling up to `runtime.NumCPU()` writers.
- No regression in existing ACID tests (`TestACID_RollbackConsistency`, `TestACID_CommitDurability`).

## References
- `pkg/txn/version_store.go` — existing version chain implementation
- `pkg/catalog/catalog_txn.go` — transaction manager
- `pkg/storage/wal.go` — WAL for durability ordering
- Spanner / CockroachDB OCC design papers for validation algorithms
