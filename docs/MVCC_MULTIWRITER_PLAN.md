# Single-Writer to Multi-Writer MVCC Plan

## Current State (2026-05-05)

CobaltDB enforces a **single-writer model**:
- `Catalog.mu.Lock()` is held for the entire duration of `Insert`, `Update`, `Delete`.
- Only one write transaction can proceed at a time.
- Long-running `SELECT` statements (with `RLock()`) block all writes.

The MVCC version store (`pkg/txn/version_store.go`) already exists:
- `Commit(key, value, commitTS)` adds a version.
- `GetAtSnapshot(key, snapshotTS)` reads a consistent snapshot.
- `Prune(minActiveTS)` garbage-collects old versions.

However, this is currently used only for **snapshot isolation of readers** against committed writes, not for concurrent writers.

## Problem

1. **Write throughput ceiling**: Even on multi-core machines, write TPS is capped by a single goroutine.
2. **Read-write interference**: A `SELECT COUNT(*)` over a large table blocks all INSERTs/UPDATEs.
3. **Not production-ready for high-concurrency OLTP**: The single mutex becomes the dominant bottleneck.

## Proposed Architecture

### Phase 1: Optimistic Concurrency Control (OCC) for Writers (Target: v3.0)

Allow multiple transactions to acquire "write intents" concurrently, then validate at commit time.

**Key data structures:**

```go
// WriteIntent marks a key as being modified by an active transaction
type WriteIntent struct {
    TxnID     uint64
    StartTS   uint64
    Key       string
}

// TxnManager enhancements
type TransactionManager struct {
    // ... existing fields ...
    activeTxns   map[uint64]*ActiveTxn  // tracks running write transactions
    intentStore  map[string]*WriteIntent // key -> write intent
    commitTSGen  atomic.Uint64
}
```

**Write transaction lifecycle:**

1. **Begin**: Assign `startTS` and `txnID`. Register in `activeTxns`.
2. **Read**: Use `versionStore.GetAtSnapshot(key, startTS)` for snapshot reads.
3. **Write**: Record intent in `intentStore` (no immediate tree mutation). Buffer writes in per-txn write set.
4. **Commit validation** (critical section):
   - Acquire `Catalog.mu.Lock()` briefly.
   - Check if any committed transaction (with `commitTS > startTS`) modified keys in our write set.
   - If conflict: abort and retry.
   - If no conflict: assign `commitTS`, write versions to `versionStore`, apply to B-tree, remove intents.
   - Release `Catalog.mu.Lock()`.
5. **Rollback**: Remove intents, discard write set.

**Conflict detection:**
- Write-Write: Two txns modify the same key. Last committer wins; earlier txn gets `ErrWriteConflict`.
- Write-Read: No conflict (readers use snapshot isolation).
- Phantom reads: Prevented by range locks or serializable validation (track read ranges and validate no new writes in range).

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
