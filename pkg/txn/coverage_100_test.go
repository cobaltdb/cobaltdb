package txn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestTxnRemainingStateHelpers(t *testing.T) {
	m := NewManager(nil)
	tx := m.Begin(nil)
	tx.mu.Lock()
	tx.State = TxnState(99)
	tx.mu.Unlock()
	if err := tx.AddLockHeldIfActive("k"); !errors.Is(err, ErrTxnNotFound) {
		t.Fatalf("unknown add state: %v", err)
	}
	if err := tx.activeStateErrorForID(tx.ID); !errors.Is(err, ErrTxnNotFound) {
		t.Fatalf("unknown active state: %v", err)
	}

	for state, want := range map[TxnState]error{TxnCommitted: ErrTxnCommitted, TxnAborted: ErrTxnAborted} {
		tx.mu.Lock()
		tx.State = state
		tx.mu.Unlock()
		if err := tx.activeStateErrorForID(tx.ID); !errors.Is(err, want) {
			t.Errorf("state %d: %v", state, err)
		}
	}
}

func TestTxnRemainingCommitAndRecyclePaths(t *testing.T) {
	m := NewManager(nil)
	// Exercise the periodic pruning call without requiring 1000 commits.
	m.commitCount.Store(999)
	tx := m.Begin(nil)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	timed := m.BeginWithContext(ctx, &Options{Isolation: SnapshotIsolation})
	cancel()
	if err := timed.Commit(); !errors.Is(err, ErrTxnCancelled) {
		t.Fatalf("cancel commit: %v", err)
	}

	withTimeout := m.BeginWithContext(context.Background(), &Options{Isolation: SnapshotIsolation, Timeout: time.Hour})
	if withTimeout.cancel == nil {
		t.Fatal("BeginWithContext timeout did not install cancel")
	}
	withTimeout.Recycle()

	pooled := m.Begin(nil)
	pooled.ReadSet = nil
	pooled.WriteSet = nil
	pooled.locksHeld = map[string]bool{"held": true}
	pooled.Recycle()
	if got := m.acquireTxn(); got == nil {
		t.Fatal("pool returned nil")
	}
}

func TestTxnRemainingDeadlockDefenses(t *testing.T) {
	m := NewManager(nil)
	m.resolveDeadlock(nil, nil)

	stale := m.Begin(nil)
	oldID := stale.ID
	shard := activeShardIdx(oldID)
	m.activeShards[shard].Lock()
	m.activeShards[shard].m[oldID] = stale
	m.activeShards[shard].Unlock()
	stale.mu.Lock()
	stale.ID++
	stale.waitingFor = 123
	stale.mu.Unlock()
	m.checkForDeadlocks()
	m.removeActive(oldID)
	_ = stale.Rollback()

	// A cycle that points to an absent snapshot entry records the deadlock but has no victim.
	m.resolveDeadlock([]uint64{999}, map[uint64]*Transaction{})

	// Existing victim with a stale ID is ignored at the final revalidation.
	victim := m.Begin(nil)
	victimID := victim.ID
	active := map[uint64]*Transaction{victimID: victim}
	victim.StartTS = 10
	m.removeActive(victimID)
	m.resolveDeadlock([]uint64{victimID}, active)
	_ = victim.Rollback()
}

func TestTxnWaiterAcquiresSharedAfterExclusiveRelease(t *testing.T) {
	m := NewManager(nil)
	holder, waiter := m.Begin(nil), m.Begin(nil)
	if err := m.AcquireLock(holder.ID, "shared-after-release", time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.AcquireLockMode(waiter.ID, "shared-after-release", LockShared, time.Second) }()
	for waiter.GetWaitingFor() != holder.ID {
		time.Sleep(time.Millisecond)
	}
	m.ReleaseLock(holder.ID, "shared-after-release")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = holder.Rollback()
	_ = waiter.Rollback()
}

func TestTxnImmediateDeadlockRejection(t *testing.T) {
	m := NewManager(nil)
	a, b := m.Begin(nil), m.Begin(nil)
	if err := m.AcquireLock(a.ID, "a", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := m.AcquireLock(b.ID, "b", time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.AcquireLock(b.ID, "a", time.Second) }()
	for b.GetWaitingFor() != a.ID {
		time.Sleep(time.Millisecond)
	}
	if err := m.AcquireLock(a.ID, "b", time.Second); !errors.Is(err, ErrDeadlockDetected) {
		t.Fatalf("deadlock precheck: %v", err)
	}
	_ = a.Rollback()
	_ = b.Rollback()
	<-done
}

func TestTxnAcquireBlockedInactiveWaiter(t *testing.T) {
	m := NewManager(nil)
	tx := m.Begin(nil)
	tx.mu.Lock()
	tx.State = TxnAborted
	tx.mu.Unlock()
	m.lockMu.Lock()
	m.lockEntries["blocked-inactive"] = &lockEntry{shared: map[uint64]bool{}, exclusive: 999}
	m.lockMu.Unlock()
	if err := m.AcquireLock(tx.ID, "blocked-inactive", time.Second); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("blocked inactive waiter: %v", err)
	}
}

func TestTxnLockHelperFailureContracts(t *testing.T) {
	m := NewManager(nil)
	tx := m.Begin(nil)
	tx.SetWaitingFor(42)
	tx.mu.Lock()
	tx.State = TxnAborted
	tx.mu.Unlock()
	if err := validateLockWaiter(tx, tx.ID); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("validate waiter: %v", err)
	}
	if tx.GetWaitingFor() != 0 {
		t.Fatal("failed waiter retained edge")
	}
	m.lockMu.Lock()
	m.lockEntries["granted"] = &lockEntry{shared: map[uint64]bool{}, exclusive: tx.ID}
	m.lockMu.Unlock()
	if err := m.recordGrantedLock(tx, tx.ID, "granted"); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("record granted: %v", err)
	}
	m.lockMu.RLock()
	_, exists := m.lockEntries["granted"]
	m.lockMu.RUnlock()
	if exists {
		t.Fatal("failed grant leaked lock entry")
	}
}

func TestTxnCommitMetricDurations(t *testing.T) {
	recordTxnCommitMetrics(0)
	recordTxnCommitMetrics(2 * time.Second)
}

func TestTxnRemainingLockPaths(t *testing.T) {
	m := NewManager(nil)
	timed := m.Begin(&Options{Timeout: time.Nanosecond})
	time.Sleep(time.Millisecond)
	if err := m.AcquireLock(timed.ID, "timed", time.Second); !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("timed lock: %v", err)
	}
	_ = timed.Rollback()

	fallback := m.Begin(&Options{Isolation: SnapshotIsolation, LockWaitTimeout: 0})
	if err := m.AcquireLock(fallback.ID, "fallback", 0); err != nil {
		t.Fatal(err)
	}
	// Reacquiring exclusive exercises the already-owner path.
	if err := m.AcquireLock(fallback.ID, "fallback", time.Second); err != nil {
		t.Fatal(err)
	}
	m.ReleaseLock(fallback.ID, "missing")
	m.ReleaseLock(999, "fallback")
	_ = fallback.Rollback()

	if got := lockEntryBlocker(&lockEntry{shared: map[uint64]bool{1: true}}, 1); got != 0 {
		t.Fatalf("self shared blocker=%d", got)
	}

	// wouldCauseDeadlock terminates safely when it encounters a cycle not involving requester.
	a, b := m.Begin(nil), m.Begin(nil)
	a.SetWaitingFor(b.ID)
	b.SetWaitingFor(a.ID)
	if m.wouldCauseDeadlock(999, a.ID) {
		t.Fatal("unrelated cycle reported as requester deadlock")
	}
	// Stale pointer ID terminates the chain.
	old := a.ID
	a.mu.Lock()
	a.ID += 100
	a.mu.Unlock()
	if m.wouldCauseDeadlock(999, old) {
		t.Fatal("stale pointer reported deadlock")
	}
	m.removeActive(old)
	_ = a.Rollback()
	_ = b.Rollback()

	m.releaseLockEntries(1, nil)
	m.releaseLockEntries(1, []string{"absent"})
}

func distinctShardKeys(n int) []string {
	seen := map[int]bool{}
	keys := make([]string, 0, n)
	for i := 0; len(keys) < n; i++ {
		key := fmt.Sprintf("k%d", i)
		s := versionShardIdx("t", key)
		if !seen[s] {
			seen[s] = true
			keys = append(keys, key)
		}
	}
	return keys
}

func TestTxnRemainingGeneralCommitPaths(t *testing.T) {
	m := NewManager(nil)
	// More than eight distinct shards exercises the spill slice and its duplicate scan.
	tx := m.Begin(nil)
	keys := distinctShardKeys(10)
	for _, key := range keys {
		tx.SetReadVersion("t", key, 0)
		tx.SetWrite("t", key, []byte("v"))
	}
	// Duplicate shard inputs make both duplicate checks observable.
	tx.SetReadVersion("t", keys[0]+"x", 0)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// General-path conflict (read and write differ) unlocks all collected shards.
	seed := m.Begin(nil)
	seed.SetWrite("t", "conflict", []byte("v"))
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	conflict := m.Begin(nil)
	conflict.SetReadVersion("t", "conflict", 0)
	conflict.SetWrite("t", "other", []byte("v"))
	if err := conflict.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("general conflict: %v", err)
	}
}

func TestTxnRemainingWALPaths(t *testing.T) {
	t.Run("wrong WAL type", func(t *testing.T) {
		m := NewManager(struct{}{})
		tx := m.Begin(nil)
		tx.SetWrite("t", "k", []byte("v"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("large single record", func(t *testing.T) {
		wal, err := storage.OpenWAL(t.TempDir() + "/wal")
		if err != nil {
			t.Fatal(err)
		}
		defer wal.Close()
		m := NewManager(wal)
		tx := m.Begin(nil)
		tx.SetWrite("tree", "key", []byte(strings.Repeat("x", 300)))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("single read-write append failure", func(t *testing.T) {
		wal, err := storage.OpenWAL(t.TempDir() + "/wal")
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		m := NewManager(wal)
		tx := m.Begin(nil)
		tx.SetReadVersion("t", "k", 0)
		tx.SetWrite("t", "k", []byte("v"))
		if err := tx.Commit(); err == nil {
			t.Fatal("expected closed WAL read-write failure")
		}
	})

	t.Run("single append failure", func(t *testing.T) {
		wal, err := storage.OpenWAL(t.TempDir() + "/wal")
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		m := NewManager(wal)
		tx := m.Begin(nil)
		tx.SetWrite("t", "k", []byte("v"))
		if err := tx.Commit(); err == nil {
			t.Fatal("expected closed WAL failure")
		}
	})

	t.Run("batch append failure", func(t *testing.T) {
		wal, err := storage.OpenWAL(t.TempDir() + "/wal")
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		m := NewManager(wal)
		tx := m.Begin(nil)
		tx.SetWrite("t", "a", []byte("v"))
		tx.SetWrite("t", "b", []byte("v"))
		if err := tx.Commit(); err == nil {
			t.Fatal("expected closed WAL batch failure")
		}
	})

	t.Run("oversize single and batch", func(t *testing.T) {
		wal, err := storage.OpenWAL(t.TempDir() + "/wal")
		if err != nil {
			t.Fatal(err)
		}
		defer wal.Close()
		m := NewManager(wal)
		one := m.Begin(nil)
		one.SetWrite("t", "k", make([]byte, maxTxnWALRecordDataBytes))
		if err := one.Commit(); err == nil {
			t.Fatal("expected oversize single error")
		}
		batch := m.Begin(nil)
		batch.SetWrite("t", "a", make([]byte, maxTxnWALRecordDataBytes))
		batch.SetWrite("t", "b", nil)
		if err := batch.Commit(); err == nil {
			t.Fatal("expected oversize batch error")
		}
	})
}

func TestTxnRemainingVersionStorePaths(t *testing.T) {
	vs := NewVersionStore()
	vs.Delete(WriteKey{Key: "deleted"}, 1)
	if _, err := vs.GetCurrent(WriteKey{Key: "deleted"}); !errors.Is(err, ErrKeyDeleted) {
		t.Fatalf("delete: %v", err)
	}

	// Explicit nil and singleton heads exercise Prune's skip guard.
	vs.mu.Lock()
	vs.versions[WriteKey{Key: "nil"}] = nil
	vs.versions[WriteKey{Key: "single"}] = &VersionedValue{Version: 1}
	vs.mu.Unlock()
	if got := vs.Prune(2); got != 0 {
		t.Fatalf("unexpected prune count %d", got)
	}
}

func TestTxnCommitSlowMetricBranchIsNotRequiredForCorrectness(t *testing.T) {
	// Keep atomic import and the commit sequence invariant explicitly exercised.
	m := NewManager(nil)
	before := atomic.LoadUint64(&m.commitSeq)
	tx := m.Begin(nil)
	tx.SetWrite("t", "k", nil)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadUint64(&m.commitSeq) <= before {
		t.Fatal("commit sequence did not advance")
	}
}
