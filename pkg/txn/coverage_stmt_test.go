package txn

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetVersionStore(t *testing.T) {
	mgr := NewManager(nil)
	vs := mgr.GetVersionStore()
	if vs == nil {
		t.Fatal("expected non-nil version store")
	}

	// Verify it's usable
	vs.Commit(WriteKey{Key: "key1"}, []byte("val1"), 1)
	val, err := vs.GetCurrent(WriteKey{Key: "key1"})
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("expected val1, got %s", val)
	}
}

func TestAcquireLockModeShared(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)

	// txn1 acquires shared lock
	err := mgr.AcquireLockMode(txn1.ID, "row1", LockShared, time.Second)
	if err != nil {
		t.Fatalf("shared lock acquire: %v", err)
	}

	// txn2 should also be able to acquire shared lock (shared-compatible)
	err = mgr.AcquireLockMode(txn2.ID, "row1", LockShared, time.Second)
	if err != nil {
		t.Fatalf("second shared lock acquire: %v", err)
	}

	mgr.ReleaseAllLocks(txn1.ID)
	mgr.ReleaseAllLocks(txn2.ID)
}

func TestAcquireLockModeSharedBlockedByExclusive(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)

	// txn1 acquires exclusive lock
	err := mgr.AcquireLockMode(txn1.ID, "row1", LockExclusive, time.Second)
	if err != nil {
		t.Fatalf("exclusive lock acquire: %v", err)
	}

	// txn2 tries shared lock — should timeout
	err = mgr.AcquireLockMode(txn2.ID, "row1", LockShared, 50*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error for shared lock blocked by exclusive")
	}

	mgr.ReleaseAllLocks(txn1.ID)
	mgr.ReleaseAllLocks(txn2.ID)
}

func TestAcquireLockModeReturnsOnTransactionTimeout(t *testing.T) {
	mgr := NewManager(nil)
	holder := mgr.Begin(nil)
	waiter := mgr.Begin(&Options{Timeout: 25 * time.Millisecond})

	if err := mgr.AcquireLock(holder.ID, "row-timeout", time.Second); err != nil {
		t.Fatalf("holder acquire lock: %v", err)
	}

	start := time.Now()
	err := mgr.AcquireLock(waiter.ID, "row-timeout", time.Second)
	if !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("expected ErrTxnTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("lock wait ignored transaction timeout; elapsed %s", elapsed)
	}
	if waitingFor := waiter.GetWaitingFor(); waitingFor != 0 {
		t.Fatalf("transaction timeout left waitingFor=%d", waitingFor)
	}

	if err := holder.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
	if err := waiter.Rollback(); err != nil {
		t.Fatalf("waiter rollback: %v", err)
	}
}

func TestAcquireLockModeReturnsWhenWaitingTransactionAborts(t *testing.T) {
	mgr := NewManager(nil)
	holder := mgr.Begin(nil)
	waiter := mgr.Begin(nil)

	if err := mgr.AcquireLock(holder.ID, "row-abort", time.Second); err != nil {
		t.Fatalf("holder acquire lock: %v", err)
	}

	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		errCh <- mgr.AcquireLock(waiter.ID, "row-abort", time.Second)
	}()

	deadline := time.After(250 * time.Millisecond)
	for waiter.GetWaitingFor() != holder.ID {
		select {
		case err := <-errCh:
			t.Fatalf("lock wait returned before entering wait state: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for waiter to enter lock wait state")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if err := waiter.Rollback(); err != nil {
		t.Fatalf("waiter rollback: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrTxnAborted) {
			t.Fatalf("expected ErrTxnAborted, got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("lock wait did not return promptly after abort; elapsed %s", time.Since(start))
	}

	if err := holder.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
}

func TestAcquireLockModeLockUpgrade(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)

	// First acquire shared
	err := mgr.AcquireLockMode(txn1.ID, "row1", LockShared, time.Second)
	if err != nil {
		t.Fatalf("shared lock: %v", err)
	}

	// Then upgrade to exclusive (we hold the only shared lock)
	err = mgr.AcquireLockMode(txn1.ID, "row1", LockExclusive, time.Second)
	if err != nil {
		t.Fatalf("lock upgrade: %v", err)
	}

	mgr.ReleaseAllLocks(txn1.ID)
}

func TestAcquireLockModeExclusiveBlockedByShared(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)

	// txn1 acquires shared lock
	err := mgr.AcquireLockMode(txn1.ID, "row1", LockShared, time.Second)
	if err != nil {
		t.Fatalf("shared lock: %v", err)
	}

	// txn2 tries exclusive — blocked by txn1's shared lock
	err = mgr.AcquireLockMode(txn2.ID, "row1", LockExclusive, 50*time.Millisecond)
	if err == nil {
		t.Error("expected timeout for exclusive blocked by shared")
	}

	mgr.ReleaseAllLocks(txn1.ID)
	mgr.ReleaseAllLocks(txn2.ID)
}

func TestReleaseAllLocksForInactiveTxn(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	// Acquire some locks
	mgr.AcquireLock(txn.ID, "key1", time.Second)
	mgr.AcquireLock(txn.ID, "key2", time.Second)

	// Manually remove from active so ReleaseAllLocks takes the inactive path
	shard := activeShardIdx(txn.ID)
	mgr.activeShards[shard].Lock()
	delete(mgr.activeShards[shard].m, txn.ID)
	mgr.activeShards[shard].Unlock()

	// Should not panic
	mgr.ReleaseAllLocks(txn.ID)
}

func TestIsTimedOutDetailed(t *testing.T) {
	t.Run("NoContext", func(t *testing.T) {
		txn := &Transaction{}
		if txn.IsTimedOut() {
			t.Error("should not be timed out with nil ctx")
		}
	})

	t.Run("ExpiredContext", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(2 * time.Nanosecond)

		txn := &Transaction{ctx: ctx}
		if !txn.IsTimedOut() {
			t.Error("should be timed out")
		}
	})

	t.Run("ActiveContext", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		txn := &Transaction{ctx: ctx}
		if txn.IsTimedOut() {
			t.Error("should not be timed out")
		}
	})
}

func TestCommitWithTimeout(t *testing.T) {
	mgr := NewManager(nil)
	opts := &Options{Timeout: 10 * time.Second}
	txn := mgr.Begin(opts)

	// Add a write
	txn.SetWrite("", "key1", []byte("val1"))

	err := txn.Commit()
	if err != nil {
		t.Fatalf("commit with timeout: %v", err)
	}
	if txn.State != TxnCommitted {
		t.Errorf("expected committed, got %d", txn.State)
	}
}

func TestReadOnlyTransactionRejectsWritesOnCommit(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(&Options{ReadOnly: true})
	txn.SetWrite("", "readonly-key", []byte("value"))

	err := txn.Commit()
	if !errors.Is(err, ErrReadOnlyTxn) {
		t.Fatalf("expected ErrReadOnlyTxn, got %v", err)
	}
	if txn.State != TxnAborted {
		t.Fatalf("read-only write commit should abort transaction, got state %d", txn.State)
	}
	if _, err := mgr.Get(txn.ID); !errors.Is(err, ErrTxnNotFound) {
		t.Fatalf("read-only write transaction should be removed from active set, got %v", err)
	}
	if got := mgr.GetCurrentVersion("", "readonly-key"); got != 0 {
		t.Fatalf("read-only write commit published version %d", got)
	}
}

func TestAcquireLockModeTxnNotFound(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)

	// txn1 holds exclusive lock
	mgr.AcquireLockMode(txn1.ID, "key1", LockExclusive, time.Second)

	// Non-existent txn tries to acquire — will enter blocking path, then fail to find itself in active
	err := mgr.AcquireLockMode(99999, "key1", LockExclusive, 50*time.Millisecond)
	if err == nil {
		t.Error("expected error for non-existent txn")
	}
}

func TestAcquireLockRejectsInactiveTxnWithoutLeakingLock(t *testing.T) {
	mgr := NewManager(nil)

	if err := mgr.AcquireLock(99999, "empty-key", time.Second); err != ErrTxnNotFound {
		t.Fatalf("expected ErrTxnNotFound for inactive transaction, got %v", err)
	}

	txn := mgr.Begin(nil)
	if err := mgr.AcquireLock(txn.ID, "empty-key", 10*time.Millisecond); err != nil {
		t.Fatalf("active transaction should acquire key after rejected inactive lock: %v", err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestAcquireLockNoWaitFailureClearsWaitingFor(t *testing.T) {
	mgr := NewManager(nil)
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)

	if err := mgr.AcquireLock(txn1.ID, "key1", time.Second); err != nil {
		t.Fatalf("txn1 acquire key1: %v", err)
	}
	if err := mgr.AcquireLock(txn2.ID, "key2", time.Second); err != nil {
		t.Fatalf("txn2 acquire key2: %v", err)
	}

	if err := mgr.AcquireLock(txn2.ID, "key1", 0); err == nil {
		t.Fatal("expected no-wait lock attempt to fail")
	}
	if waitingFor := txn2.GetWaitingFor(); waitingFor != 0 {
		t.Fatalf("failed no-wait lock attempt left stale waitingFor=%d", waitingFor)
	}

	err := mgr.AcquireLock(txn1.ID, "key2", 0)
	if err == nil {
		t.Fatal("expected txn1 no-wait lock attempt to fail")
	}
	if err == ErrDeadlockDetected {
		t.Fatal("stale waitingFor caused false deadlock detection")
	}

	if err := txn1.Rollback(); err != nil {
		t.Fatalf("txn1 rollback: %v", err)
	}
	if err := txn2.Rollback(); err != nil {
		t.Fatalf("txn2 rollback: %v", err)
	}
}

func TestAddLockHeldIdempotent(t *testing.T) {
	txn := &Transaction{locksHeld: make(map[string]bool)}
	txn.AddLockHeld("key1")
	txn.AddLockHeld("key1") // should not panic
	if len(txn.locksHeld) != 1 {
		t.Errorf("expected 1 lock, got %d", len(txn.locksHeld))
	}
}
