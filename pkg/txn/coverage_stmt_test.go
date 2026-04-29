package txn

import (
	"context"
	"testing"
	"time"
)

func TestGetVersionStore(t *testing.T) {
	mgr := NewManager(nil, nil)
	vs := mgr.GetVersionStore()
	if vs == nil {
		t.Fatal("expected non-nil version store")
	}

	// Verify it's usable
	vs.Commit("key1", []byte("val1"), 1)
	val, err := vs.GetCurrent("key1")
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("expected val1, got %s", val)
	}
}

func TestAcquireLockModeShared(t *testing.T) {
	mgr := NewManager(nil, nil)
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
	mgr := NewManager(nil, nil)
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

func TestAcquireLockModeLockUpgrade(t *testing.T) {
	mgr := NewManager(nil, nil)
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
	mgr := NewManager(nil, nil)
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
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	// Acquire some locks
	mgr.AcquireLock(txn.ID, "key1", time.Second)
	mgr.AcquireLock(txn.ID, "key2", time.Second)

	// Manually remove from active so ReleaseAllLocks takes the inactive path
	mgr.mu.Lock()
	delete(mgr.active, txn.ID)
	mgr.mu.Unlock()

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
	mgr := NewManager(nil, nil)
	opts := &Options{Timeout: 10 * time.Second}
	txn := mgr.Begin(opts)

	// Add a write
	txn.WriteSet["key1"] = []byte("val1")

	err := txn.Commit()
	if err != nil {
		t.Fatalf("commit with timeout: %v", err)
	}
	if txn.State != TxnCommitted {
		t.Errorf("expected committed, got %d", txn.State)
	}
}

func TestAcquireLockModeTxnNotFound(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn1 := mgr.Begin(nil)

	// txn1 holds exclusive lock
	mgr.AcquireLockMode(txn1.ID, "key1", LockExclusive, time.Second)

	// Non-existent txn tries to acquire — will enter blocking path, then fail to find itself in active
	err := mgr.AcquireLockMode(99999, "key1", LockExclusive, 50*time.Millisecond)
	if err == nil {
		t.Error("expected error for non-existent txn")
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
