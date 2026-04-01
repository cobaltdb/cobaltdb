package txn

import (
	"context"
	"testing"
	"time"
)

// TestGetLocksHeld tests the GetLocksHeld method
func TestGetLocksHeld(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	// Initially no locks held
	locks := txn.GetLocksHeld()
	if len(locks) != 0 {
		t.Errorf("Expected 0 locks initially, got %d", len(locks))
	}

	// Add some locks
	txn.AddLockHeld("key1")
	txn.AddLockHeld("key2")
	txn.AddLockHeld("key3")

	// Get locks held
	locks = txn.GetLocksHeld()
	if len(locks) != 3 {
		t.Errorf("Expected 3 locks, got %d", len(locks))
	}

	// Verify all locks are present
	lockSet := make(map[string]bool)
	for _, lock := range locks {
		lockSet[lock] = true
	}
	if !lockSet["key1"] || !lockSet["key2"] || !lockSet["key3"] {
		t.Error("Not all expected locks found")
	}

	// Remove a lock and verify
	txn.RemoveLockHeld("key1")
	locks = txn.GetLocksHeld()
	if len(locks) != 2 {
		t.Errorf("Expected 2 locks after removal, got %d", len(locks))
	}
}

// TestTransactionContext tests the Context method
func TestTransactionContext(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	// Get context
	txnCtx := txn.Context()
	if txnCtx == nil {
		t.Error("Expected non-nil context")
	}
}

// TestBeginWithContext tests the BeginWithContext method
func TestBeginWithContext(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	opts := &Options{
		Timeout: 100 * time.Millisecond,
	}

	txn := mgr.BeginWithContext(ctx, opts)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	// Verify the transaction has a non-nil context (it may be wrapped)
	if txn.Context() == nil {
		t.Error("Transaction context should not be nil")
	}
}

// TestIsTimedOut tests the IsTimedOut method
func TestIsTimedOut(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Test with regular Begin (no timeout)
	txn := mgr.Begin(nil)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	// Should not be timed out immediately
	if txn.IsTimedOut() {
		t.Error("Transaction should not be timed out immediately")
	}

	// Test with timed context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	opts := &Options{
		Timeout: 1 * time.Millisecond,
	}

	txn2 := mgr.BeginWithContext(ctx, opts)
	if txn2 == nil {
		t.Fatal("Transaction is nil")
	}

	// Wait for timeout
	time.Sleep(10 * time.Millisecond)

	// Should be timed out now
	if !txn2.IsTimedOut() {
		t.Error("Transaction should be timed out after delay")
	}
}

// TestReleaseAllLocks tests the ReleaseAllLocks method
func TestReleaseAllLocks(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	// Acquire some locks
	txn.AddLockHeld("lock1")
	txn.AddLockHeld("lock2")
	txn.AddLockHeld("lock3")

	// Verify locks are held
	if len(txn.GetLocksHeld()) != 3 {
		t.Fatalf("Expected 3 locks before release")
	}

	// Release all locks via manager
	mgr.ReleaseAllLocks(txn.ID)
}
