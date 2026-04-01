package txn

import (
	"testing"
	"time"
)

// TestDeadlockDetectionTwoTransactions tests basic deadlock between two transactions
func TestDeadlockDetectionTwoTransactions(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	// Create two transactions
	txn1 := m.Begin(nil)
	txn2 := m.Begin(nil)

	// txn1 acquires lock on key1
	err := m.AcquireLock(txn1.ID, "key1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("txn1 failed to acquire lock on key1: %v", err)
	}

	// txn2 acquires lock on key2
	err = m.AcquireLock(txn2.ID, "key2", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("txn2 failed to acquire lock on key2: %v", err)
	}

	// txn1 tries to acquire lock on key2 (held by txn2)
	// This should set up waiting relationship
	txn1.SetWaitingFor(txn2.ID)

	// txn2 tries to acquire lock on key1 (held by txn1)
	// This should create a deadlock
	txn2.SetWaitingFor(txn1.ID)

	// Check if deadlock would be detected
	if m.wouldCauseDeadlock(txn2.ID, txn1.ID) {
		t.Log("Deadlock correctly detected")
	} else {
		t.Error("Deadlock should have been detected")
	}

	// Clean up
	txn1.Rollback()
	txn2.Rollback()
}

// TestDeadlockDetectionThreeTransactions tests deadlock cycle with three transactions
func TestDeadlockDetectionThreeTransactions(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	txn1 := m.Begin(nil)
	txn2 := m.Begin(nil)
	txn3 := m.Begin(nil)

	// Create cycle: txn1 -> txn2 -> txn3 -> txn1
	txn1.SetWaitingFor(txn2.ID)
	txn2.SetWaitingFor(txn3.ID)
	txn3.SetWaitingFor(txn1.ID)

	// Check for deadlock
	if m.wouldCauseDeadlock(txn3.ID, txn1.ID) {
		t.Log("Three-way deadlock correctly detected")
	} else {
		t.Error("Three-way deadlock should have been detected")
	}

	txn1.Rollback()
	txn2.Rollback()
	txn3.Rollback()
}

// TestDeadlockResolution tests that deadlocks are automatically resolved
func TestDeadlockResolution(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	txn1 := m.Begin(nil)
	txn2 := m.Begin(nil)

	// Acquire locks
	m.AcquireLock(txn1.ID, "key1", 100*time.Millisecond)
	m.AcquireLock(txn2.ID, "key2", 100*time.Millisecond)

	// Create waiting relationship
	txn1.SetWaitingFor(txn2.ID)
	txn2.SetWaitingFor(txn1.ID)

	// Trigger deadlock check
	m.checkForDeadlocks()

	// Give some time for resolution
	time.Sleep(50 * time.Millisecond)

	// At least one transaction should be aborted
	t1Active := false
	t2Active := false

	m.mu.RLock()
	if _, ok := m.active[txn1.ID]; ok {
		t1Active = true
	}
	if _, ok := m.active[txn2.ID]; ok {
		t2Active = true
	}
	m.mu.RUnlock()

	if t1Active && t2Active {
		t.Error("At least one transaction should have been aborted to resolve deadlock")
	}

	if !t1Active {
		t.Log("Transaction 1 was aborted to resolve deadlock")
	}
	if !t2Active {
		t.Log("Transaction 2 was aborted to resolve deadlock")
	}
}

// TestLockTimeout tests lock acquisition timeout
func TestLockTimeout(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	txn1 := m.Begin(nil)
	txn2 := m.Begin(nil)

	// txn1 acquires lock
	err := m.AcquireLock(txn1.ID, "key1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// txn2 tries to acquire same lock with short timeout - should timeout
	err = m.AcquireLock(txn2.ID, "key1", 50*time.Millisecond)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	txn1.Rollback()
	txn2.Rollback()
}

// TestNoDeadlockFalsePositive ensures no false positives on non-deadlock scenarios
func TestNoDeadlockFalsePositive(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	txn1 := m.Begin(nil)
	txn2 := m.Begin(nil)
	txn3 := m.Begin(nil)

	// Linear chain: txn1 -> txn2, txn2 -> txn3 (no cycle)
	txn1.SetWaitingFor(txn2.ID)
	txn2.SetWaitingFor(txn3.ID)

	// txn3 waiting for nothing
	txn3.SetWaitingFor(0)

	// Should not detect deadlock
	if m.wouldCauseDeadlock(txn3.ID, 0) {
		t.Error("False positive: no cycle exists")
	}

	txn1.Rollback()
	txn2.Rollback()
	txn3.Rollback()
}

// TestTransactionTimeout tests transaction-level timeout
func TestTransactionTimeout(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	opts := &Options{
		Isolation: SnapshotIsolation,
		Timeout:   100 * time.Millisecond,
	}

	txn := m.Begin(opts)

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Try to commit - should fail with timeout
	err := txn.Commit()
	if err != ErrTxnTimeout {
		t.Errorf("Expected ErrTxnTimeout, got: %v", err)
	}
}

// TestLockReleaseOnRollback tests that locks are released when transaction rolls back
func TestLockReleaseOnRollback(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	txn1 := m.Begin(nil)

	// Acquire lock
	err := m.AcquireLock(txn1.ID, "key1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Rollback
	txn1.Rollback()

	// Lock should be released - txn2 should be able to acquire it
	txn2 := m.Begin(nil)
	err = m.AcquireLock(txn2.ID, "key1", 100*time.Millisecond)
	if err != nil {
		t.Errorf("Lock was not released on rollback: %v", err)
	}

	txn2.Rollback()
}

// TestDeadlockDetectorPerformance tests that deadlock detection doesn't cause performance issues
func TestDeadlockDetectorPerformance(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	// Create many transactions
	const numTxns = 100
	txns := make([]*Transaction, numTxns)
	for i := 0; i < numTxns; i++ {
		txns[i] = m.Begin(nil)
	}

	// Each transaction waits for the next one (no cycle)
	for i := 0; i < numTxns-1; i++ {
		txns[i].SetWaitingFor(txns[i+1].ID)
	}

	// Measure time for deadlock check
	start := time.Now()
	m.checkForDeadlocks()
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("Deadlock check took too long: %v", elapsed)
	}

	// Clean up
	for _, txn := range txns {
		txn.Rollback()
	}
}
