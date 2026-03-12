package txn

import (
	"sync"
	"testing"
)

// ---- pruneVersions tests ----

func TestPruneVersionsNoActive(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Commit some transactions to populate versions
	txn1 := mgr.Begin(nil)
	txn1.SetWrite("key1", []byte("v1"))
	txn1.Commit()

	txn2 := mgr.Begin(nil)
	txn2.SetWrite("key2", []byte("v2"))
	txn2.Commit()

	// Verify versions are set
	if mgr.GetCurrentVersion("key1") == 0 {
		t.Fatal("Expected version for key1")
	}
	if mgr.GetCurrentVersion("key2") == 0 {
		t.Fatal("Expected version for key2")
	}

	// No active transactions, pruneVersions should clear all versions
	mgr.pruneVersions()

	if mgr.GetCurrentVersion("key1") != 0 {
		t.Error("Expected key1 version to be cleared after prune")
	}
	if mgr.GetCurrentVersion("key2") != 0 {
		t.Error("Expected key2 version to be cleared after prune")
	}
}

func TestPruneVersionsWithActive(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Commit a transaction to populate versions
	txn1 := mgr.Begin(nil)
	txn1.SetWrite("key1", []byte("v1"))
	txn1.Commit()

	// Start a new active transaction
	txn2 := mgr.Begin(nil)
	_ = txn2 // keep active

	// Prune should NOT clear versions since there's an active txn
	mgr.pruneVersions()

	// The versions map should not have been reset because there is an active txn
	// (The pruneVersions function only clears if no active transactions)
	// After prune with active, key1 should still be there
	mgr.mu.RLock()
	_, exists := mgr.versions["key1"]
	mgr.mu.RUnlock()
	if !exists {
		t.Error("Expected key1 version to still exist with active transaction")
	}

	// Cleanup
	txn2.Rollback()
}

func TestPruneVersionsEmpty(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Prune on empty manager should not panic
	mgr.pruneVersions()

	// Versions should be empty
	mgr.mu.RLock()
	vLen := len(mgr.versions)
	mgr.mu.RUnlock()
	if vLen != 0 {
		t.Errorf("Expected 0 versions, got %d", vLen)
	}
}

// ---- GetTransaction tests ----

func TestGetTransactionExisting(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	id := txn.ID

	got := mgr.GetTransaction(id)
	if got == nil {
		t.Fatal("Expected to find transaction")
	}
	if got.ID != id {
		t.Errorf("Expected ID %d, got %d", id, got.ID)
	}
}

func TestGetTransactionNonExisting(t *testing.T) {
	mgr := NewManager(nil, nil)

	got := mgr.GetTransaction(99999)
	if got != nil {
		t.Error("Expected nil for non-existing transaction")
	}
}

func TestGetTransactionAfterCommit(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	id := txn.ID
	txn.Commit()

	// After commit, transaction is removed from active
	got := mgr.GetTransaction(id)
	if got != nil {
		t.Error("Expected nil after commit (removed from active)")
	}
}

func TestGetTransactionAfterRollback(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	id := txn.ID
	txn.Rollback()

	got := mgr.GetTransaction(id)
	if got != nil {
		t.Error("Expected nil after rollback (removed from active)")
	}
}

// ---- applyWrites tests ----

func TestApplyWritesSingleKey(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	txn.SetWrite("testkey", []byte("testvalue"))

	err := mgr.applyWrites(txn)
	if err != nil {
		t.Fatalf("applyWrites failed: %v", err)
	}

	v := mgr.GetCurrentVersion("testkey")
	if v != txn.ID {
		t.Errorf("Expected version %d, got %d", txn.ID, v)
	}
}

func TestApplyWritesMultipleKeys(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	txn.SetWrite("key1", []byte("val1"))
	txn.SetWrite("key2", []byte("val2"))
	txn.SetWrite("key3", []byte("val3"))

	err := mgr.applyWrites(txn)
	if err != nil {
		t.Fatalf("applyWrites failed: %v", err)
	}

	for _, key := range []string{"key1", "key2", "key3"} {
		v := mgr.GetCurrentVersion(key)
		if v != txn.ID {
			t.Errorf("Key %s: expected version %d, got %d", key, txn.ID, v)
		}
	}
}

func TestApplyWritesEmptyWriteSet(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	// No writes

	err := mgr.applyWrites(txn)
	if err != nil {
		t.Fatalf("applyWrites with empty write set should succeed: %v", err)
	}
}

func TestApplyWritesOverwritesVersion(t *testing.T) {
	mgr := NewManager(nil, nil)

	// First transaction writes key1
	txn1 := mgr.Begin(nil)
	txn1.SetWrite("key1", []byte("val1"))
	mgr.applyWrites(txn1)
	v1 := mgr.GetCurrentVersion("key1")

	// Second transaction overwrites key1
	txn2 := mgr.Begin(nil)
	txn2.SetWrite("key1", []byte("val2"))
	mgr.applyWrites(txn2)
	v2 := mgr.GetCurrentVersion("key1")

	if v2 <= v1 {
		t.Errorf("Expected version to increase: v1=%d, v2=%d", v1, v2)
	}
	if v2 != txn2.ID {
		t.Errorf("Expected version %d, got %d", txn2.ID, v2)
	}
}

// ---- Concurrent commit scenarios ----

func TestConcurrentCommits(t *testing.T) {
	mgr := NewManager(nil, nil)

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txn := mgr.Begin(nil)
			txn.SetWrite(string(rune('a'+i)), []byte("value"))
			if err := txn.Commit(); err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent commit error: %v", err)
	}
}

// ---- Conflict detection tests ----

func TestConflictDetectionWriteWrite(t *testing.T) {
	mgr := NewManager(nil, nil)

	// txn1 reads key1 at version 0
	txn1 := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn1.SetReadVersion("key1", 0)

	// txn2 writes key1 and commits, updating the version
	txn2 := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn2.SetWrite("key1", []byte("new_val"))
	err := txn2.Commit()
	if err != nil {
		t.Fatalf("txn2 commit failed: %v", err)
	}

	// txn1 should detect a conflict because key1 was modified
	txn1.SetWrite("key1", []byte("conflicting_val"))
	err = txn1.Commit()
	if err == nil {
		t.Error("Expected conflict error, got nil")
	}
	if err != nil && err != ErrConflict {
		t.Logf("Got error (expected ErrConflict): %v", err)
	}
}

func TestConflictDetectionNoConflictDifferentKeys(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn1 := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn1.SetReadVersion("key1", 0)

	txn2 := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn2.SetWrite("key2", []byte("val2"))
	txn2.Commit()

	// txn1 writes to a different key, no conflict
	txn1.SetWrite("key3", []byte("val3"))
	err := txn1.Commit()
	if err != nil {
		t.Errorf("Should not conflict on different keys: %v", err)
	}
}

func TestConflictDetectionReadCommittedSkipsCheck(t *testing.T) {
	mgr := NewManager(nil, nil)

	// First populate key1
	txn0 := mgr.Begin(nil)
	txn0.SetWrite("key1", []byte("initial"))
	txn0.Commit()

	// ReadCommitted txn reads key1
	txn1 := mgr.Begin(&Options{Isolation: ReadCommitted})
	txn1.SetReadVersion("key1", 0)

	// Another txn writes key1
	txn2 := mgr.Begin(nil)
	txn2.SetWrite("key1", []byte("updated"))
	txn2.Commit()

	// ReadCommitted should not detect conflicts
	txn1.SetWrite("key1", []byte("rc_write"))
	err := txn1.Commit()
	if err != nil {
		t.Errorf("ReadCommitted should not detect conflicts: %v", err)
	}
}

func TestSerializableConflictDetection(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn1 := mgr.Begin(&Options{Isolation: Serializable})
	txn1.SetReadVersion("key1", 0)

	txn2 := mgr.Begin(&Options{Isolation: Serializable})
	txn2.SetWrite("key1", []byte("val"))
	txn2.Commit()

	txn1.SetWrite("key1", []byte("conflict"))
	err := txn1.Commit()
	if err == nil {
		t.Error("Serializable should detect conflict")
	}
}

// ---- Double rollback ----

func TestDoubleRollback(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	err1 := txn.Rollback()
	if err1 != nil {
		t.Fatalf("First rollback failed: %v", err1)
	}

	err2 := txn.Rollback()
	if err2 != nil {
		t.Errorf("Second rollback should return nil (already aborted), got: %v", err2)
	}
}

// ---- Rollback after commit ----

func TestRollbackAfterCommit(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.Commit()

	err := txn.Rollback()
	if err != ErrTxnCommitted {
		t.Errorf("Expected ErrTxnCommitted, got %v", err)
	}
}

// ---- SetReadVersion on nil ReadSet ----

func TestSetReadVersionNilReadSet(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	// Force nil ReadSet
	txn.mu.Lock()
	txn.ReadSet = nil
	txn.mu.Unlock()

	// Should not panic
	txn.SetReadVersion("key1", 100)
	v, ok := txn.GetReadVersion("key1")
	if !ok || v != 100 {
		t.Errorf("Expected version 100, got %d, ok=%v", v, ok)
	}
}

// ---- SetWrite on nil WriteSet ----

func TestSetWriteNilWriteSet(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	// Force nil WriteSet
	txn.mu.Lock()
	txn.WriteSet = nil
	txn.mu.Unlock()

	// Should not panic
	txn.SetWrite("key1", []byte("val"))
	v, ok := txn.GetWrite("key1")
	if !ok || string(v) != "val" {
		t.Errorf("Expected val, got %s, ok=%v", v, ok)
	}
}

// ---- Manager Begin with all isolation levels ----

func TestBeginAllIsolationLevels(t *testing.T) {
	mgr := NewManager(nil, nil)

	levels := []IsolationLevel{ReadCommitted, SnapshotIsolation, Serializable}
	for _, lvl := range levels {
		txn := mgr.Begin(&Options{Isolation: lvl})
		if txn.Isolation != lvl {
			t.Errorf("Expected isolation %d, got %d", lvl, txn.Isolation)
		}
		txn.Rollback()
	}
}

// ---- removeActive is idempotent ----

func TestRemoveActiveIdempotent(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	id := txn.ID

	mgr.removeActive(id)
	// Second removal should not panic
	mgr.removeActive(id)
	// Removing non-existent should not panic
	mgr.removeActive(99999)
}

// ---- Concurrent begin and commit ----

func TestConcurrentBeginAndCommit(t *testing.T) {
	mgr := NewManager(nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txn := mgr.Begin(nil)
			txn.SetWrite("shared", []byte("value"))
			txn.Commit()
		}()
	}
	wg.Wait()
}

// ---- Verify StartTS equals ID ----

func TestStartTSEqualsID(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)
	if txn.StartTS != txn.ID {
		t.Errorf("Expected StartTS=%d to equal ID=%d", txn.StartTS, txn.ID)
	}
}

// ---- detectConflicts with non-existing key in versions ----

func TestDetectConflictsNonExistingKey(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn.SetReadVersion("nonexistent_key", 100)

	// No conflict because key was never committed
	err := mgr.detectConflicts(txn)
	if err != nil {
		t.Errorf("Should not conflict on non-existing version key: %v", err)
	}
}

// ---- detectConflicts with read version equal to current version ----

func TestDetectConflictsEqualVersion(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Set a version for key1
	txn1 := mgr.Begin(nil)
	txn1.SetWrite("key1", []byte("val"))
	txn1.Commit()

	// Read the same version
	txn2 := mgr.Begin(&Options{Isolation: SnapshotIsolation})
	txn2.SetReadVersion("key1", txn1.ID) // read at committed version

	err := mgr.detectConflicts(txn2)
	if err != nil {
		t.Errorf("Should not conflict when read version equals current: %v", err)
	}
}
