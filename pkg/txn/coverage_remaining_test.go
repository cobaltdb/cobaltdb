package txn

import (
	"testing"
)

func TestAddLockHeldIfActiveActive(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	err := txn.AddLockHeldIfActive("lock_key")
	if err != nil {
		t.Fatalf("AddLockHeldIfActive failed: %v", err)
	}
}

func TestAddLockHeldIfActiveCommitted(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	txn.Commit()

	err := txn.AddLockHeldIfActive("lock_key")
	if err == nil {
		t.Fatal("expected error for committed txn")
	}
}

func TestAddLockHeldIfActiveAborted(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	txn.Rollback()

	err := txn.AddLockHeldIfActive("lock_key")
	if err == nil {
		t.Fatal("expected error for aborted txn")
	}
}

func TestRemoveLockHeld(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	// Add a lock then remove it
	txn.AddLockHeldIfActive("lock_key")
	txn.RemoveLockHeld("lock_key")

	// Removing again should not panic
	txn.RemoveLockHeld("nonexistent")
}

func TestRemoveLockHeldNilMap(t *testing.T) {
	// Removing from a transaction with nil locksHeld should not panic
	txn := &Transaction{}
	txn.RemoveLockHeld("key")
}

func TestRecycle(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	// Recycle the transaction
	txn.Recycle()

	// The transaction should have manager = nil after recycle
	if txn.manager != nil {
		t.Error("expected manager to be nil after recycle")
	}
}

func TestRecycleNoManager(t *testing.T) {
	// Transaction without a manager — Recycle should be a no-op
	txn := &Transaction{}
	txn.Recycle() // should not panic
}

func TestRecycleNilTxn(t *testing.T) {
	mgr := NewManager(nil)
	// RecycleTxn with nil should not panic
	mgr.RecycleTxn(nil)
}

func TestRecycleTxnAlreadyRecycled(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	txn.Recycle()

	// Recycling again should be a no-op
	mgr.RecycleTxn(txn)
}

func TestRemoveWrite(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	txn.SetWrite("tree1", "key1", []byte("value1"))

	txn.RemoveWrite("tree1", "key1")

	_, exists := txn.GetWrite("tree1", "key1")
	if exists {
		t.Error("expected key to be removed from WriteSet")
	}
}

func TestRemoveWriteNonExistent(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)

	// Removing a non-existent key should not panic
	txn.RemoveWrite("tree1", "nonexistent")
}

func TestRemoveWriteNilSet(t *testing.T) {
	// Removing from a nil WriteSet should not panic
	txn := &Transaction{}
	txn.RemoveWrite("tree1", "key1")
}

func TestVersionStoreClear(t *testing.T) {
	vs := NewVersionStore()

	vs.Commit(WriteKey{Key: "key1"}, []byte("v1"), 1)
	vs.Commit(WriteKey{Key: "key2"}, []byte("v2"), 2)

	// Verify data exists
	_, err := vs.GetAtSnapshot(WriteKey{Key: "key1"}, 1)
	if err != nil {
		t.Fatalf("expected key1 to exist before clear: %v", err)
	}

	vs.Clear()

	// After clear, keys should not be found
	_, err = vs.GetAtSnapshot(WriteKey{Key: "key1"}, 1)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after clear, got %v", err)
	}

	_, err = vs.GetAtSnapshot(WriteKey{Key: "key2"}, 2)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after clear, got %v", err)
	}

	if vs.Len() != 0 {
		t.Errorf("expected Len()=0 after clear, got %d", vs.Len())
	}
}

func TestVersionStoreClearEmpty(t *testing.T) {
	vs := NewVersionStore()
	// Clearing an empty store should not panic
	vs.Clear()

	if vs.Len() != 0 {
		t.Errorf("expected Len()=0, got %d", vs.Len())
	}
}
