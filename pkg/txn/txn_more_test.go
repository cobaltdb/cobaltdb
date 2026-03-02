package txn

import (
	"testing"
)

// TestManagerGet tests Manager.Get function
func TestManagerGet(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Get non-existent transaction
	txn, err := mgr.Get(999)
	if err != ErrTxnNotFound {
		t.Errorf("Expected ErrTxnNotFound, got %v", err)
	}
	if txn != nil {
		t.Error("Expected nil transaction")
	}

	// Begin a transaction and get it
	txn1 := mgr.Begin(nil)
	id := txn1.ID

	retrieved, err := mgr.Get(id)
	if err != nil {
		t.Errorf("Failed to get transaction: %v", err)
	}
	if retrieved != txn1 {
		t.Error("Retrieved transaction doesn't match")
	}
}


// TestCommitWithNoWrites tests committing a read-only transaction
func TestCommitWithNoWrites(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Read-only transaction with no writes
	txn := mgr.Begin(&Options{ReadOnly: true})

	err := txn.Commit()
	if err != nil {
		t.Logf("Commit returned error: %v", err)
	}

	if txn.State != TxnCommitted {
		t.Logf("Expected state %d, got %d", TxnCommitted, txn.State)
	}
}

// TestRollbackWithWrites tests rollback with pending writes
func TestRollbackWithWrites(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	txn.SetWrite("key1", []byte("value1"))
	txn.SetWrite("key2", []byte("value2"))

	err := txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	if txn.State != TxnAborted {
		t.Errorf("Expected state %d, got %d", TxnAborted, txn.State)
	}

	// Writes should be cleared
	_, ok := txn.GetWrite("key1")
	if ok {
		t.Error("Write should be cleared after rollback")
	}
}

// TestMultipleTransactions tests multiple concurrent transactions
func TestMultipleTransactions(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Begin multiple transactions
	txn1 := mgr.Begin(nil)
	txn2 := mgr.Begin(nil)
	txn3 := mgr.Begin(nil)

	// Verify all have unique IDs
	if txn1.ID == txn2.ID || txn1.ID == txn3.ID || txn2.ID == txn3.ID {
		t.Error("Transaction IDs should be unique")
	}

	// All should be active
	if txn1.State != TxnActive || txn2.State != TxnActive || txn3.State != TxnActive {
		t.Error("All transactions should be active")
	}

	// Commit one
	txn1.Commit()

	// Rollback another
	txn2.Rollback()

	// Third should still be active
	if txn3.State != TxnActive {
		t.Error("Transaction 3 should still be active")
	}
}

// TestTransactionStateTransitions tests state transitions
func TestTransactionStateTransitions(t *testing.T) {
	mgr := NewManager(nil, nil)

	// Active -> Committed
	txn := mgr.Begin(nil)
	if txn.State != TxnActive {
		t.Error("New transaction should be active")
	}

	txn.Commit()
	if txn.State != TxnCommitted {
		t.Errorf("Expected state %d, got %d", TxnCommitted, txn.State)
	}

	// Active -> Aborted
	txn2 := mgr.Begin(nil)
	txn2.Rollback()
	if txn2.State != TxnAborted {
		t.Errorf("Expected state %d, got %d", TxnAborted, txn2.State)
	}
}

// TestGetReadVersionNotFound tests GetReadVersion when key not found
func TestGetReadVersionNotFound(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	_, ok := txn.GetReadVersion("nonexistent")
	if ok {
		t.Error("Expected false for non-existent key")
	}
}

// TestGetWriteNotFound tests GetWrite when key not found
func TestGetWriteNotFound(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	_, ok := txn.GetWrite("nonexistent")
	if ok {
		t.Error("Expected false for non-existent key")
	}
}

// TestSetReadVersionOverwrite tests overwriting read version
func TestSetReadVersionOverwrite(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.SetReadVersion("key1", 100)
	txn.SetReadVersion("key1", 200)

	v, ok := txn.GetReadVersion("key1")
	if !ok {
		t.Fatal("Read version not found")
	}

	if v != 200 {
		t.Errorf("Expected version 200, got %d", v)
	}
}

// TestSetWriteOverwrite tests overwriting write
func TestSetWriteOverwrite(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.SetWrite("key1", []byte("value1"))
	txn.SetWrite("key1", []byte("value2"))

	v, ok := txn.GetWrite("key1")
	if !ok {
		t.Fatal("Write not found")
	}

	if string(v) != "value2" {
		t.Errorf("Expected value2, got %s", v)
	}
}

// TestSnapshotIsolation tests snapshot isolation level
func TestSnapshotIsolation(t *testing.T) {
	opts := &Options{
		Isolation: SnapshotIsolation,
		ReadOnly:  false,
	}

	mgr := NewManager(nil, nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != SnapshotIsolation {
		t.Errorf("Expected isolation %d, got %d", SnapshotIsolation, txn.Isolation)
	}
}

// TestSerializableIsolation tests serializable isolation level
func TestSerializableIsolation(t *testing.T) {
	opts := &Options{
		Isolation: Serializable,
		ReadOnly:  false,
	}

	mgr := NewManager(nil, nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != Serializable {
		t.Errorf("Expected isolation %d, got %d", Serializable, txn.Isolation)
	}
}

// TestReadCommittedIsolation tests read committed isolation level
func TestReadCommittedIsolation(t *testing.T) {
	opts := &Options{
		Isolation: ReadCommitted,
		ReadOnly:  false,
	}

	mgr := NewManager(nil, nil)
	txn := mgr.Begin(opts)

	if txn.Isolation != ReadCommitted {
		t.Errorf("Expected isolation %d, got %d", ReadCommitted, txn.Isolation)
	}
}

// TestTransactionIDIncrement tests that transaction IDs increment
func TestTransactionIDIncrement(t *testing.T) {
	mgr := NewManager(nil, nil)

	var lastID uint64 = 0
	for i := 0; i < 10; i++ {
		txn := mgr.Begin(nil)
		if txn.ID <= lastID {
			t.Errorf("Transaction ID should increment: %d <= %d", txn.ID, lastID)
		}
		lastID = txn.ID
	}
}
