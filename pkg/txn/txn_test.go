package txn

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager(nil, nil)
	if mgr == nil {
		t.Fatal("Manager is nil")
	}
}

func TestBegin(t *testing.T) {
	mgr := NewManager(nil, nil)

	txn := mgr.Begin(nil)
	if txn == nil {
		t.Fatal("Transaction is nil")
	}

	if txn.State != TxnActive {
		t.Errorf("Expected state %d, got %d", TxnActive, txn.State)
	}
}

func TestBeginWithOptions(t *testing.T) {
	mgr := NewManager(nil, nil)

	opts := &Options{
		Isolation: SnapshotIsolation,
		ReadOnly:  true,
	}

	txn := mgr.Begin(opts)
	if txn.Isolation != SnapshotIsolation {
		t.Errorf("Expected isolation %d, got %d", SnapshotIsolation, txn.Isolation)
	}

	if !txn.ReadOnly {
		t.Error("Expected read-only transaction")
	}
}

func TestCommit(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	err := txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if txn.State != TxnCommitted {
		t.Errorf("Expected state %d, got %d", TxnCommitted, txn.State)
	}
}

func TestRollback(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	err := txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	if txn.State != TxnAborted {
		t.Errorf("Expected state %d, got %d", TxnAborted, txn.State)
	}
}

func TestDoubleCommit(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.Commit()

	err := txn.Commit()
	if err == nil {
		t.Error("Expected error when committing twice")
	}
}

func TestCommitAfterRollback(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.Rollback()

	err := txn.Commit()
	if err == nil {
		t.Error("Expected error when committing after rollback")
	}
}

func TestReadVersion(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.SetReadVersion("key1", 100)

	v, ok := txn.GetReadVersion("key1")
	if !ok {
		t.Fatal("Read version not found")
	}

	if v != 100 {
		t.Errorf("Expected version 100, got %d", v)
	}
}

func TestWrite(t *testing.T) {
	mgr := NewManager(nil, nil)
	txn := mgr.Begin(nil)

	txn.SetWrite("key1", []byte("value1"))

	v, ok := txn.GetWrite("key1")
	if !ok {
		t.Fatal("Write not found")
	}

	if string(v) != "value1" {
		t.Errorf("Expected value1, got %s", v)
	}
}

func TestGetCurrentVersion(t *testing.T) {
	mgr := NewManager(nil, nil)

	v := mgr.GetCurrentVersion("nonexistent")
	if v != 0 {
		t.Errorf("Expected version 0, got %d", v)
	}
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.Isolation != SnapshotIsolation {
		t.Errorf("Expected isolation %d, got %d", SnapshotIsolation, opts.Isolation)
	}

	if opts.ReadOnly {
		t.Error("Expected read-write by default")
	}
}
