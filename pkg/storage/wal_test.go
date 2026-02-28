package storage

import (
	"path/filepath"
	"testing"
)

func TestOpenWAL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	if wal == nil {
		t.Fatal("WAL is nil")
	}
}

func TestWALAppend(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	record := &WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Offset: 0,
		Data:   []byte("test data"),
	}

	err = wal.Append(record)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
}

func TestWALMultipleAppends(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 10; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}

		err := wal.Append(record)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}
}

func TestWALCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()
	pool.SetWAL(wal)

	// Append some records
	for i := 0; i < 5; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}
		wal.Append(record)
	}

	// Create checkpoint
	err = wal.Checkpoint(pool)
	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}
}

func TestWALRecover(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	// Write some data
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	for i := 0; i < 5; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}
		wal.Append(record)
	}
	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
}

func TestWALLSN(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	lsn := wal.LSN()
	if lsn != 0 {
		t.Errorf("Expected initial LSN 0, got %d", lsn)
	}

	record := &WALRecord{
		Type: WALInsert,
		Data: []byte("data"),
	}
	wal.Append(record)

	lsn = wal.LSN()
	if lsn == 0 {
		t.Error("Expected non-zero LSN after append")
	}
}

func TestWALCheckpointLSN(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Before checkpoint
	cpLSN := wal.CheckpointLSN()
	if cpLSN != 0 {
		t.Errorf("Expected checkpoint LSN 0, got %d", cpLSN)
	}

	// Append
	record := &WALRecord{Type: WALInsert, Data: []byte("data")}
	wal.Append(record)
}

func TestWALRecordTypes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	types := []WALRecordType{WALInsert, WALUpdate, WALDelete, WALCommit, WALRollback, WALCheckpoint}

	for i, rt := range types {
		record := &WALRecord{
			TxnID: uint64(i),
			Type:  rt,
			Data:  []byte("data"),
		}
		err := wal.Append(record)
		if err != nil {
			t.Errorf("Failed to append record type %d: %v", rt, err)
		}
	}
}

func TestWALCloseTwice(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Close again should not error
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL twice: %v", err)
	}
}
