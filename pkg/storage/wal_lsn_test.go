package storage

import (
	"path/filepath"
	"testing"
)

// TestWALLSNPersistence verifies that LSN is persisted to disk and
// correctly recovered after close/reopen (CRITICAL fix: LSN was not
// written into the WAL record format, causing LSN to reset to 0 on restart)
func TestWALLSNPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	// Open WAL and append 5 records
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		record := &WALRecord{
			TxnID: uint64(i + 1),
			Type:  WALInsert,
			Data:  []byte("data"),
		}
		if err := wal.Append(record); err != nil {
			t.Fatal(err)
		}
	}
	lsnBefore := wal.LSN()
	if lsnBefore != 5 {
		t.Fatalf("LSN before close = %d, want 5", lsnBefore)
	}
	wal.Close()

	// Reopen WAL — LSN should be recovered from disk
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	lsnAfter := wal2.LSN()
	if lsnAfter != lsnBefore {
		t.Fatalf("LSN after reopen = %d, want %d (LSN was not persisted to disk)", lsnAfter, lsnBefore)
	}

	// New records should continue from the recovered LSN
	record := &WALRecord{
		TxnID: 6,
		Type:  WALInsert,
		Data:  []byte("more data"),
	}
	if err := wal2.Append(record); err != nil {
		t.Fatal(err)
	}
	if wal2.LSN() != 6 {
		t.Fatalf("LSN after new append = %d, want 6", wal2.LSN())
	}
}

// TestWALCommitRecordRecovery verifies that committed transactions
// are properly recovered after crash (CRITICAL fix: WAL commit records
// were not being written by the transaction manager)
func TestWALCommitRecordRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	// Create a page for writing
	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	pageID := page.ID()
	pool.Unpin(page)

	// Write WAL records: txn 1 committed, txn 2 not committed
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	// Txn 1: write + commit
	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: pageID, Offset: 100, Data: []byte("committed")})
	wal.Append(&WALRecord{TxnID: 1, Type: WALCommit})

	// Txn 2: write only (no commit — simulates crash before commit)
	wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, PageID: pageID, Offset: 200, Data: []byte("uncommitted")})

	wal.Close()

	// Recover — only txn 1 should be applied
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatal(err)
	}

	p, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Unpin(p)

	// Committed data should be there
	if string(p.Data()[100:109]) != "committed" {
		t.Errorf("committed data not recovered: got %q", string(p.Data()[100:109]))
	}
	// Uncommitted data should NOT be there
	if string(p.Data()[200:211]) == "uncommitted" {
		t.Error("uncommitted data should not be recovered")
	}
}

// TestWALAppendWithoutSyncValidation verifies AppendWithoutSync rejects oversized data
func TestWALAppendWithoutSyncValidation(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Data exceeding 65535 bytes should be rejected
	record := &WALRecord{
		TxnID: 1,
		Type:  WALInsert,
		Data:  make([]byte, 70000),
	}
	err = wal.AppendWithoutSync(record)
	if err == nil {
		t.Error("AppendWithoutSync should reject oversized data")
	}
}
