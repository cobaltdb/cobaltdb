package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestWALAppendBatchDurableBeforeClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	records := []*WALRecord{
		{TxnID: 1, Type: WALInsert, PageID: 1, Offset: 0, Data: []byte("small")},
		{TxnID: 1, Type: WALCommit},
	}
	if err := wal.AppendBatch(records); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Reopen after AppendBatch failed: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LSN(); got != 2 {
		t.Fatalf("reopened LSN = %d, want 2", got)
	}
}

func TestWALAppendBatchLargeRecordCRC(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large-batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	largePayload := make([]byte, walBatchBufferSize+512)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	if err := wal.AppendBatch([]*WALRecord{
		{TxnID: 7, Type: WALInsert, PageID: 1, Offset: 0, Data: largePayload},
	}); err != nil {
		t.Fatalf("AppendBatch large record failed: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Reopen after large AppendBatch failed: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LSN(); got != 1 {
		t.Fatalf("reopened LSN = %d, want 1", got)
	}
}

func TestOpenWALTruncatesPartialTrailingRecord(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "partial-tail.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("valid")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	validSize := info.Size()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.Write([]byte{0xde, 0xad, 0xbe, 0xef}); err != nil {
		_ = file.Close()
		t.Fatalf("Write partial tail: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close partial tail: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL with partial tail: %v", err)
	}
	if got := reopened.LSN(); got != 1 {
		t.Fatalf("LSN after truncating partial tail = %d, want 1", got)
	}
	if err := reopened.Append(&WALRecord{TxnID: 2, Type: WALCommit}); err != nil {
		t.Fatalf("Append after tail truncation: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened WAL: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat after reopen: %v", err)
	}
	if info.Size() <= validSize {
		t.Fatalf("expected appended WAL size > %d, got %d", validSize, info.Size())
	}

	reopenedAgain, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL after append: %v", err)
	}
	defer reopenedAgain.Close()
	if got := reopenedAgain.LSN(); got != 2 {
		t.Fatalf("LSN after append = %d, want 2", got)
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
		if err := wal.Append(record); err != nil {
			t.Fatalf("Failed to append WAL record: %v", err)
		}
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
		if err := wal.Append(record); err != nil {
			t.Fatalf("Failed to append WAL record: %v", err)
		}
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
	if err := wal.Append(record); err != nil {
		t.Fatalf("Failed to append WAL record: %v", err)
	}

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
	if err := wal.Append(record); err != nil {
		t.Fatalf("Failed to append WAL record: %v", err)
	}
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

func TestWALApplyRecordBoundsCheck(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()
	if _, err := backend.WriteAt(make([]byte, PageSize), int64(PageSize)); err != nil {
		t.Fatalf("failed to prime test page: %v", err)
	}

	wal := &WAL{}
	record := &WALRecord{
		Type:   WALUpdate,
		PageID: 1,
		Offset: uint16(PageSize - 2),
		Data:   []byte("toolong"),
	}

	err := wal.applyRecord(pool, record)
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("expected ErrWALCorrupted, got %v", err)
	}
}

func TestWALGroupCommit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Enable group commit with a small interval so the test doesn't hang
	wal.EnableGroupCommit(0, 5*time.Millisecond)

	done := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(idx int) {
			rec := &WALRecord{
				TxnID: uint64(idx),
				Type:  WALInsert,
				Data:  []byte(fmt.Sprintf("row%d", idx)),
			}
			done <- wal.Append(rec)
		}(i)
	}

	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// All three appends should have completed after the next ticker sync
	wal.DisableGroupCommit()

	if wal.LSN() != 3 {
		t.Fatalf("expected LSN=3, got %d", wal.LSN())
	}
}

func TestWALGroupCommitBatchSize(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Enable group commit with batch size of 2 (no ticker)
	wal.EnableGroupCommit(2, 0)

	// First append should block until second append triggers the batch
	done1 := make(chan error, 1)
	go func() {
		done1 <- wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("a")})
	}()

	// Give goroutine time to start and block
	time.Sleep(20 * time.Millisecond)

	// Second append should trigger immediate sync and unblock both
	if err := wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, Data: []byte("b")}); err != nil {
		t.Fatalf("Second append failed: %v", err)
	}

	if err := <-done1; err != nil {
		t.Fatalf("First append failed: %v", err)
	}

	wal.DisableGroupCommit()

	if wal.LSN() != 2 {
		t.Fatalf("expected LSN=2, got %d", wal.LSN())
	}
}

func TestWALGroupCommitSyncOff(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_off.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// SyncOff: no background sync, no batch size
	wal.EnableGroupCommit(0, 0)

	// Append should return immediately without blocking
	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("x")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Data is in buffer but not synced
	wal.DisableGroupCommit() // this flushes pending

	if wal.LSN() != 1 {
		t.Fatalf("expected LSN=1, got %d", wal.LSN())
	}
}

// TestWALRecoverLogicalRecords verifies that logical WAL records (PageID==0,
// Offset==0) are buffered in replayOps rather than corrupting page 0.
func TestWALRecoverLogicalRecords(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "logical.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write a committed logical transaction.
	txnID := uint64(42)
	key := []byte("users:row1")
	val := []byte("value1")
	data := make([]byte, 4+len(key)+len(val))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	copy(data[4:4+len(key)], key)
	copy(data[4+len(key):], val)

	if err := wal.Append(&WALRecord{TxnID: txnID, Type: WALUpdate, Data: data}); err != nil {
		t.Fatalf("append data: %v", err)
	}
	if err := wal.Append(&WALRecord{TxnID: txnID, Type: WALCommit}); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	ops := wal2.GetReplayOps()
	if len(ops) != 1 {
		t.Fatalf("expected 1 replay op, got %d", len(ops))
	}
	if ops[0].Type != WALUpdate {
		t.Errorf("expected WALUpdate, got %d", ops[0].Type)
	}
	if string(ops[0].Data) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", ops[0].Data, data)
	}
}

func TestWALAppendBatchWithoutSync(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	records := []*WALRecord{
		{TxnID: 1, Type: WALInsert, PageID: 1, Data: []byte("row1")},
		{TxnID: 1, Type: WALInsert, PageID: 2, Data: []byte("row2")},
		{TxnID: 1, Type: WALUpdate, PageID: 1, Data: []byte("row1_updated")},
	}

	if err := wal.AppendBatchWithoutSync(records); err != nil {
		t.Fatalf("AppendBatchWithoutSync failed: %v", err)
	}

	if wal.LSN() != 3 {
		t.Fatalf("expected LSN=3, got %d", wal.LSN())
	}

	// Sync so records are durable for reopen
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if wal2.LSN() != 3 {
		t.Errorf("expected LSN=3 after recover, got %d", wal2.LSN())
	}
}
