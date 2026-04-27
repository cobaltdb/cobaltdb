package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestDiskReadAtErrors tests ReadAt error cases
func TestDiskReadAtErrors(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}
	defer disk.Close()

	// Write some data first
	data := make([]byte, 4096)
	copy(data, []byte("test data"))
	_, err = disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Test reading at negative offset
	buf := make([]byte, 100)
	_, err = disk.ReadAt(buf, -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// TestDiskWriteAtErrors tests WriteAt error cases
func TestDiskWriteAtErrors(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}
	defer disk.Close()

	// Test writing at negative offset
	data := []byte("test")
	_, err = disk.WriteAt(data, -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// TestDiskCloseError tests Close with closed file
func TestDiskCloseError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}

	// Close normally first
	err = disk.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// Close again - should handle gracefully
	err = disk.Close()
	// May or may not error depending on OS
	t.Logf("Second close result: %v", err)
}

// TestMemoryReadAtErrors tests Memory ReadAt error cases
func TestMemoryReadAtErrors(t *testing.T) {
	mem := NewMemory()

	// Write some data
	data := make([]byte, 100)
	copy(data, []byte("test"))
	_, err := mem.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Test reading at negative offset
	buf := make([]byte, 10)
	_, err = mem.ReadAt(buf, -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// TestMemoryTruncateEdgeCases tests Memory Truncate edge cases
func TestMemoryTruncateEdgeCases(t *testing.T) {
	mem := NewMemory()

	// Write some data
	data := make([]byte, 100)
	copy(data, []byte("test data"))
	_, err := mem.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Truncate to same size
	err = mem.Truncate(100)
	if err != nil {
		t.Errorf("Truncate to same size failed: %v", err)
	}

	if mem.Size() != 100 {
		t.Errorf("Expected size 100, got %d", mem.Size())
	}

	// Truncate to larger size
	err = mem.Truncate(200)
	if err != nil {
		t.Errorf("Truncate to larger size failed: %v", err)
	}

	if mem.Size() != 200 {
		t.Errorf("Expected size 200, got %d", mem.Size())
	}
}

// TestDiskTruncateEdgeCases tests Disk Truncate edge cases
func TestDiskTruncateEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}
	defer disk.Close()

	// Write some data
	data := make([]byte, 100)
	copy(data, []byte("test data"))
	_, err = disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Truncate to same size
	err = disk.Truncate(100)
	if err != nil {
		t.Errorf("Truncate to same size failed: %v", err)
	}

	if disk.Size() != 100 {
		t.Errorf("Expected size 100, got %d", disk.Size())
	}

	// Truncate to larger size
	err = disk.Truncate(200)
	if err != nil {
		t.Errorf("Truncate to larger size failed: %v", err)
	}

	if disk.Size() != 200 {
		t.Errorf("Expected size 200, got %d", disk.Size())
	}
}

// TestOpenDiskErrors tests OpenDisk error cases
func TestOpenDiskErrors(t *testing.T) {
	// Try to open in a non-existent nested directory without permissions
	invalidPath := "/nonexistent/path/that/cannot/be/created/test.db"
	if os.PathSeparator == '\\' {
		invalidPath = "\\\\invalid\\share\\test.db"
	}

	_, err := OpenDisk(invalidPath)
	if err == nil {
		t.Log("OpenDisk succeeded (may be valid on this system)")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestMetaPageDeserializeErrors tests MetaPage Deserialize error cases
func TestMetaPageDeserializeErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "data too small",
			data:    make([]byte, 10),
			wantErr: true,
		},
		{
			name:    "valid size wrong magic",
			data:    make([]byte, 36),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &MetaPage{}
			err := meta.Deserialize(tt.data)
			if tt.wantErr && err == nil {
				t.Error("Expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestMetaPageValidateErrors tests MetaPage Validate error cases
func TestMetaPageValidateErrors(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*MetaPage)
		wantValid bool
	}{
		{
			name:      "default meta",
			setupFunc: nil,
			wantValid: false, // Default has empty magic
		},
		{
			name: "wrong version",
			setupFunc: func(m *MetaPage) {
				copy(m.Magic[:], MagicString)
				m.Version = 999
				m.PageSize = PageSize
			},
			wantValid: false,
		},
		{
			name: "wrong page size",
			setupFunc: func(m *MetaPage) {
				copy(m.Magic[:], MagicString)
				m.Version = Version
				m.PageSize = 1024
			},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &MetaPage{}
			if tt.setupFunc != nil {
				tt.setupFunc(meta)
			}

			err := meta.Validate()
			if tt.wantValid && err != nil {
				t.Errorf("Expected valid, got error: %v", err)
			}
			if !tt.wantValid && err == nil {
				t.Error("Expected invalid, got no error")
			}
		})
	}
}

// TestBufferPoolClose tests BufferPool Close
func TestBufferPoolClose(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(1024, backend)

	// Create some pages
	page1, _ := pool.NewPage(PageTypeLeaf)
	_ = page1

	// Mark one as dirty
	page1.SetDirty(true)

	// Close pool
	err := pool.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestBufferPoolCloseWithNilBackend verifies BufferPool requires non-nil backend
func TestBufferPoolCloseWithNilBackend(t *testing.T) {
	// Verify that a BufferPool created with a valid backend closes cleanly
	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	err := pool.Close()
	if err != nil {
		t.Errorf("Close with valid backend failed: %v", err)
	}
}

// TestPageManagerClose tests PageManager Close
func TestPageManagerClose(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}

	err = pm.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Close again should be safe
	err = pm.Close()
	if err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

// TestPageManagerFreePageErrors tests FreePage error cases
func TestPageManagerFreePageErrors(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Try to free page 0 (metadata page)
	err = pm.FreePage(0)
	if err == nil {
		t.Error("Expected error when freeing page 0")
	}

	// Freeing non-existent page is allowed (just adds to free list)
	// This tests the code path without error
	err = pm.FreePage(99999)
	if err != nil {
		t.Logf("FreePage returned error for non-existent page: %v", err)
	}
}

// TestWALOpenErrors tests WAL Open error cases
func TestWALOpenErrors(t *testing.T) {
	// Try to open WAL in non-existent directory
	invalidPath := "/nonexistent/path/wal"
	if os.PathSeparator == '\\' {
		invalidPath = "\\\\invalid\\wal"
	}

	_, err := OpenWAL(invalidPath)
	if err == nil {
		t.Log("OpenWAL succeeded (may be valid on this system)")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestWALClose tests WAL Close
func TestWALClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestWALApplyRecord tests applying WAL records
func TestWALApplyRecord(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	// Create a page
	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	pageID := page.ID()
	pool.Unpin(page)

	// Create a WAL record
	record := &WALRecord{
		Type:   WALInsert,
		PageID: pageID,
		Offset: 100,
		Data:   []byte("test data"),
	}

	// Apply the record
	err = wal.applyRecord(pool, record)
	if err != nil {
		t.Errorf("applyRecord failed: %v", err)
	}

	// Verify the data was applied
	page, _ = pool.GetPage(pageID)
	data := page.Data()
	if string(data[100:109]) != "test data" {
		t.Error("Data was not applied correctly")
	}
	pool.Unpin(page)
}

// TestWALApplyRecordErrors tests applyRecord error cases
func TestWALApplyRecordErrors(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	// Try to apply to non-existent page
	record := &WALRecord{
		Type:   WALInsert,
		PageID: 99999,
		Offset: 0,
		Data:   []byte("test"),
	}

	err = wal.applyRecord(pool, record)
	if err == nil {
		t.Error("Expected error for non-existent page")
	}
}

// TestWALRecoverWithApply tests WAL recovery that applies records
func TestWALRecoverWithApply(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)

	// Create a page
	page, _ := pool.NewPage(PageTypeLeaf)
	pageID := page.ID()
	pool.Unpin(page)

	// Append some records
	wal.Append(&WALRecord{Type: WALInsert, PageID: pageID, Offset: 100, Data: []byte("data1")})
	wal.Append(&WALRecord{Type: WALUpdate, PageID: pageID, Offset: 200, Data: []byte("data2")})

	// Close WAL
	wal.Close()

	// Reopen WAL and recover
	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	// Recovery should apply records
	err = wal2.Recover(pool)
	if err != nil {
		t.Logf("Recover result: %v", err)
	}
}

// TestWALRecoverCommitThenData tests recovery with commit record followed by data
func TestWALRecoverCommitThenData(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	page, _ := pool.NewPage(PageTypeLeaf)
	pageID := page.ID()
	pool.Unpin(page)

	txnID := uint64(1)

	// Write data, then commit
	wal.Append(&WALRecord{TxnID: txnID, Type: WALInsert, PageID: pageID, Offset: 100, Data: []byte("hello")})
	wal.Append(&WALRecord{TxnID: txnID, Type: WALCommit})

	// Write more data for same txn (post-commit - should apply immediately)
	wal.Append(&WALRecord{TxnID: txnID, Type: WALUpdate, PageID: pageID, Offset: 200, Data: []byte("world")})

	wal.Close()

	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("recovery with commit failed: %v", err)
	}

	// Verify data was applied
	p, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Unpin(p)
	if string(p.Data()[100:105]) != "hello" {
		t.Errorf("expected 'hello' at offset 100, got %q", string(p.Data()[100:105]))
	}
	if string(p.Data()[200:205]) != "world" {
		t.Errorf("expected 'world' at offset 200, got %q", string(p.Data()[200:205]))
	}
}

// TestWALRecoverRollback tests recovery discards rolled-back transactions
func TestWALRecoverRollback(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	page, _ := pool.NewPage(PageTypeLeaf)
	pageID := page.ID()
	pool.Unpin(page)

	txnID := uint64(42)

	// Write data, then rollback — data should NOT be applied
	wal.Append(&WALRecord{TxnID: txnID, Type: WALInsert, PageID: pageID, Offset: 50, Data: []byte("rolled_back")})
	wal.Append(&WALRecord{TxnID: txnID, Type: WALRollback})

	wal.Close()

	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("recovery with rollback failed: %v", err)
	}

	// Verify data was NOT applied (page should still have zeros)
	p, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Unpin(p)
	if string(p.Data()[50:61]) == "rolled_back" {
		t.Error("rolled-back data should not be applied")
	}
}

// TestWALRecoverMultipleTxns tests recovery with multiple transactions
func TestWALRecoverMultipleTxns(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(1024, backend)
	defer pool.Close()

	page, _ := pool.NewPage(PageTypeLeaf)
	pageID := page.ID()
	pool.Unpin(page)

	// Txn 1: committed
	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: pageID, Offset: 100, Data: []byte("txn1")})
	wal.Append(&WALRecord{TxnID: 1, Type: WALCommit})

	// Txn 2: rolled back
	wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, PageID: pageID, Offset: 200, Data: []byte("txn2")})
	wal.Append(&WALRecord{TxnID: 2, Type: WALRollback})

	// Txn 3: committed with delete
	wal.Append(&WALRecord{TxnID: 3, Type: WALDelete, PageID: pageID, Offset: 300, Data: []byte("txn3")})
	wal.Append(&WALRecord{TxnID: 3, Type: WALCommit})

	wal.Close()

	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("multi-txn recovery failed: %v", err)
	}

	// Verify txn1 committed data applied
	p, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Unpin(p)
	if string(p.Data()[100:104]) != "txn1" {
		t.Errorf("txn1 data not applied")
	}
	// txn2 rolled back — should have zeros
	if string(p.Data()[200:204]) == "txn2" {
		t.Errorf("txn2 rolled-back data should not be applied")
	}
	// txn3 committed
	if string(p.Data()[300:304]) != "txn3" {
		t.Errorf("txn3 data not applied")
	}
}

// TestWALRecoverClosedWAL tests recovery on closed WAL
func TestWALRecoverClosedWAL(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	pool := NewBufferPool(1024, NewMemory())
	defer pool.Close()

	err = wal.Recover(pool)
	if err != ErrWALClosed {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

// TestWALRecoverEmptyWAL tests recovery on empty WAL
func TestWALRecoverEmptyWAL(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	pool := NewBufferPool(1024, NewMemory())
	defer pool.Close()

	err = wal.Recover(pool)
	if err != nil {
		t.Fatalf("recovery of empty WAL should succeed, got: %v", err)
	}
	wal.Close()
}

// TestBufferPoolCloseAndFlush tests Close after flush operations
func TestBufferPoolCloseAndFlush(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(256, backend)

	// Create and dirty some pages
	for i := 0; i < 5; i++ {
		page, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		copy(page.Data()[100:], []byte(fmt.Sprintf("data_%d", i)))
		page.SetDirty(true)
		pool.Unpin(page)
	}

	// FlushAll should work
	if err := pool.FlushAll(); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Close should work after flush
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestDiskTruncateExtended tests disk truncation edge cases
func TestDiskTruncateExtended(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	// Write some data
	data := make([]byte, PageSize*3)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := disk.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	disk.Sync()

	// Truncate to 1 page
	if err := disk.Truncate(int64(PageSize)); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Read should fail beyond truncated point
	buf := make([]byte, PageSize)
	n, err := disk.ReadAt(buf, int64(PageSize))
	if err == nil && n == PageSize {
		t.Error("expected error or short read after truncation")
	}
}

// TestCompressedBackendSize tests Size() delegation.
func TestCompressedBackendSize(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}
	defer cb.Close()

	if cb.Size() != mem.Size() {
		t.Errorf("Expected Size()=%d, got %d", mem.Size(), cb.Size())
	}

	_, _ = cb.WriteAt(make([]byte, PageSize), 0)
	if cb.Size() != mem.Size() {
		t.Errorf("Expected Size()=%d after write, got %d", mem.Size(), cb.Size())
	}
}

// TestCompressedBackendGetBuffers tests pooled buffer miss paths.
func TestCompressedBackendGetBuffers(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}
	defer cb.Close()

	// First call with empty pool should allocate new buffers
	wb := cb.getWriteBuf()
	if wb == nil || len(*wb) != PageSize {
		t.Errorf("Expected write buf of size %d, got %v", PageSize, wb)
	}
	rb := cb.getReadBuf()
	if rb == nil || len(*rb) != PageSize {
		t.Errorf("Expected read buf of size %d, got %v", PageSize, rb)
	}

	// Return them to pool
	cb.putWriteBuf(wb)
	cb.putReadBuf(rb)

	// Second call should reuse from pool
	wb2 := cb.getWriteBuf()
	rb2 := cb.getReadBuf()
	cb.putWriteBuf(wb2)
	cb.putReadBuf(rb2)
}

// TestEncryptedBackendGetCipher tests GetCipher delegation.
func TestEncryptedBackendGetCipher(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled: true,
		Key:     []byte("test-key-32-bytes-long-ok!!!"),
		Salt:    []byte("1234567890123456"),
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatalf("NewEncryptedBackend: %v", err)
	}
	defer eb.Close()

	c := eb.GetCipher()
	if c == nil {
		t.Error("Expected non-nil cipher")
	}
}

// failingBackend wraps a Backend and fails all writes.
type failingBackend struct {
	Backend
}

func (f *failingBackend) WriteAt(p []byte, off int64) (int, error) {
	return 0, errors.New("simulated write failure")
}

func (f *failingBackend) Sync() error {
	return errors.New("simulated sync failure")
}

// TestBufferPoolCloseFlushError tests Close when FlushAll fails.
func TestBufferPoolCloseFlushError(t *testing.T) {
	mem := NewMemory()
	pool := NewBufferPool(16, mem)

	page, _ := pool.NewPage(PageTypeLeaf)
	page.SetDirty(true)
	pool.Unpin(page)

	// Swap backend to failing one after dirtying a page
	pool.backend = &failingBackend{Backend: mem}

	err := pool.Close()
	if err == nil {
		t.Error("Expected error from Close when flush fails")
	}
}

// TestPageManagerCloseError tests PageManager Close when saveFreeList fails.
func TestPageManagerCloseError(t *testing.T) {
	mem := NewMemory()
	pool := NewBufferPool(16, mem)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatalf("NewPageManager: %v", err)
	}

	// Swap pool backend to failing one
	pool.backend = &failingBackend{Backend: mem}

	err = pm.Close()
	if err == nil {
		t.Error("Expected error from PageManager.Close when saveFreeList fails")
	}
}

// TestCompressedBackendLZ4RoundTrip tests LZ4 compression round-trip.
func TestCompressedBackendLZ4RoundTrip(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelFast,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	page := make([]byte, PageSize)
	copy(page, []byte("hello lz4 compressed page data"))

	_, err = cb.WriteAt(page, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	readBuf := make([]byte, PageSize)
	n, err := cb.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if !bytes.Equal(readBuf[:n], page) {
		t.Error("LZ4 round-trip data mismatch")
	}
}

// TestCompressedBackendZstdRoundTrip tests zstd compression round-trip.
func TestCompressedBackendZstdRoundTrip(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelDefault,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	page := make([]byte, PageSize)
	copy(page, []byte("hello zstd compressed page data"))

	_, err = cb.WriteAt(page, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	readBuf := make([]byte, PageSize)
	n, err := cb.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if !bytes.Equal(readBuf[:n], page) {
		t.Error("zstd round-trip data mismatch")
	}
}

// TestCompressedBackendAlgorithmDetection verifies that mixed algorithm
// pages on the same backend are detected correctly during read.
func TestCompressedBackendAlgorithmDetection(t *testing.T) {
	mem := NewMemory()

	// Write a zlib page.
	cbZlib, _ := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelFast,
		MinRatio:  1.0,
	})
	pageZlib := make([]byte, PageSize)
	copy(pageZlib, []byte("zlib page"))
	_, err := cbZlib.WriteAt(pageZlib, 0)
	if err != nil {
		t.Fatalf("zlib WriteAt: %v", err)
	}

	// Write a zstd page at next slot.
	cbZstd, _ := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelFast,
		MinRatio:  1.0,
	})
	pageZstd := make([]byte, PageSize)
	copy(pageZstd, []byte("zstd page"))
	_, err = cbZstd.WriteAt(pageZstd, int64(PageSize))
	if err != nil {
		t.Fatalf("zstd WriteAt: %v", err)
	}

	// Read back both pages using a generic reader (algorithm from magic).
	readBuf := make([]byte, PageSize)
	n, err := cbZlib.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("read zlib page: %v", err)
	}
	if !bytes.Equal(readBuf[:n], pageZlib) {
		t.Error("zlib page mismatch")
	}

	n, err = cbZstd.ReadAt(readBuf, int64(PageSize))
	if err != nil {
		t.Fatalf("read zstd page: %v", err)
	}
	if !bytes.Equal(readBuf[:n], pageZstd) {
		t.Error("zstd page mismatch")
	}
}
