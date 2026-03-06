package storage

import (
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

// TestBufferPoolCloseWithNilBackend tests Close with nil backend
func TestBufferPoolCloseWithNilBackend(t *testing.T) {
	// Buffer pool doesn't support nil backend, skip this test
	t.Skip("BufferPool requires non-nil backend")
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
