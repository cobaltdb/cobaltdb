package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestDiskTruncate tests the Truncate method of Disk
func TestDiskTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}
	defer disk.Close()

	// Write some data
	data := make([]byte, 4096)
	copy(data, []byte("test data"))
	_, err = disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify size
	size := disk.Size()
	if size != 4096 {
		t.Errorf("Expected size 4096, got %d", size)
	}

	// Truncate to smaller size
	err = disk.Truncate(2048)
	if err != nil {
		t.Errorf("Failed to truncate: %v", err)
	}

	// Verify new size
	size = disk.Size()
	if size != 2048 {
		t.Errorf("Expected size 2048 after truncate, got %d", size)
	}
}

// TestMemoryTruncate tests the Truncate method of Memory
func TestMemoryTruncate(t *testing.T) {
	mem := NewMemory()

	// Write some data
	data := make([]byte, 4096)
	copy(data, []byte("test data"))
	_, err := mem.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify size
	size := mem.Size()
	if size != 4096 {
		t.Errorf("Expected size 4096, got %d", size)
	}

	// Truncate to smaller size
	err = mem.Truncate(2048)
	if err != nil {
		t.Errorf("Failed to truncate: %v", err)
	}

	// Verify new size
	size = mem.Size()
	if size != 2048 {
		t.Errorf("Expected size 2048 after truncate, got %d", size)
	}
}

// TestDiskOpenError tests error handling when opening disk with invalid path
func TestDiskOpenError(t *testing.T) {
	// Try to open a disk in a non-existent directory
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

// TestDiskSync tests the Sync method
func TestDiskSync(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("Failed to open disk: %v", err)
	}
	defer disk.Close()

	// Write some data
	data := make([]byte, 4096)
	_, err = disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Sync
	err = disk.Sync()
	if err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

// TestPageDeserializeHeader tests DeserializeHeader method
func TestPageDeserializeHeader(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)

	// Set some header values
	page.Header.PageID = 42
	page.Header.PageType = PageTypeInternal
	page.Header.Flags = 0x03
	page.Header.CellCount = 10
	page.Header.FreeStart = 100
	page.Header.FreeEnd = 200
	page.Header.RightPtr = 999

	// Serialize header to data
	page.SerializeHeader()

	// Clear header
	page.Header = PageHeader{}

	// Deserialize
	page.DeserializeHeader()

	// Verify
	if page.Header.PageID != 42 {
		t.Errorf("Expected PageID 42, got %d", page.Header.PageID)
	}
	if page.Header.PageType != PageTypeInternal {
		t.Errorf("Expected PageTypeInternal, got %d", page.Header.PageType)
	}
	if page.Header.Flags != 0x03 {
		t.Errorf("Expected Flags 0x03, got %d", page.Header.Flags)
	}
	if page.Header.CellCount != 10 {
		t.Errorf("Expected CellCount 10, got %d", page.Header.CellCount)
	}
	if page.Header.FreeStart != 100 {
		t.Errorf("Expected FreeStart 100, got %d", page.Header.FreeStart)
	}
	if page.Header.FreeEnd != 200 {
		t.Errorf("Expected FreeEnd 200, got %d", page.Header.FreeEnd)
	}
	if page.Header.RightPtr != 999 {
		t.Errorf("Expected RightPtr 999, got %d", page.Header.RightPtr)
	}
}

// TestCachedPageSetData tests SetData method
func TestCachedPageSetData(t *testing.T) {
	page := &CachedPage{
		id:   1,
		data: bytes.Repeat([]byte{0xff}, PageSize),
	}

	newData := []byte("test data")
	page.SetData(newData)
	newData[0] = 'X'

	if len(page.data) != PageSize {
		t.Fatalf("Expected page data length %d, got %d", PageSize, len(page.data))
	}
	if string(page.data[:9]) != "test data" {
		t.Errorf("Expected data prefix 'test data', got %q", string(page.data[:9]))
	}
	if page.data[9] != 0 {
		t.Errorf("Expected stale data to be cleared, got byte %x", page.data[9])
	}

	empty := &CachedPage{id: 2}
	empty.SetData([]byte("x"))
	if len(empty.data) != PageSize {
		t.Fatalf("Expected empty page data length %d, got %d", PageSize, len(empty.data))
	}
	if string(empty.data[:1]) != "x" {
		t.Errorf("Expected empty page data prefix 'x', got %q", string(empty.data[:1]))
	}
}

// TestPageSetDirty tests SetDirty method
func TestPageSetDirty(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)

	// Initially not dirty
	if page.IsDirty() {
		t.Error("New page should not be dirty")
	}

	// Set dirty
	page.SetDirty(true)
	if !page.IsDirty() {
		t.Error("Page should be dirty after SetDirty(true)")
	}

	// Clear dirty
	page.SetDirty(false)
	if page.IsDirty() {
		t.Error("Page should not be dirty after SetDirty(false)")
	}
}

// TestPageSetPinned tests SetPinned method
func TestPageSetPinned(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)

	// Initially not pinned
	if page.IsPinned() {
		t.Error("New page should not be pinned")
	}

	// Set pinned
	page.SetPinned(true)
	if !page.IsPinned() {
		t.Error("Page should be pinned after SetPinned(true)")
	}

	// Clear pinned
	page.SetPinned(false)
	if page.IsPinned() {
		t.Error("Page should not be pinned after SetPinned(false)")
	}
}

// TestOpenDiskInvalidPath covers OpenDisk with a path that cannot be
// created (nonexistent parent). Ported from coverage_boost_storage_test.go
// — no untagged test exercised OpenDisk with an invalid path.
func TestOpenDiskInvalidPath(t *testing.T) {
	_, err := OpenDisk("/nonexistent/path/that/cannot/be/created/test.db")
	if err == nil {
		t.Error("Expected error for invalid disk path")
	}
}

// TestOpenWALInvalidPath covers OpenWAL with an invalid path.
// Ported from coverage_boost_storage_test.go.
func TestOpenWALInvalidPath(t *testing.T) {
	_, err := OpenWAL("/nonexistent/path/that/cannot/be/created/test.wal")
	if err == nil {
		t.Error("Expected error for invalid WAL path")
	}
}

// TestDiskBackendCloseIdempotent covers calling Close twice on a
// disk backend. The second close should not panic. Ported from
// coverage_boost_storage_test.go.
func TestDiskBackendCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backend, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("First close: %v", err)
	}
	// Second close should not panic
	if err := backend.Close(); err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// TestMemoryBackendCloseIdempotent covers double-close on a memory
// backend. Ported from coverage_boost_storage_test.go.
func TestMemoryBackendCloseIdempotent(t *testing.T) {
	mem := NewMemory()
	if err := mem.Close(); err != nil {
		t.Fatalf("First close: %v", err)
	}
	if err := mem.Close(); err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}
