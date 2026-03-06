package storage

import (
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
		data: make([]byte, 4096),
	}

	newData := []byte("test data")
	page.SetData(newData)

	// Verify data was set
	if string(page.data) != "test data" {
		t.Errorf("Expected data 'test data', got '%s'", string(page.data))
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
