package storage

import (
	"testing"
)

func TestDiskBackend(t *testing.T) {
	// Create temp file
	tmpFile := t.TempDir() + "/test.cb"

	backend, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open disk backend: %v", err)
	}
	defer backend.Close()

	// Test write
	data := []byte("Hello, CobaltDB!")
	n, err := backend.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// Test read
	buf := make([]byte, len(data))
	n, err = backend.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, read %d", len(data), n)
	}
	if string(buf) != string(data) {
		t.Fatalf("Expected %q, got %q", string(data), string(buf))
	}

	// Test size
	size := backend.Size()
	if size != int64(len(data)) {
		t.Fatalf("Expected size %d, got %d", len(data), size)
	}

	// Test truncate
	if err := backend.Truncate(100); err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}
	if backend.Size() != 100 {
		t.Fatalf("Expected size 100 after truncate, got %d", backend.Size())
	}

	// Test sync
	if err := backend.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
}

func TestMemoryBackend(t *testing.T) {
	backend := NewMemory()
	defer backend.Close()

	// Test write
	data := []byte("Hello, CobaltDB!")
	n, err := backend.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// Test read
	buf := make([]byte, len(data))
	n, err = backend.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, read %d", len(data), n)
	}
	if string(buf) != string(data) {
		t.Fatalf("Expected %q, got %q", string(data), string(buf))
	}

	// Test size
	size := backend.Size()
	if size != int64(len(data)) {
		t.Fatalf("Expected size %d, got %d", len(data), size)
	}

	// Test Data and LoadFromData
	backendData := backend.Data()
	backend2 := NewMemory()
	backend2.LoadFromData(backendData)

	buf2 := make([]byte, len(data))
	n, err = backend2.ReadAt(buf2, 0)
	if err != nil {
		t.Fatalf("Failed to read from backend2: %v", err)
	}
	if string(buf2) != string(data) {
		t.Fatalf("Expected %q in backend2, got %q", string(data), string(buf2))
	}
}

func TestPage(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)

	if page.Header.PageID != 1 {
		t.Errorf("Expected PageID 1, got %d", page.Header.PageID)
	}
	if page.Header.PageType != PageTypeLeaf {
		t.Errorf("Expected PageType %d, got %d", PageTypeLeaf, page.Header.PageType)
	}

	// Test free space
	freeSpace := page.FreeSpace()
	if freeSpace <= 0 {
		t.Errorf("Expected positive free space, got %d", freeSpace)
	}

	// Test dirty flag
	page.SetDirty(true)
	if !page.IsDirty() {
		t.Error("Expected page to be dirty")
	}

	// Test pinned flag
	page.SetPinned(true)
	if !page.IsPinned() {
		t.Error("Expected page to be pinned")
	}
}

func TestMetaPage(t *testing.T) {
	meta := NewMetaPage()

	if string(meta.Magic[:]) != "CBDB" {
		t.Errorf("Expected magic 'CBDB', got %q", string(meta.Magic[:]))
	}
	if meta.Version != Version {
		t.Errorf("Expected version %d, got %d", Version, meta.Version)
	}
	if meta.PageSize != PageSize {
		t.Errorf("Expected page size %d, got %d", PageSize, meta.PageSize)
	}

	// Test serialize/deserialize
	page := NewPage(0, PageTypeMeta)
	meta.Serialize(page.Data)

	meta2 := &MetaPage{}
	if err := meta2.Deserialize(page.Data); err != nil {
		t.Fatalf("Failed to deserialize meta page: %v", err)
	}

	if string(meta2.Magic[:]) != "CBDB" {
		t.Errorf("Deserialized magic mismatch")
	}
	if meta2.Version != meta.Version {
		t.Errorf("Deserialized version mismatch")
	}
}
