package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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

type shortDiskAtWriter struct {
	n int
}

func (w shortDiskAtWriter) WriteAt(buf []byte, _ int64) (int, error) {
	if w.n > len(buf) {
		return len(buf), nil
	}
	return w.n, nil
}

func TestWriteDiskFullAtRejectsShortWrite(t *testing.T) {
	n, err := writeDiskFullAt(shortDiskAtWriter{n: 2}, []byte("abcdef"), 123)
	if n != 2 {
		t.Fatalf("write count = %d, want 2", n)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestOpenDiskRestrictsExistingFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatalf("write existing db: %v", err)
	}

	backend, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}
	defer backend.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat db: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("database permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestOpenDiskRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.db")
	link := filepath.Join(dir, "link.db")
	if err := os.WriteFile(target, []byte("data"), 0600); err != nil {
		t.Fatalf("write target db: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := OpenDisk(link)
	if err == nil {
		t.Fatal("expected symlink database path to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestOpenDiskRejectsSymlinkParentComponent(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	linkDir := filepath.Join(dir, "link")
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	dbPath := filepath.Join(linkDir, "nested", "test.db")
	backend, err := OpenDisk(dbPath)
	if backend != nil {
		_ = backend.Close()
	}
	if err == nil {
		t.Fatal("expected symlink parent component to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(targetDir, "nested", "test.db")); !os.IsNotExist(statErr) {
		t.Fatalf("database file should not be created through symlink parent, stat err=%v", statErr)
	}
}

func TestOpenDiskDoesNotChmodSymlinkRaceTarget(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "race.db")
	targetPath := filepath.Join(dir, "target.db")
	if err := os.WriteFile(dbPath, []byte("original"), 0600); err != nil {
		t.Fatalf("write original db: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("target"), 0644); err != nil {
		t.Fatalf("write target db: %v", err)
	}

	originalOpenFile := diskOpenFile
	defer func() { diskOpenFile = originalOpenFile }()

	swapped := false
	diskOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if name == filepath.Clean(dbPath) && !swapped {
			swapped = true
			if err := os.Remove(dbPath); err != nil {
				t.Fatalf("remove original db: %v", err)
			}
			if err := os.Symlink(targetPath, dbPath); err != nil {
				t.Skipf("symlink not supported: %v", err)
			}
		}
		return originalOpenFile(name, flag, perm)
	}

	backend, err := OpenDisk(dbPath)
	if backend != nil {
		_ = backend.Close()
	}
	if err == nil {
		t.Fatal("expected raced database path to be rejected")
	}
	if !strings.Contains(err.Error(), "changed while opening") {
		t.Fatalf("expected changed-while-opening error, got %v", err)
	}
	info, statErr := os.Stat(targetPath)
	if statErr != nil {
		t.Fatalf("stat target db: %v", statErr)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("race target permissions = %v, want 0644", info.Mode().Perm())
	}
}

func TestOpenDiskCreateRejectsSymlinkRace(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "new-race.db")
	targetPath := filepath.Join(dir, "target.db")
	if err := os.WriteFile(targetPath, []byte("target"), 0644); err != nil {
		t.Fatalf("write target db: %v", err)
	}

	originalOpenFile := diskOpenFile
	defer func() { diskOpenFile = originalOpenFile }()

	swapped := false
	diskOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if name == filepath.Clean(dbPath) && !swapped {
			swapped = true
			if err := os.Symlink(targetPath, dbPath); err != nil {
				t.Skipf("symlink not supported: %v", err)
			}
		}
		return originalOpenFile(name, flag, perm)
	}

	backend, err := OpenDisk(dbPath)
	if backend != nil {
		_ = backend.Close()
	}
	if err == nil {
		t.Fatal("expected raced database create path to be rejected")
	}
	info, statErr := os.Stat(targetPath)
	if statErr != nil {
		t.Fatalf("stat target db: %v", statErr)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("race target permissions = %v, want 0644", info.Mode().Perm())
	}
}

func TestOpenDiskRejectsNonRegularFile(t *testing.T) {
	_, err := OpenDisk(t.TempDir())
	if err == nil {
		t.Fatal("expected directory database path to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
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
	_, err = backend2.ReadAt(buf2, 0)
	if err != nil {
		t.Fatalf("Failed to read from backend2: %v", err)
	}
	if string(buf2) != string(data) {
		t.Fatalf("Expected %q in backend2, got %q", string(data), string(buf2))
	}
}

func TestMemoryBackendOperationsAfterClose(t *testing.T) {
	backend := NewMemory()
	if _, err := backend.WriteAt([]byte("data"), 0); err != nil {
		t.Fatalf("WriteAt before close: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	buf := make([]byte, 4)
	if _, err := backend.ReadAt(buf, 0); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("ReadAt after close error = %v, want %v", err, ErrBackendClosed)
	}
	if _, err := backend.WriteAt([]byte("x"), 0); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("WriteAt after close error = %v, want %v", err, ErrBackendClosed)
	}
	if err := backend.Sync(); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("Sync after close error = %v, want %v", err, ErrBackendClosed)
	}
	if err := backend.Truncate(0); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("Truncate after close error = %v, want %v", err, ErrBackendClosed)
	}
}

func TestMemoryBackendLoadFromDataInitializesSnapshot(t *testing.T) {
	backend := NewMemory()
	if err := backend.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend.LoadFromData([]byte("snapshot"))

	buf := make([]byte, len("snapshot"))
	if _, err := backend.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after LoadFromData: %v", err)
	}
	if string(buf) != "snapshot" {
		t.Fatalf("LoadFromData content = %q, want snapshot", string(buf))
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
