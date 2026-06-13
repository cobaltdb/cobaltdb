package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistLoadSaltRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	salt := []byte("0123456789abcdef0123456789abcdef")

	if err := PersistSalt(dbPath, salt); err != nil {
		t.Fatalf("PersistSalt failed: %v", err)
	}

	loaded, err := LoadSalt(dbPath)
	if err != nil {
		t.Fatalf("LoadSalt failed: %v", err)
	}
	if string(loaded) != string(salt) {
		t.Fatalf("salt mismatch: got %q want %q", loaded, salt)
	}

	info, err := os.Stat(dbPath + ".salt")
	if err != nil {
		t.Fatalf("salt file stat failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("salt file permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestPersistSaltAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	first := []byte("first-salt")
	second := []byte("second-salt")

	if err := PersistSalt(dbPath, first); err != nil {
		t.Fatalf("PersistSalt first failed: %v", err)
	}
	if err := PersistSalt(dbPath, second); err != nil {
		t.Fatalf("PersistSalt second failed: %v", err)
	}

	loaded, err := LoadSalt(dbPath)
	if err != nil {
		t.Fatalf("LoadSalt failed: %v", err)
	}
	if string(loaded) != string(second) {
		t.Fatalf("salt mismatch after replace: got %q want %q", loaded, second)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "test.db.salt.tmp-*"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary salt files left behind: %v", matches)
	}
}

func TestPersistSaltRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	linkDir := filepath.Join(dir, "data")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	dbPath := filepath.Join(linkDir, "test.db")
	err := PersistSalt(dbPath, []byte("secret-salt"))
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("PersistSalt symlink dir error = %v, want symlink rejection", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "test.db.salt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("salt was written through symlink, stat err=%v", err)
	}
}

func TestPrepareAtomicFileDirCreatesRestrictiveDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "sidecars")
	if err := prepareAtomicFileDir(dir); err != nil {
		t.Fatalf("prepareAtomicFileDir: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0750 {
		t.Fatalf("atomic file dir mode = %o, want 750", got)
	}
}

func TestWriteFileFullRejectsShortWrite(t *testing.T) {
	writer := &shortFileWriter{limit: 5}

	n, err := writeFileFull(writer, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFileFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 5 {
		t.Fatalf("writeFileFull wrote %d bytes, want 5", n)
	}
}

func TestPersistLoadSaltEdges(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "missing.db")

	loaded, err := LoadSalt(dbPath)
	if err != nil {
		t.Fatalf("LoadSalt missing file failed: %v", err)
	}
	if loaded != nil {
		t.Fatalf("LoadSalt missing file = %v, want nil", loaded)
	}

	if err := PersistSalt(dbPath, nil); err != nil {
		t.Fatalf("PersistSalt empty failed: %v", err)
	}
	if _, err := os.Stat(dbPath + ".salt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty salt should not create file, stat err = %v", err)
	}

	badPath := filepath.Join(dir, "bad.db")
	if err := os.WriteFile(badPath+".salt", []byte("bad marker"), 0600); err != nil {
		t.Fatalf("write bad salt file: %v", err)
	}
	if _, err := LoadSalt(badPath); !errors.Is(err, ErrInvalidSalt) {
		t.Fatalf("LoadSalt bad marker error = %v, want ErrInvalidSalt", err)
	}

	emptySaltPath := filepath.Join(dir, "empty.db")
	if err := os.WriteFile(emptySaltPath+".salt", []byte(saltFileMarker+"\n"), 0600); err != nil {
		t.Fatalf("write empty salt file: %v", err)
	}
	if _, err := LoadSalt(emptySaltPath); !errors.Is(err, ErrInvalidSalt) {
		t.Fatalf("LoadSalt empty salt error = %v, want ErrInvalidSalt", err)
	}
}

func TestSaltRejectsOversizedInputs(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oversized.db")
	oversizedSalt := []byte(strings.Repeat("x", maxEncryptionSaltBytes+1))

	if err := PersistSalt(dbPath, oversizedSalt); !errors.Is(err, ErrInvalidSalt) {
		t.Fatalf("PersistSalt oversized error = %v, want ErrInvalidSalt", err)
	}
	if _, err := os.Stat(dbPath + ".salt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized salt should not create sidecar, stat err = %v", err)
	}

	if err := os.WriteFile(dbPath+".salt", append([]byte(saltFileMarker+"\n"), oversizedSalt...), 0600); err != nil {
		t.Fatalf("write oversized salt file: %v", err)
	}
	if _, err := LoadSalt(dbPath); !errors.Is(err, ErrInvalidSalt) {
		t.Fatalf("LoadSalt oversized error = %v, want ErrInvalidSalt", err)
	}
}

func TestEncryptedBackendRejectsOversizedSalt(t *testing.T) {
	_, err := NewEncryptedBackend(NewMemory(), &EncryptionConfig{
		Enabled: true,
		Key:     []byte("oversized-salt-test-password-32"),
		Salt:    []byte(strings.Repeat("s", maxEncryptionSaltBytes+1)),
	})
	if !errors.Is(err, ErrInvalidSalt) {
		t.Fatalf("NewEncryptedBackend oversized salt error = %v, want ErrInvalidSalt", err)
	}
}

func TestLoadSaltRejectsUnsafePath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "unsafe.db")
	saltPath := dbPath + ".salt"
	targetPath := filepath.Join(dir, "target.salt")
	if err := os.WriteFile(targetPath, []byte(saltFileMarker+"\nsecret"), 0600); err != nil {
		t.Fatalf("write target salt: %v", err)
	}
	if err := os.Symlink(targetPath, saltPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := LoadSalt(dbPath)
	if err == nil {
		t.Fatal("expected symlink salt path to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	dirBackedDB := filepath.Join(dir, "dir-backed.db")
	dirBackedSalt := dirBackedDB + ".salt"
	if err := os.Mkdir(dirBackedSalt, 0750); err != nil {
		t.Fatalf("mkdir salt path: %v", err)
	}
	_, err = LoadSalt(dirBackedDB)
	if err == nil {
		t.Fatal("expected directory salt path to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestLoadSaltRestrictsExistingPermissions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	saltPath := dbPath + ".salt"
	if err := os.WriteFile(saltPath, []byte(saltFileMarker+"\nsecret"), 0644); err != nil {
		t.Fatalf("write salt: %v", err)
	}

	if _, err := LoadSalt(dbPath); err != nil {
		t.Fatalf("LoadSalt failed: %v", err)
	}
	info, err := os.Stat(saltPath)
	if err != nil {
		t.Fatalf("stat salt: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("salt permissions = %v, want 0600", info.Mode().Perm())
	}
}

type shortFileWriter struct {
	limit int
}

func (w *shortFileWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}
