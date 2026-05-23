package storage

import (
	"errors"
	"os"
	"path/filepath"
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
