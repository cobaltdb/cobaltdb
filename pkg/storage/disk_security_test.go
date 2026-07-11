package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncDiskParentDirRejectsSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	linkDir := filepath.Join(dir, "data")
	if err := os.Mkdir(targetDir, 0700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Fatalf("symlink not supported: %v", err)
	}

	err := syncDiskParentDir(filepath.Join(linkDir, "db.cobalt"))
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("syncDiskParentDir symlink error = %v, want symlink rejection", err)
	}
}
