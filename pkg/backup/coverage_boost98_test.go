package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestCopyWALFilesErrors tests copyWALFiles error paths
func TestCopyWALFilesErrors(t *testing.T) {
	tempDir := t.TempDir()

	// Test 1: WAL path is empty
	t.Run("EmptyWALPath", func(t *testing.T) {
		dbFile := filepath.Join(tempDir, "test1.db")
		os.WriteFile(dbFile, []byte("test"), 0644)

		config := DefaultConfig()
		config.BackupDir = filepath.Join(tempDir, "backups1")

		db := &MockDatabase{dbPath: dbFile} // No WAL path
		mgr := NewManager(config, db)

		backup := &Backup{ID: "test-backup"}
		err := mgr.copyWALFiles(context.Background(), backup)
		if err != nil {
			t.Errorf("Should return nil for empty WAL path, got: %v", err)
		}
	})

	// Test 2: WAL path is a file (not directory) - causes ReadDir error
	t.Run("WALPathIsFile", func(t *testing.T) {
		dbFile := filepath.Join(tempDir, "test2.db")
		walFile := filepath.Join(tempDir, "test2.wal")
		os.WriteFile(dbFile, []byte("test"), 0644)
		os.WriteFile(walFile, []byte("not a dir"), 0644)

		config := DefaultConfig()
		config.BackupDir = filepath.Join(tempDir, "backups2")

		db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walFile}
		mgr := NewManager(config, db)

		backup := &Backup{ID: "test-backup2"}
		err := mgr.copyWALFiles(context.Background(), backup)
		if err == nil {
			t.Error("Should return error when WAL path is not a directory")
		}
	})

	// Test 3: WAL directory with subdirectory (should be skipped)
	t.Run("WALWithSubdirectory", func(t *testing.T) {
		dbFile := filepath.Join(tempDir, "test3.db")
		walDir := filepath.Join(tempDir, "test3.wal")
		os.WriteFile(dbFile, []byte("test"), 0644)
		os.MkdirAll(walDir, 0755)
		// Create a subdirectory in WAL dir
		subDir := filepath.Join(walDir, "subdir")
		os.MkdirAll(subDir, 0755)
		// Create a WAL file
		os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("wal data"), 0644)

		config := DefaultConfig()
		config.BackupDir = filepath.Join(tempDir, "backups3")

		db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir}
		mgr := NewManager(config, db)

		backup := &Backup{ID: "test-backup3"}
		err := mgr.copyWALFiles(context.Background(), backup)
		if err != nil {
			t.Errorf("Should succeed, got error: %v", err)
		}
		// Should have only copied the file, not the subdirectory
		if len(backup.WALFiles) != 1 {
			t.Errorf("Expected 1 WAL file, got %d", len(backup.WALFiles))
		}
	})
}

// TestVerifyBackupGzipError tests verifyBackup with gzip error
func TestVerifyBackupGzipError(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: filepath.Join(tempDir, "test.db")}
	mgr := NewManager(config, db)

	// Create a backup file with .gz extension but invalid gzip content
	backup := &Backup{
		ID:          "test-gz",
		Destination: filepath.Join(tempDir, "test.gz"),
		Checksum:    12345,
	}
	os.WriteFile(backup.Destination, []byte("not valid gzip content"), 0644)

	err := mgr.verifyBackup(backup)
	if err == nil {
		t.Error("Should return error for invalid gzip content")
	}
}

// TestCreateBackupCopyWALError tests backup when WAL copy fails
func TestCreateBackupCopyWALError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	walDir := filepath.Join(tempDir, "test.wal")
	os.WriteFile(dbFile, []byte("test database"), 0644)
	os.MkdirAll(walDir, 0755)

	// Create a WAL file that will fail to copy (by making source unreadable)
	walFile := filepath.Join(walDir, "000001.wal")
	os.WriteFile(walFile, []byte("wal content"), 0000) // No read permissions

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	// May or may not fail depending on OS permissions
	if err != nil {
		t.Logf("Backup failed as expected with unreadable WAL: %v", err)
	}

	// Restore permissions for cleanup
	os.Chmod(walFile, 0644)
}

// TestCopyFilePermissionError tests copyFile with permission issues
func TestCopyFilePermissionError(t *testing.T) {
	tempDir := t.TempDir()

	// Create source file
	src := filepath.Join(tempDir, "src")
	os.WriteFile(src, []byte("content"), 0644)

	// Try to copy to a directory that doesn't exist and can't be created
	// (This tests the MkdirAll error path)
	dst := filepath.Join(tempDir, "nonexistent", "subdir", "dest")
	err := copyFile(src, dst)
	if err == nil {
		t.Error("Should return error when destination directory can't be created")
	}
}

// TestDeleteBackupWithMissingFile tests deleting backup when file already gone
func TestDeleteBackupWithMissingFile(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: filepath.Join(tempDir, "test.db")}
	mgr := NewManager(config, db)

	// Add backup metadata without creating the file
	backup := &Backup{
		ID:          "missing-file",
		Destination: filepath.Join(tempDir, "nonexistent.backup"),
	}
	mgr.metadata.Backups = append(mgr.metadata.Backups, backup)

	// Should succeed even if file is missing (best effort)
	err := mgr.DeleteBackup(backup.ID)
	if err != nil {
		t.Logf("DeleteBackup with missing file returned: %v", err)
	}
}

// TestCleanupOldBackupsEmptyList tests cleanup with empty backup list
func TestCleanupOldBackupsEmptyList(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = tempDir
	config.MaxBackups = 5

	db := &MockDatabase{dbPath: filepath.Join(tempDir, "test.db")}
	mgr := NewManager(config, db)

	err := mgr.cleanupOldBackups()
	if err != nil {
		t.Errorf("Should succeed with empty list, got: %v", err)
	}
}

// TestCopyFileDstExists tests copying when destination exists
func TestCopyFileDstExists(t *testing.T) {
	tempDir := t.TempDir()

	src := filepath.Join(tempDir, "src")
	dst := filepath.Join(tempDir, "dst")
	os.WriteFile(src, []byte("new content"), 0644)
	os.WriteFile(dst, []byte("old content"), 0644)

	err := copyFile(src, dst)
	if err != nil {
		t.Errorf("Should succeed, got: %v", err)
	}

	// Verify content was overwritten
	content, _ := os.ReadFile(dst)
	if string(content) != "new content" {
		t.Error("Destination file was not overwritten")
	}
}
