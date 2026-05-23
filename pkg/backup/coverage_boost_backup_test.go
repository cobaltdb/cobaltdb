package backup

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDeleteBackupWithWALFiles tests deleting a backup that has WAL files
func TestDeleteBackupWithWALFiles(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL files
	if err := os.WriteFile(filepath.Join(walDir, "wal_1.log"), []byte("wal content 1"), 0644); err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify WAL backup directory exists
	walBackupDir := filepath.Join(config.BackupDir, backup.ID+"_wal")
	if _, err := os.Stat(walBackupDir); os.IsNotExist(err) {
		t.Fatal("WAL backup directory should exist before deletion")
	}

	// Delete the backup
	if err := mgr.DeleteBackup(backup.ID); err != nil {
		t.Fatalf("Failed to delete backup: %v", err)
	}

	// Verify WAL backup directory is removed
	if _, err := os.Stat(walBackupDir); !os.IsNotExist(err) {
		t.Error("WAL backup directory should be removed after deletion")
	}

	// Verify backup is removed from metadata
	if mgr.GetBackup(backup.ID) != nil {
		t.Error("Backup should be removed from metadata")
	}
}

// TestDeleteBackupWithoutWALFiles tests deleting a backup without WAL files
func TestDeleteBackupWithoutWALFiles(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify no WAL files in backup
	if len(backup.WALFiles) != 0 {
		t.Error("Backup should not have WAL files")
	}

	// Delete the backup
	if err := mgr.DeleteBackup(backup.ID); err != nil {
		t.Fatalf("Failed to delete backup: %v", err)
	}

	// Verify backup file is removed
	if _, err := os.Stat(backup.Destination); !os.IsNotExist(err) {
		t.Error("Backup file should be removed after deletion")
	}
}

// TestDeleteBackupFileNotFound tests deleting a backup when file is already removed
func TestDeleteBackupFileNotFound(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Create a backup entry with a file that doesn't exist
	backupFile := filepath.Join(tempDir, "nonexistent.db")
	backup := &Backup{
		ID:          "test_backup",
		Destination: backupFile,
		CompletedAt: time.Now(),
	}
	mgr.metadata.Backups = append(mgr.metadata.Backups, backup)

	// Delete should succeed even if file doesn't exist
	if err := mgr.DeleteBackup("test_backup"); err != nil {
		t.Errorf("DeleteBackup should succeed even if file doesn't exist: %v", err)
	}

	// Verify backup is removed from metadata
	if mgr.GetBackup("test_backup") != nil {
		t.Error("Backup should be removed from metadata")
	}
}

// TestDeleteBackupNonExistent tests deleting a backup that doesn't exist
func TestDeleteBackupNonExistent(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	err := mgr.DeleteBackup("non_existent_backup")
	if err == nil {
		t.Error("Expected error for non-existent backup")
	}

	expectedMsg := "backup not found: non_existent_backup"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestCopyWALFilesEmptyWALDir tests copyWALFiles with empty WAL directory
func TestCopyWALFilesEmptyWALDir(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Should have no WAL files since WAL directory is empty
	if len(backup.WALFiles) != 0 {
		t.Errorf("Expected 0 WAL files for empty WAL dir, got %d", len(backup.WALFiles))
	}
}

// TestCopyWALFilesNoWALPath tests copyWALFiles when GetWALPath returns empty
func TestCopyWALFilesNoWALPath(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	// MockDatabase returns empty WAL path
	db := &MockDatabase{dbPath: dbFile, walPath: ""}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Should have no WAL files since WAL path is empty
	if len(backup.WALFiles) != 0 {
		t.Errorf("Expected 0 WAL files when WAL path is empty, got %d", len(backup.WALFiles))
	}
}

// TestCopyWALFilesWithSubdirectories tests that copyWALFiles skips directories
func TestCopyWALFilesWithSubdirectories(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)
	os.MkdirAll(filepath.Join(walDir, "subdir"), 0755)

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL files
	if err := os.WriteFile(filepath.Join(walDir, "wal_1.log"), []byte("wal content 1"), 0644); err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Should only have 1 WAL file (subdirectory should be skipped)
	if len(backup.WALFiles) != 1 {
		t.Errorf("Expected 1 WAL file (subdir skipped), got %d", len(backup.WALFiles))
	}
}

// TestCopyWALFilesNonExistentWALDir tests copyWALFiles with non-existent WAL directory
func TestCopyWALFilesNonExistentWALDir(t *testing.T) {
	tempDir := t.TempDir()
	nonExistentWALDir := filepath.Join(tempDir, "nonexistent_wal")

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: nonExistentWALDir, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when WAL directory doesn't exist")
	}
}

// TestCleanupOldBackupsByRetentionOnly tests cleanup based only on retention period
func TestCleanupOldBackupsByRetentionOnly(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 0 // Disable max backups
	config.RetentionPeriod = 1 * time.Hour

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add old backup (beyond retention)
	oldBackup := &Backup{
		ID:          "old_backup",
		Destination: filepath.Join(config.BackupDir, "old_backup.db"),
		CompletedAt: time.Now().Add(-2 * time.Hour),
	}
	os.WriteFile(oldBackup.Destination, []byte("old data"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, oldBackup)

	// Add recent backup (within retention)
	recentBackup := &Backup{
		ID:          "recent_backup",
		Destination: filepath.Join(config.BackupDir, "recent_backup.db"),
		CompletedAt: time.Now().Add(-30 * time.Minute),
	}
	os.WriteFile(recentBackup.Destination, []byte("recent data"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, recentBackup)

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Only recent backup should remain
	if len(mgr.metadata.Backups) != 1 {
		t.Errorf("Expected 1 backup after retention cleanup, got %d", len(mgr.metadata.Backups))
	}

	if mgr.metadata.Backups[0].ID != "recent_backup" {
		t.Error("Recent backup should remain")
	}
}

// TestCleanupOldBackupsByCountOnly tests cleanup based only on max count
func TestCleanupOldBackupsByCountOnly(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 2
	config.RetentionPeriod = 0 // Disable retention

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	now := time.Now()

	// Add 3 backups
	for i := 1; i <= 3; i++ {
		backup := &Backup{
			ID:          "backup_" + string(rune('0'+i)),
			Destination: filepath.Join(config.BackupDir, "backup_"+string(rune('0'+i))+".db"),
			CompletedAt: now.Add(-time.Duration(4-i) * time.Hour),
		}
		os.WriteFile(backup.Destination, []byte("data"), 0644)
		mgr.metadata.Backups = append(mgr.metadata.Backups, backup)
	}

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Should have only 2 backups (the newest ones)
	if len(mgr.metadata.Backups) != 2 {
		t.Errorf("Expected 2 backups after count cleanup, got %d", len(mgr.metadata.Backups))
	}

	// Oldest backup (backup_1) should be removed
	for _, b := range mgr.metadata.Backups {
		if b.ID == "backup_1" {
			t.Error("Oldest backup should have been removed")
		}
	}
}

// TestCleanupOldBackupsCombinedPolicy tests cleanup with both retention and count
func TestCleanupOldBackupsCombinedPolicy(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 3
	config.RetentionPeriod = 2 * time.Hour

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	now := time.Now()

	// Add 4 backups - 2 old (beyond retention), 2 recent
	backups := []*Backup{
		{ID: "backup_1", CompletedAt: now.Add(-4 * time.Hour)}, // old, will be removed by retention
		{ID: "backup_2", CompletedAt: now.Add(-3 * time.Hour)}, // old, will be removed by retention
		{ID: "backup_3", CompletedAt: now.Add(-1 * time.Hour)}, // recent
		{ID: "backup_4", CompletedAt: now},                     // recent
	}

	for _, b := range backups {
		b.Destination = filepath.Join(config.BackupDir, b.ID+".db")
		os.WriteFile(b.Destination, []byte("data"), 0644)
		mgr.metadata.Backups = append(mgr.metadata.Backups, b)
	}

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Should have 2 recent backups
	if len(mgr.metadata.Backups) != 2 {
		t.Errorf("Expected 2 backups after cleanup, got %d", len(mgr.metadata.Backups))
	}
}

// TestCleanupOldBackupsNoPolicy tests cleanup when no policy is set
func TestCleanupOldBackupsNoPolicy(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 0      // No limit
	config.RetentionPeriod = 0 // No retention

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add backups
	for i := 1; i <= 5; i++ {
		backup := &Backup{
			ID:          "backup_" + string(rune('0'+i)),
			Destination: filepath.Join(config.BackupDir, "backup_"+string(rune('0'+i))+".db"),
			CompletedAt: time.Now().Add(-time.Duration(i) * time.Hour),
		}
		os.WriteFile(backup.Destination, []byte("data"), 0644)
		mgr.metadata.Backups = append(mgr.metadata.Backups, backup)
	}

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// All backups should remain since no policy is set
	if len(mgr.metadata.Backups) != 5 {
		t.Errorf("Expected 5 backups (no cleanup), got %d", len(mgr.metadata.Backups))
	}
}

// TestCleanupOldBackupsEmptyMetadata tests cleanup with no backups
func TestCleanupOldBackupsEmptyMetadata(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 2
	config.RetentionPeriod = 1 * time.Hour

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	if len(mgr.metadata.Backups) != 0 {
		t.Errorf("Expected 0 backups, got %d", len(mgr.metadata.Backups))
	}
}

// TestCleanupOldBackupsWithWALFiles tests cleanup removes WAL files too
func TestCleanupOldBackupsWithWALFiles(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 1
	config.RetentionPeriod = 0

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add backup with WAL files
	oldBackup := &Backup{
		ID:          "old_backup",
		Destination: filepath.Join(config.BackupDir, "old_backup.db"),
		CompletedAt: time.Now().Add(-2 * time.Hour),
		WALFiles:    []string{"wal_1.log", "wal_2.log"},
	}
	os.WriteFile(oldBackup.Destination, []byte("old data"), 0644)

	// Create WAL directory
	walDir := filepath.Join(config.BackupDir, "old_backup_wal")
	os.MkdirAll(walDir, 0755)
	os.WriteFile(filepath.Join(walDir, "wal_1.log"), []byte("wal1"), 0644)
	os.WriteFile(filepath.Join(walDir, "wal_2.log"), []byte("wal2"), 0644)

	mgr.metadata.Backups = append(mgr.metadata.Backups, oldBackup)

	// Add newer backup
	newBackup := &Backup{
		ID:          "new_backup",
		Destination: filepath.Join(config.BackupDir, "new_backup.db"),
		CompletedAt: time.Now(),
	}
	os.WriteFile(newBackup.Destination, []byte("new data"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, newBackup)

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Old backup should be removed
	if len(mgr.metadata.Backups) != 1 || mgr.metadata.Backups[0].ID != "new_backup" {
		t.Error("Old backup should have been removed")
	}

	// WAL directory should be removed
	if _, err := os.Stat(walDir); !os.IsNotExist(err) {
		t.Error("WAL directory should be removed")
	}
}

// TestVerifyBackupWithWrongSize tests verification with wrong file size
func TestVerifyBackupWithWrongSize(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := []byte("test content for verification")
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Modify the backup file content (changes size and checksum)
	if err := os.WriteFile(backup.Destination, []byte("modified content"), 0644); err != nil {
		t.Fatalf("Failed to modify backup: %v", err)
	}

	// Verify should fail
	if err := mgr.verifyBackup(backup); err == nil {
		t.Error("Verification should fail for modified backup")
	}
}

// TestVerifyBackupMissingFile tests verification when backup file is missing
func TestVerifyBackupMissingFile(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	backup := &Backup{
		ID:          "test_backup",
		Destination: filepath.Join(config.BackupDir, "nonexistent.db"),
		Checksum:    12345,
	}

	err := mgr.verifyBackup(backup)
	if err == nil {
		t.Error("Expected error when backup file is missing")
	}
}

// TestVerifyBackupCompressed tests verification of compressed backup
func TestVerifyBackupCompressed(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*10)
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 6

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify should pass for valid compressed backup
	if err := mgr.verifyBackup(backup); err != nil {
		t.Errorf("Verification failed for valid compressed backup: %v", err)
	}
}

// TestVerifyBackupWithCallback tests the OnVerify callback
func TestVerifyBackupWithCallback(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	verifyCalled := false
	verifyResult := false
	mgr.OnVerify = func(backup *Backup, valid bool) {
		verifyCalled = true
		verifyResult = valid
	}

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify should pass and call callback
	if err := mgr.verifyBackup(backup); err != nil {
		t.Errorf("Verification failed: %v", err)
	}

	if !verifyCalled {
		t.Error("OnVerify callback should have been called")
	}

	if !verifyResult {
		t.Error("Verify result should be true for valid backup")
	}
}

// TestVerifyBackupCorruptedWithCallback tests OnVerify callback with corrupted backup
func TestVerifyBackupCorruptedWithCallback(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	verifyCalled := false
	verifyResult := true
	mgr.OnVerify = func(backup *Backup, valid bool) {
		verifyCalled = true
		verifyResult = valid
	}

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Corrupt the backup
	if err := os.WriteFile(backup.Destination, []byte("corrupted"), 0644); err != nil {
		t.Fatalf("Failed to corrupt backup: %v", err)
	}

	// Verify should fail and call callback with valid=false
	if err := mgr.verifyBackup(backup); err == nil {
		t.Error("Verification should fail for corrupted backup")
	}

	if !verifyCalled {
		t.Error("OnVerify callback should have been called")
	}

	if verifyResult {
		t.Error("Verify result should be false for corrupted backup")
	}
}

// TestRestoreWithProgressCallback tests restore with progress callback
func TestRestoreWithProgressCallback(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*100) // 100KB for progress updates
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	progressCalled := false
	mgr.OnProgress = func(percent int) {
		progressCalled = true
	}

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	targetPath := filepath.Join(targetDir, "restored.db")
	if err := mgr.Restore(ctx, backup.ID, targetPath); err != nil {
		t.Fatalf("Failed to restore backup: %v", err)
	}

	if !progressCalled {
		t.Error("Progress callback should have been called during restore")
	}

	// Verify restored content
	restoredContent, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read restored file: %v", err)
	}

	if string(restoredContent) != string(content) {
		t.Error("Restored content doesn't match original")
	}
}

// TestRestoreWithCancelledContextDuringCopy tests restore cancellation during copy
func TestRestoreWithCancelledContextDuringCopy(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*1024) // 1MB
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Cancel context before restore
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	targetPath := filepath.Join(targetDir, "restored.db")
	err = mgr.Restore(cancelCtx, backup.ID, targetPath)
	if err == nil {
		t.Error("Expected error for cancelled context during restore")
	}
}

func TestRestoreFailurePreservesExistingTarget(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	backupPath := filepath.Join(config.BackupDir, "corrupt.db.gz")
	file, err := os.Create(backupPath)
	if err != nil {
		t.Fatalf("Failed to create corrupt backup: %v", err)
	}
	gz := gzip.NewWriter(file)
	payload := []byte(strings.Repeat("restored-data-", 4096))
	if _, err := gz.Write(payload); err != nil {
		t.Fatalf("Failed to write gzip payload: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("Failed to close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close gzip file: %v", err)
	}
	raw, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("Failed to read gzip file: %v", err)
	}
	if err := os.WriteFile(backupPath, raw[:len(raw)-4], 0600); err != nil {
		t.Fatalf("Failed to corrupt gzip trailer: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "corrupt",
		Type:        TypeFull,
		Destination: backupPath,
		CompletedAt: time.Now(),
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, 0644); err != nil {
		t.Fatalf("Failed to create existing target: %v", err)
	}

	if err := mgr.Restore(context.Background(), "corrupt", targetPath); err == nil {
		t.Fatal("Expected restore to fail for corrupt gzip backup")
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed restore should preserve existing target, got %q", string(got))
	}
}

// TestRestoreWithWALFilesMissing tests restore when WAL files are missing
func TestRestoreWithWALFilesMissing(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Manually add WAL files to backup metadata (but don't create them)
	backup.WALFiles = []string{"missing_wal.log"}

	targetPath := filepath.Join(targetDir, "restored.db")
	err = mgr.Restore(ctx, backup.ID, targetPath)
	if err == nil {
		t.Error("Expected error when WAL files are missing")
	}
}

// TestRestoreCreatesTargetDirectory tests that restore creates target directory
func TestRestoreCreatesTargetDirectory(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "nested", "target", "dir")

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	targetPath := filepath.Join(targetDir, "restored.db")
	if err := mgr.Restore(ctx, backup.ID, targetPath); err != nil {
		t.Fatalf("Failed to restore backup: %v", err)
	}

	// Verify target directory was created
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		t.Error("Target directory should have been created")
	}

	// Verify restored file exists
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		t.Error("Restored file should exist")
	}
}

// TestCopyFileErrorCases tests copyFile error handling
func TestCopyFileErrorCases(t *testing.T) {
	tempDir := t.TempDir()

	// Test with non-existent source file
	nonExistentSrc := filepath.Join(tempDir, "nonexistent.txt")
	dstFile := filepath.Join(tempDir, "dest.txt")

	err := copyFile(nonExistentSrc, dstFile)
	if err == nil {
		t.Error("Expected error when source file doesn't exist")
	}

	// Test with invalid destination (read-only directory)
	srcFile := filepath.Join(tempDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	nonExistentDir := filepath.Join(tempDir, "nonexistent", "subdir")
	invalidDst := filepath.Join(nonExistentDir, "dest.txt")

	err = copyFile(srcFile, invalidDst)
	if err == nil {
		t.Error("Expected error when destination directory doesn't exist")
	}
}

// TestCopyFileSuccess tests successful copyFile
func TestCopyFileSuccess(t *testing.T) {
	tempDir := t.TempDir()

	srcFile := filepath.Join(tempDir, "source.txt")
	dstFile := filepath.Join(tempDir, "dest.txt")

	content := []byte("test content for file copy")
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	if err := copyFile(srcFile, dstFile); err != nil {
		t.Fatalf("Failed to copy file: %v", err)
	}

	// Verify destination file
	destContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}

	if string(destContent) != string(content) {
		t.Error("Destination content should match source")
	}
}

// TestCopyFileLargeFile tests copyFile with large files
func TestCopyFileLargeFile(t *testing.T) {
	tempDir := t.TempDir()

	srcFile := filepath.Join(tempDir, "source.bin")
	dstFile := filepath.Join(tempDir, "dest.bin")

	// Create 1MB file
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	if err := copyFile(srcFile, dstFile); err != nil {
		t.Fatalf("Failed to copy file: %v", err)
	}

	// Verify destination file
	destContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}

	if string(destContent) != string(content) {
		t.Error("Destination content should match source for large file")
	}
}

// Additional tests to reach 90%+ coverage

// MockDatabaseWithErrors simulates database errors for testing

type MockDatabaseWithErrors struct {
	MockDatabase
	checkpointError  error
	beginBackupError error
	endBackupError   error
}

func (m *MockDatabaseWithErrors) Checkpoint() error {
	if m.checkpointError != nil {
		return m.checkpointError
	}
	return nil
}

func (m *MockDatabaseWithErrors) BeginHotBackup() error {
	if m.beginBackupError != nil {
		return m.beginBackupError
	}
	return nil
}

func (m *MockDatabaseWithErrors) EndHotBackup() error {
	if m.endBackupError != nil {
		return m.endBackupError
	}
	return nil
}

// TestCreateBackupCheckpointError tests CreateBackup when checkpoint fails
func TestCreateBackupCheckpointError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithErrors{
		MockDatabase:    MockDatabase{dbPath: dbFile},
		checkpointError: errors.New("checkpoint failed"),
	}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when checkpoint fails")
	}
}

func TestCreateBackupOnCompleteReceivesError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	db := &MockDatabaseWithErrors{
		MockDatabase:    MockDatabase{dbPath: dbFile},
		checkpointError: errors.New("checkpoint failed"),
	}
	mgr := NewManager(config, db)

	var callbackBackup *Backup
	var callbackErr error
	mgr.OnComplete = func(backup *Backup, err error) {
		callbackBackup = backup
		callbackErr = err
	}

	_, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err == nil {
		t.Fatal("Expected error when checkpoint fails")
	}
	if callbackErr == nil {
		t.Fatal("expected OnComplete error")
	}
	if callbackBackup == nil {
		t.Fatal("expected OnComplete backup context")
	}
	if callbackBackup.ID == "" {
		t.Fatal("expected backup ID in callback")
	}
}

func TestCreateBackupEndHotBackupError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.IncludeWAL = false
	config.Verify = false
	db := &MockDatabaseWithErrors{
		MockDatabase:   MockDatabase{dbPath: dbFile},
		endBackupError: errors.New("end failed"),
	}
	mgr := NewManager(config, db)

	var callbackBackup *Backup
	var callbackErr error
	mgr.OnComplete = func(backup *Backup, err error) {
		callbackBackup = backup
		callbackErr = err
	}

	backup, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err == nil {
		t.Fatal("Expected EndHotBackup error")
	}
	if backup == nil {
		t.Fatal("expected backup result to be returned with end error")
	}
	if callbackBackup == nil || callbackBackup.ID != backup.ID {
		t.Fatal("expected OnComplete backup result")
	}
	if callbackErr == nil {
		t.Fatal("expected OnComplete error")
	}
}

// TestCreateBackupBeginHotBackupError tests CreateBackup when BeginHotBackup fails
func TestCreateBackupBeginHotBackupError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithErrors{
		MockDatabase:     MockDatabase{dbPath: dbFile},
		beginBackupError: errors.New("begin hot backup failed"),
	}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when BeginHotBackup fails")
	}
}

// TestCreateBackupSourceNotFound tests CreateBackup when source database doesn't exist
func TestCreateBackupSourceNotFound(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	// Use non-existent database file
	db := &MockDatabase{dbPath: filepath.Join(tempDir, "nonexistent.db")}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when source database doesn't exist")
	}
}

// TestCreateBackupAlreadyInProgress tests CreateBackup when backup is already in progress
func TestCreateBackupAlreadyInProgress(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Simulate backup in progress
	mgr.mu.Lock()
	mgr.activeBackup = true
	mgr.mu.Unlock()

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when backup already in progress")
	}

	expectedMsg := "backup already in progress"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestCreateBackupDifferential tests creating a differential backup
func TestCreateBackupDifferential(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeDifferential)
	if err != nil {
		t.Fatalf("Failed to create differential backup: %v", err)
	}

	if backup.Type != TypeDifferential {
		t.Errorf("Expected type Differential, got %v", backup.Type)
	}

	if !backup.Incremental {
		t.Error("Incremental flag should be true for differential backup")
	}
}

// TestCreateBackupWithoutVerification tests backup creation with verification disabled
func TestCreateBackupWithoutVerification(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false // Disable verification

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	verifyCalled := false
	mgr.OnVerify = func(backup *Backup, valid bool) {
		verifyCalled = true
	}

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if verifyCalled {
		t.Error("OnVerify should not be called when verification is disabled")
	}

	if backup.Checksum == 0 {
		t.Error("Checksum should still be calculated even without verification")
	}
}

// TestCreateBackupWithoutWAL tests backup creation with WAL disabled
func TestCreateBackupWithoutWAL(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL files
	if err := os.WriteFile(filepath.Join(walDir, "wal_1.log"), []byte("wal content"), 0644); err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.IncludeWAL = false // Disable WAL

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Should have no WAL files since WAL is disabled
	if len(backup.WALFiles) != 0 {
		t.Errorf("Expected 0 WAL files when WAL is disabled, got %d", len(backup.WALFiles))
	}
}

// TestCopyDatabaseReadError tests copyDatabase when read fails
func TestCopyDatabaseReadError(t *testing.T) {
	// This test is tricky because we need to simulate a read error
	// We'll use a directory instead of a file to cause a read error
	tempDir := t.TempDir()

	// Create a directory instead of a file - reading will fail
	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.Mkdir(dbFile, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error when source is a directory")
	}
}

// TestRestoreNonExistentBackupID tests Restore with non-existent backup ID
func TestRestoreNonExistentBackupID(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	ctx := context.Background()
	err := mgr.Restore(ctx, "non_existent_id", "/tmp/restored.db")
	if err == nil {
		t.Error("Expected error for non-existent backup ID")
	}

	expectedMsg := "backup not found: non_existent_id"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestRestoreInvalidTargetPath tests Restore with invalid target path
func TestRestoreInvalidTargetPath(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Try to restore to an invalid path using null character
	invalidTarget := filepath.Join(tempDir, "invalid\x00file.db")
	err = mgr.Restore(ctx, backup.ID, invalidTarget)
	if err == nil {
		t.Error("Expected error for invalid target path")
	}
}

func TestIncrementalBackupRecordsLatestParent(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	full, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}
	time.Sleep(time.Nanosecond)

	incremental, err := mgr.CreateBackup(ctx, TypeIncremental)
	if err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}
	if incremental.ParentID != full.ID {
		t.Fatalf("Expected incremental parent %s, got %s", full.ID, incremental.ParentID)
	}
}

// TestIncrementalBackupOnProgress tests copyDatabaseDelta OnProgress callback.
func TestIncrementalBackupOnProgress(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content for delta"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	if _, err := mgr.CreateBackup(ctx, TypeFull); err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}

	// Modify the database so delta has work to do
	if err := os.WriteFile(dbFile, []byte("modified content here"), 0644); err != nil {
		t.Fatalf("Failed to modify database: %v", err)
	}

	var progressCalled bool
	mgr.OnProgress = func(percent int) {
		progressCalled = true
	}

	if _, err := mgr.CreateBackup(ctx, TypeIncremental); err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}
	if !progressCalled {
		t.Fatal("expected OnProgress to be called")
	}
}

// TestIncrementalBackupWithCompression tests copyDatabaseDelta with valid compression.
func TestIncrementalBackupWithCompression(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = gzip.BestSpeed
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	if _, err := mgr.CreateBackup(ctx, TypeFull); err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}

	// Modify the database
	if err := os.WriteFile(dbFile, []byte("modified content here"), 0644); err != nil {
		t.Fatalf("Failed to modify database: %v", err)
	}

	if _, err := mgr.CreateBackup(ctx, TypeIncremental); err != nil {
		t.Fatalf("Failed to create compressed incremental backup: %v", err)
	}
}

// TestIncrementalBackupInvalidCompression tests copyDatabaseDelta with invalid compression level.
func TestIncrementalBackupInvalidCompression(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 5
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	if _, err := mgr.CreateBackup(ctx, TypeFull); err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}

	// Modify the database
	if err := os.WriteFile(dbFile, []byte("modified content here"), 0644); err != nil {
		t.Fatalf("Failed to modify database: %v", err)
	}

	// Set invalid compression level to trigger gzip.NewWriterLevel error in copyDatabaseDelta
	mgr.config.CompressionLevel = 100
	if _, err := mgr.CreateBackup(ctx, TypeIncremental); err == nil {
		t.Fatal("expected error for invalid compression level in incremental backup")
	}
}

// TestIncrementalBackupCancelledContext tests copyDatabaseDelta context cancellation.
func TestIncrementalBackupCancelledContext(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*1024) // 1MB
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	if _, err := mgr.CreateBackup(ctx, TypeFull); err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}

	// Modify the database
	if err := os.WriteFile(dbFile, []byte("modified content here"), 0644); err != nil {
		t.Fatalf("Failed to modify database: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := mgr.CreateBackup(cancelCtx, TypeIncremental); err == nil {
		t.Fatal("expected error for cancelled context in incremental backup")
	}
}

func TestDifferentialBackupRecordsLatestFullParent(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)
	ctx := context.Background()

	full, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}
	time.Sleep(time.Nanosecond)
	if _, err := mgr.CreateBackup(ctx, TypeIncremental); err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}
	time.Sleep(time.Nanosecond)

	differential, err := mgr.CreateBackup(ctx, TypeDifferential)
	if err != nil {
		t.Fatalf("Failed to create differential backup: %v", err)
	}
	if differential.ParentID != full.ID {
		t.Fatalf("Expected differential parent %s, got %s", full.ID, differential.ParentID)
	}
}

func TestRestoreIncrementalMissingParentFails(t *testing.T) {
	tempDir := t.TempDir()
	backupPath := filepath.Join(tempDir, "incremental.db")
	if err := os.WriteFile(backupPath, []byte("incremental content"), 0644); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = tempDir

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "inc",
		Type:        TypeIncremental,
		Destination: backupPath,
		ParentID:    "missing",
		CompletedAt: time.Now(),
	})

	err := mgr.Restore(context.Background(), "inc", filepath.Join(tempDir, "restored.db"))
	if err == nil {
		t.Fatal("Expected missing parent error")
	}
	if !strings.Contains(err.Error(), "parent not found") {
		t.Fatalf("Expected parent not found error, got %v", err)
	}
}

func TestRestoreDifferentialInvalidParentTypeFails(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.db")
	diffPath := filepath.Join(tempDir, "diff.db")
	if err := os.WriteFile(parentPath, []byte("parent content"), 0644); err != nil {
		t.Fatalf("Failed to create parent file: %v", err)
	}
	if err := os.WriteFile(diffPath, []byte("diff content"), 0644); err != nil {
		t.Fatalf("Failed to create diff file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = tempDir

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "inc-parent", Type: TypeIncremental, Destination: parentPath, ParentID: "full", CompletedAt: time.Now()},
		&Backup{ID: "diff", Type: TypeDifferential, Destination: diffPath, ParentID: "inc-parent", CompletedAt: time.Now()},
	)

	err := mgr.Restore(context.Background(), "diff", filepath.Join(tempDir, "restored.db"))
	if err == nil {
		t.Fatal("Expected invalid differential parent error")
	}
	if !strings.Contains(err.Error(), "requires full parent") {
		t.Fatalf("Expected differential parent error, got %v", err)
	}
}

func TestRestoreBackupChainCycleFails(t *testing.T) {
	tempDir := t.TempDir()
	backupPath := filepath.Join(tempDir, "backup.db")
	if err := os.WriteFile(backupPath, []byte("backup content"), 0644); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = tempDir

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "a", Type: TypeIncremental, Destination: backupPath, ParentID: "b", CompletedAt: time.Now()},
		&Backup{ID: "b", Type: TypeIncremental, Destination: backupPath, ParentID: "a", CompletedAt: time.Now()},
	)

	err := mgr.Restore(context.Background(), "a", filepath.Join(tempDir, "restored.db"))
	if err == nil {
		t.Fatal("Expected cycle error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Expected cycle error, got %v", err)
	}
}

func TestRestoreIncrementalAppliesDeltaChain(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "source.db")
	if err := os.WriteFile(dbFile, []byte("first version of database"), 0644); err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = true

	mgr := NewManager(config, &MockDatabase{dbPath: dbFile})
	ctx := context.Background()

	full, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}
	if err := os.WriteFile(dbFile, []byte("second version with changed bytes"), 0644); err != nil {
		t.Fatalf("Failed to update source database: %v", err)
	}

	incremental, err := mgr.CreateBackup(ctx, TypeIncremental)
	if err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}
	if incremental.ParentID != full.ID {
		t.Fatalf("Expected incremental parent %s, got %s", full.ID, incremental.ParentID)
	}

	payload, err := os.ReadFile(incremental.Destination)
	if err != nil {
		t.Fatalf("Failed to read incremental payload: %v", err)
	}
	if !strings.HasPrefix(string(payload), deltaMagic) {
		t.Fatalf("Expected incremental payload to use delta format")
	}

	restorePath := filepath.Join(tempDir, "restored.db")
	if err := mgr.Restore(ctx, incremental.ID, restorePath); err != nil {
		t.Fatalf("Restore incremental failed: %v", err)
	}
	restored, err := os.ReadFile(restorePath)
	if err != nil {
		t.Fatalf("Failed to read restored database: %v", err)
	}
	if string(restored) != "second version with changed bytes" {
		t.Fatalf("Unexpected restored content: %q", string(restored))
	}
}

func TestRestoreDifferentialAppliesFullParentDelta(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "source.db")
	if err := os.WriteFile(dbFile, []byte("full database state"), 0644); err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = true

	mgr := NewManager(config, &MockDatabase{dbPath: dbFile})
	ctx := context.Background()

	full, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}
	if err := os.WriteFile(dbFile, []byte("intermediate incremental state"), 0644); err != nil {
		t.Fatalf("Failed to write incremental state: %v", err)
	}
	if _, err := mgr.CreateBackup(ctx, TypeIncremental); err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}
	if err := os.WriteFile(dbFile, []byte("differential target state"), 0644); err != nil {
		t.Fatalf("Failed to write differential state: %v", err)
	}

	differential, err := mgr.CreateBackup(ctx, TypeDifferential)
	if err != nil {
		t.Fatalf("Failed to create differential backup: %v", err)
	}
	if differential.ParentID != full.ID {
		t.Fatalf("Expected differential parent %s, got %s", full.ID, differential.ParentID)
	}

	restorePath := filepath.Join(tempDir, "restored-diff.db")
	if err := mgr.Restore(ctx, differential.ID, restorePath); err != nil {
		t.Fatalf("Restore differential failed: %v", err)
	}
	restored, err := os.ReadFile(restorePath)
	if err != nil {
		t.Fatalf("Failed to read restored differential database: %v", err)
	}
	if string(restored) != "differential target state" {
		t.Fatalf("Unexpected restored content: %q", string(restored))
	}
}

func TestRestoreLegacyIncrementalSnapshotStillWorks(t *testing.T) {
	tempDir := t.TempDir()
	fullPath := filepath.Join(tempDir, "full.db")
	legacyPath := filepath.Join(tempDir, "legacy-incremental.db")
	if err := os.WriteFile(fullPath, []byte("old state"), 0644); err != nil {
		t.Fatalf("Failed to create full backup file: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte("legacy snapshot state"), 0644); err != nil {
		t.Fatalf("Failed to create legacy incremental file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = tempDir

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "full", Type: TypeFull, Destination: fullPath, CompletedAt: time.Now()},
		&Backup{ID: "legacy", Type: TypeIncremental, Destination: legacyPath, ParentID: "full", CompletedAt: time.Now()},
	)

	restorePath := filepath.Join(tempDir, "restored-legacy.db")
	if err := mgr.Restore(context.Background(), "legacy", restorePath); err != nil {
		t.Fatalf("Restore legacy incremental failed: %v", err)
	}
	restored, err := os.ReadFile(restorePath)
	if err != nil {
		t.Fatalf("Failed to read restored legacy database: %v", err)
	}
	if string(restored) != "legacy snapshot state" {
		t.Fatalf("Unexpected restored legacy content: %q", string(restored))
	}
}

func TestBackupMetadataPersistsAcrossManagers(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	backup, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	reloaded := NewManager(config, db)
	loadedBackup := reloaded.GetBackup(backup.ID)
	if loadedBackup == nil {
		t.Fatalf("Expected backup %s to load from metadata", backup.ID)
	}
	if loadedBackup.Destination != backup.Destination {
		t.Fatalf("Expected destination %s, got %s", backup.Destination, loadedBackup.Destination)
	}
}

func TestDeleteBackupPersistsMetadata(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = false

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	backup, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}
	if err := mgr.DeleteBackup(backup.ID); err != nil {
		t.Fatalf("DeleteBackup failed: %v", err)
	}

	reloaded := NewManager(config, db)
	if got := reloaded.GetBackup(backup.ID); got != nil {
		t.Fatalf("Expected deleted backup to be absent after reload, got %+v", got)
	}
}

func TestCleanupOldBackupsPersistsMetadata(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.MaxBackups = 1
	config.RetentionPeriod = 0
	if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	db := &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")}
	mgr := NewManager(config, db)

	now := time.Now()
	oldPath := filepath.Join(config.BackupDir, "old.db")
	newPath := filepath.Join(config.BackupDir, "new.db")
	if err := os.WriteFile(oldPath, []byte("old"), 0644); err != nil {
		t.Fatalf("Failed to create old backup file: %v", err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0644); err != nil {
		t.Fatalf("Failed to create new backup file: %v", err)
	}

	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "old", Type: TypeFull, Destination: oldPath, CompletedAt: now.Add(-time.Hour)},
		&Backup{ID: "new", Type: TypeFull, Destination: newPath, CompletedAt: now},
	)

	if err := mgr.saveMetadataLocked(); err != nil {
		t.Fatalf("saveMetadataLocked failed: %v", err)
	}
	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("cleanupOldBackups failed: %v", err)
	}

	reloaded := NewManager(config, db)
	if reloaded.GetBackup("old") != nil {
		t.Fatal("Expected old backup metadata to be removed after reload")
	}
	if reloaded.GetBackup("new") == nil {
		t.Fatal("Expected new backup metadata to remain after reload")
	}
}

func TestCleanupOldBackupsRemoveErrorKeepsMetadata(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.MaxBackups = 1
	config.RetentionPeriod = 0
	if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	oldPath := filepath.Join(config.BackupDir, "old.db")
	if err := os.MkdirAll(filepath.Join(oldPath, "nested"), 0755); err != nil {
		t.Fatalf("Failed to create undeletable backup path: %v", err)
	}
	newPath := filepath.Join(config.BackupDir, "new.db")
	if err := os.WriteFile(newPath, []byte("new"), 0644); err != nil {
		t.Fatalf("Failed to create new backup file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	now := time.Now()
	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "old", Type: TypeFull, Destination: oldPath, CompletedAt: now.Add(-time.Hour)},
		&Backup{ID: "new", Type: TypeFull, Destination: newPath, CompletedAt: now},
	)

	if err := mgr.cleanupOldBackups(); err == nil {
		t.Fatal("cleanupOldBackups should fail when an old backup file cannot be removed")
	}
	if mgr.GetBackup("old") == nil {
		t.Fatal("old backup metadata should remain after failed removal")
	}
}

func TestCreateBackupRecordsCleanupError(t *testing.T) {
	tempDir := t.TempDir()

	dbPath := filepath.Join(tempDir, "source.db")
	if err := os.WriteFile(dbPath, []byte("source database"), 0644); err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.MaxBackups = 1
	config.RetentionPeriod = 0
	config.CompressionLevel = 0
	config.IncludeWAL = false
	config.Verify = false
	if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	oldPath := filepath.Join(config.BackupDir, "old.db")
	if err := os.MkdirAll(filepath.Join(oldPath, "nested"), 0755); err != nil {
		t.Fatalf("Failed to create undeletable backup path: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: dbPath})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "old",
		Type:        TypeFull,
		Destination: oldPath,
		CompletedAt: time.Now().Add(-time.Hour),
	})

	backup, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err != nil {
		t.Fatalf("CreateBackup should keep the completed backup when retention cleanup fails: %v", err)
	}
	if backup == nil {
		t.Fatal("CreateBackup returned nil backup")
	}
	if mgr.LastCleanupError() == nil {
		t.Fatal("Expected retention cleanup failure to be recorded")
	}
	if mgr.GetBackup("old") == nil {
		t.Fatal("old backup metadata should remain when cleanup fails")
	}
}

type errWriter struct {
	errAfter int
	written  int
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.written >= e.errAfter {
		return 0, errors.New("mock write error")
	}
	e.written += len(p)
	return len(p), nil
}

// TestWriteDeltaRecordErrors tests error paths in writeDeltaRecord
func TestWriteDeltaRecordErrors(t *testing.T) {
	tests := []struct {
		name     string
		errAfter int
		wantErr  string
	}{
		{"offset write fails", 0, "failed to write delta offset"},
		{"length write fails", 8, "failed to write delta length"},
		{"data write fails", 12, "failed to write delta data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &errWriter{errAfter: tt.errAfter}
			err := writeDeltaRecord(w, 42, []byte("hello"))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

// TestSaveMetadataLockedNoDir tests saveMetadataLocked when backup dir does not exist
func TestSaveMetadataLockedNoDir(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "nonexistent")
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	if err := mgr.saveMetadataLocked(); err != nil {
		t.Fatalf("saveMetadataLocked should return nil when dir missing: %v", err)
	}
}

// TestSaveMetadataLockedCreateError tests saveMetadataLocked when metadata file cannot be created
func TestSaveMetadataLockedCreateError(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = backupDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Make backup dir read-only so os.Create fails
	if err := os.Chmod(backupDir, 0555); err != nil {
		t.Fatalf("Failed to chmod backup dir: %v", err)
	}
	defer os.Chmod(backupDir, 0755)

	if err := mgr.saveMetadataLocked(); err == nil {
		t.Fatal("saveMetadataLocked should error when file cannot be created")
	}
}

// TestSaveMetadataLockedRenameError tests saveMetadataLocked when rename fails
func TestSaveMetadataLockedRenameError(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Create metadata path as a directory so os.Rename fails
	metaPath := mgr.metadataPath()
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}

	if err := mgr.saveMetadataLocked(); err == nil {
		t.Fatal("saveMetadataLocked should error when rename fails")
	}
}

func TestSyncParentDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.json")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := syncParentDir(path); err != nil {
		t.Fatalf("syncParentDir failed: %v", err)
	}
	if err := syncParentDir(filepath.Join(t.TempDir(), "missing", "metadata.json")); err == nil {
		t.Fatal("syncParentDir should fail for missing parent directory")
	}
}

// TestCopyDatabaseDeltaNoParent tests copyDatabaseDelta when parent backup is missing
func TestCopyDatabaseDeltaNoParent(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	backup := &Backup{ID: "child", Type: TypeIncremental, ParentID: "missing", Destination: filepath.Join(tempDir, "child.db")}
	ctx := context.Background()
	if err := mgr.copyDatabaseDelta(ctx, backup); err == nil {
		t.Fatal("expected error when parent backup is missing")
	}
}

// TestCopyDatabaseDeltaMaterializeError tests copyDatabaseDelta when materializeBackup fails
func TestCopyDatabaseDeltaMaterializeError(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Add parent backup without a file
	parent := &Backup{ID: "parent", Type: TypeFull, Destination: filepath.Join(tempDir, "parent.db")}
	mgr.metadata.Backups = append(mgr.metadata.Backups, parent)

	child := &Backup{ID: "child", Type: TypeIncremental, ParentID: "parent", Destination: filepath.Join(tempDir, "child.db")}
	ctx := context.Background()
	if err := mgr.copyDatabaseDelta(ctx, child); err == nil {
		t.Fatal("expected error when parent file is missing")
	}
}

// TestCreateBackupMkdirError tests CreateBackup when MkdirAll fails
func TestCreateBackupMkdirError(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = "/dev/null/invalid"
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	ctx := context.Background()
	if _, err := mgr.CreateBackup(ctx, TypeFull); err == nil {
		t.Fatal("expected error when backup dir cannot be created")
	}
}

// TestCopyDatabaseCompressionError tests copyDatabase with invalid compression level
func TestCopyDatabaseCompressionError(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	config.CompressionLevel = 10 // invalid
	dbPath := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbPath, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	db := &MockDatabase{dbPath: dbPath}
	mgr := NewManager(config, db)

	backup := &Backup{ID: "b1", Destination: filepath.Join(tempDir, "b1.db")}
	ctx := context.Background()
	if err := mgr.copyDatabase(ctx, backup); err == nil {
		t.Fatal("expected error for invalid compression level")
	}
	assertPathDoesNotExist(t, backup.Destination)
	assertNoBackupTempFiles(t, tempDir)
}

// TestCopyDatabaseDeltaContextCancel tests copyDatabaseDelta with cancelled context
func TestCopyDatabaseDeltaContextCancel(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir

	dbPath := filepath.Join(tempDir, "test.db")
	parentPath := filepath.Join(tempDir, "parent.db")
	if err := os.WriteFile(dbPath, []byte("src data"), 0644); err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	if err := os.WriteFile(parentPath, []byte("parent data"), 0644); err != nil {
		t.Fatalf("failed to create parent db: %v", err)
	}

	db := &MockDatabase{dbPath: dbPath}
	mgr := NewManager(config, db)

	parent := &Backup{ID: "parent", Type: TypeFull, Destination: parentPath}
	mgr.metadata.Backups = append(mgr.metadata.Backups, parent)

	child := &Backup{ID: "child", Type: TypeIncremental, ParentID: "parent", Destination: filepath.Join(tempDir, "child.db")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mgr.copyDatabaseDelta(ctx, child); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context canceled error, got %v", err)
	}
	assertPathDoesNotExist(t, child.Destination)
	assertNoBackupTempFiles(t, tempDir)
}

func assertPathDoesNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist, got err=%v", path, err)
	}
}

func assertNoBackupTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read %s: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("found leftover backup temp file %s in %s", entry.Name(), dir)
		}
	}
}

// TestBuildRestoreChainErrors tests error paths in buildRestoreChain
func TestBuildRestoreChainErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Nil backup
	if _, err := mgr.buildRestoreChain(nil); err == nil {
		t.Fatal("expected error for nil backup")
	}

	// Missing parent in chain
	child := &Backup{ID: "child", Type: TypeIncremental, ParentID: "missing"}
	mgr.metadata.Backups = append(mgr.metadata.Backups, child)
	if _, err := mgr.buildRestoreChain(child); err == nil {
		t.Fatal("expected error when parent missing in chain")
	}
}

// TestLoadMetadataErrors tests error paths in loadMetadata
func TestLoadMetadataErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Make metadata path a directory so Open fails
	metaPath := mgr.metadataPath()
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}
	if err := mgr.loadMetadata(); err == nil {
		t.Fatal("expected error when metadata path is directory")
	}
	os.RemoveAll(metaPath)

	// Invalid JSON in metadata file
	if err := os.WriteFile(metaPath, []byte("not json"), 0644); err != nil {
		t.Fatalf("failed to write bad metadata: %v", err)
	}
	if err := mgr.loadMetadata(); err == nil {
		t.Fatal("expected error for invalid metadata JSON")
	}
}

func TestNewManagerPreservesMetadataLoadError(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	metadataPath := filepath.Join(backupDir, metadataFileName)
	if err := os.WriteFile(metadataPath, []byte("not json"), backupFilePerm); err != nil {
		t.Fatalf("WriteFile metadata failed: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbPath, []byte("database"), 0644); err != nil {
		t.Fatalf("WriteFile db failed: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = backupDir
	config.CompressionLevel = 0
	mgr := NewManager(config, &MockDatabase{dbPath: dbPath})
	if mgr.MetadataError() == nil {
		t.Fatal("expected metadata load error")
	}
	if _, err := mgr.CreateBackup(context.Background(), TypeFull); err == nil {
		t.Fatal("CreateBackup should fail when metadata failed to load")
	}

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatalf("ReadFile metadata failed: %v", err)
	}
	if string(data) != "not json" {
		t.Fatalf("metadata was overwritten: %q", data)
	}
}

// TestCompoundReadCloserError tests Close when a closer returns an error
func TestCompoundReadCloserError(t *testing.T) {
	firstErr := errors.New("first close error")
	secondErr := errors.New("second close error")
	crc := &compoundReadCloser{
		Reader: strings.NewReader("data"),
		closers: []io.Closer{
			&MockReadCloser{errOnClose: firstErr},
			&MockReadCloser{errOnClose: secondErr},
		},
	}
	err := crc.Close()
	if !errors.Is(err, firstErr) {
		t.Fatalf("expected first close error, got %v", err)
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("expected second close error, got %v", err)
	}
}

type MockReadCloser struct {
	errOnClose error
}

func (m *MockReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (m *MockReadCloser) Close() error               { return m.errOnClose }

// TestMaterializeBackupErrors tests error paths in materializeBackup
func TestMaterializeBackupErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	ctx := context.Background()

	// Nil backup
	if err := mgr.materializeBackup(ctx, nil, filepath.Join(tempDir, "out.db")); err == nil {
		t.Fatal("expected error for nil backup")
	}

	// Backup with missing file
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{ID: "bad", Destination: filepath.Join(tempDir, "missing")})
	if err := mgr.materializeBackup(ctx, mgr.metadata.Backups[0], filepath.Join(tempDir, "out.db")); err == nil {
		t.Fatal("expected error for missing backup file")
	}
}

// TestOpenBackupReaderErrors tests error paths in openBackupReader
func TestOpenBackupReaderErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Non-existent file
	_, err := mgr.openBackupReader(&Backup{Destination: filepath.Join(tempDir, "missing")})
	if err == nil {
		t.Fatal("expected error for missing file")
	}

	// Invalid gzip file
	badGz := filepath.Join(tempDir, "bad.gz")
	if err := os.WriteFile(badGz, []byte("not gz"), 0644); err != nil {
		t.Fatalf("failed to write bad.gz: %v", err)
	}
	_, err = mgr.openBackupReader(&Backup{Destination: badGz})
	if err == nil {
		t.Fatal("expected error for invalid gzip")
	}
}

// TestApplyDeltaPayloadErrors tests error paths in applyDeltaPayload
func TestApplyDeltaPayloadErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	targetPath := filepath.Join(tempDir, "target.db")
	if err := os.WriteFile(targetPath, make([]byte, 1024), 0644); err != nil {
		t.Fatalf("failed to create target: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"invalid json header", "not json\n", "failed to decode delta header"},
		{"invalid chunk size", `{"chunk_size":0,"target_size":100}` + "\n", "invalid delta header"},
		{"short offset", `{"chunk_size":1024,"target_size":100}` + "\nABCD", "failed to read delta offset"},
		{"length exceeds chunk", `{"chunk_size":2,"target_size":100}` + "\n\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00", "delta record length"},
		{"short data", `{"chunk_size":1024,"target_size":100}` + "\n\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00AB", "failed to read delta data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.applyDeltaPayload(ctx, strings.NewReader(tt.input), targetPath)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}

	// Context cancellation
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	validHeader := `{"chunk_size":1024,"target_size":100}` + "\n"
	err := mgr.applyDeltaPayload(cancelledCtx, strings.NewReader(validHeader), targetPath)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestBuildRestoreChainMoreErrors tests additional error paths in buildRestoreChain.
func TestBuildRestoreChainMoreErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	backupPath := filepath.Join(tempDir, "backup.db")
	if err := os.WriteFile(backupPath, []byte("backup"), 0644); err != nil {
		t.Fatalf("failed to create backup file: %v", err)
	}

	// Full backup with parent
	mgr.metadata.Backups = []*Backup{
		{ID: "full", Type: TypeFull, Destination: backupPath, ParentID: "parent"},
	}
	if _, err := mgr.buildRestoreChain(mgr.metadata.Backups[0]); err == nil {
		t.Fatal("expected error for full backup with parent")
	}

	// Unknown backup type (must have a parent to enter the switch)
	parentPath2 := filepath.Join(tempDir, "parent2.db")
	if err := os.WriteFile(parentPath2, []byte("parent2"), 0644); err != nil {
		t.Fatalf("failed to create parent file: %v", err)
	}
	mgr.metadata.Backups = []*Backup{
		{ID: "parent2", Type: TypeFull, Destination: parentPath2},
		{ID: "unknown", Type: Type(99), Destination: backupPath, ParentID: "parent2"},
	}
	if _, err := mgr.buildRestoreChain(mgr.metadata.Backups[1]); err == nil {
		t.Fatal("expected error for unknown backup type")
	}

	// Incremental with differential parent
	parentPath := filepath.Join(tempDir, "parent.db")
	if err := os.WriteFile(parentPath, []byte("parent"), 0644); err != nil {
		t.Fatalf("failed to create parent file: %v", err)
	}
	mgr.metadata.Backups = []*Backup{
		{ID: "parent", Type: TypeDifferential, Destination: parentPath},
		{ID: "inc", Type: TypeIncremental, Destination: backupPath, ParentID: "parent"},
	}
	if _, err := mgr.buildRestoreChain(mgr.metadata.Backups[1]); err == nil {
		t.Fatal("expected error for incremental with differential parent")
	}

	// Parent file not found
	mgr.metadata.Backups = []*Backup{
		{ID: "parent2", Type: TypeFull, Destination: filepath.Join(tempDir, "nonexistent.db")},
		{ID: "inc2", Type: TypeIncremental, Destination: backupPath, ParentID: "parent2"},
	}
	if _, err := mgr.buildRestoreChain(mgr.metadata.Backups[1]); err == nil {
		t.Fatal("expected error when parent file not found")
	}
}

// TestFindParentBackupIDZeroTime tests findParentBackupID skips backups with zero CompletedAt.
func TestFindParentBackupIDZeroTime(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	backupPath := filepath.Join(tempDir, "backup.db")
	if err := os.WriteFile(backupPath, []byte("backup"), 0644); err != nil {
		t.Fatalf("failed to create backup file: %v", err)
	}

	mgr.metadata.Backups = []*Backup{
		{ID: "zero", Type: TypeFull, Destination: backupPath, CompletedAt: time.Time{}},
	}
	if id := mgr.findParentBackupID(TypeIncremental); id != "" {
		t.Fatalf("expected empty parent ID, got %s", id)
	}
}

// TestSaveMetadataLockedStatError tests saveMetadataLocked when os.Stat returns a non-IsNotExist error.
func TestSaveMetadataLockedStatError(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "circular")

	// Create a self-referential symlink so os.Stat returns a symlink loop error
	if err := os.Symlink(config.BackupDir, config.BackupDir); err != nil {
		t.Fatalf("failed to create circular symlink: %v", err)
	}

	db := &MockDatabase{}
	mgr := NewManager(config, db)

	if err := mgr.saveMetadataLocked(); err == nil {
		t.Fatal("expected error for circular symlink backup dir")
	}
}

// TestDeleteBackupRemoveError tests DeleteBackup when os.Remove returns a non-IsNotExist error.
func TestDeleteBackupRemoveError(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	db := &MockDatabase{}
	mgr := NewManager(config, db)

	// Create a non-empty directory to act as the backup destination
	backupDir := filepath.Join(tempDir, "backup_dir")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		t.Fatalf("failed to create backup dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "file.txt"), []byte("x"), 0644); err != nil {
		t.Fatalf("failed to create file in backup dir: %v", err)
	}

	mgr.metadata.Backups = []*Backup{
		{ID: "del1", Type: TypeFull, Destination: backupDir, CompletedAt: time.Now()},
	}

	if err := mgr.DeleteBackup("del1"); err == nil {
		t.Fatal("expected error when removing non-empty directory")
	}
}
