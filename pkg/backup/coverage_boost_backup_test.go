package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
