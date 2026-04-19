package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// MockDatabaseWithWAL implements Database interface with WAL
 type MockDatabaseWithWAL struct {
	dbPath  string
	walPath string
	lsn     uint64
}

func (m *MockDatabaseWithWAL) GetDatabasePath() string {
	return m.dbPath
}

func (m *MockDatabaseWithWAL) GetWALPath() string {
	return m.walPath
}

func (m *MockDatabaseWithWAL) Checkpoint() error {
	return nil
}

func (m *MockDatabaseWithWAL) BeginHotBackup() error {
	return nil
}

func (m *MockDatabaseWithWAL) EndHotBackup() error {
	return nil
}

func (m *MockDatabaseWithWAL) GetCurrentLSN() uint64 {
	return m.lsn
}

// TestCreateBackupWithCompression tests backup with compression enabled
func TestCreateBackupWithCompression(t *testing.T) {
	tempDir := t.TempDir()

	// Create mock database file
	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*10) // 10KB of data
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 6 // Enable compression

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if backup.Type != TypeFull {
		t.Errorf("Expected type Full, got %v", backup.Type)
	}

	if backup.Size == 0 {
		t.Error("Backup size should not be zero")
	}

	// Verify backup file exists with .gz extension
	if _, err := os.Stat(backup.Destination); os.IsNotExist(err) {
		t.Error("Backup file should exist")
	}
}

// TestCreateIncrementalBackup tests incremental backup
func TestCreateIncrementalBackup(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile, lsn: 100}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeIncremental)
	if err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}

	if backup.Type != TypeIncremental {
		t.Errorf("Expected type Incremental, got %v", backup.Type)
	}

	if !backup.Incremental {
		t.Error("Incremental flag should be true")
	}
}

// TestBackupCallbacks tests progress and completion callbacks
func TestBackupCallbacks(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*100) // 100KB for progress updates
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	progressCalled := false
	completeCalled := false

	mgr.OnProgress = func(percent int) {
		progressCalled = true
	}

	mgr.OnComplete = func(backup *Backup, err error) {
		completeCalled = true
		if err != nil {
			t.Errorf("Unexpected error in completion callback: %v", err)
		}
	}

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if !progressCalled {
		t.Error("Progress callback should have been called")
	}

	if !completeCalled {
		t.Error("Completion callback should have been called")
	}
}

// TestBackupWithWAL tests backup with WAL files
func TestBackupWithWAL(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(walDir, "wal_2.log"), []byte("wal content 2"), 0644); err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir, lsn: 200}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if len(backup.WALFiles) != 2 {
		t.Errorf("Expected 2 WAL files, got %d", len(backup.WALFiles))
	}

	// Verify WAL backup directory exists
	walBackupDir := filepath.Join(config.BackupDir, backup.ID+"_wal")
	if _, err := os.Stat(walBackupDir); os.IsNotExist(err) {
		t.Error("WAL backup directory should exist")
	}
}

// TestBackupWithCancelledContext tests backup cancellation
func TestBackupWithCancelledContext(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*1024*10) // 10MB to ensure cancellation has time
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

// TestVerifyBackup tests backup verification
func TestVerifyBackup(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := []byte("test database content for verification")
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0 // Disable compression for this test

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify should pass for valid backup
	if err := mgr.verifyBackup(backup); err != nil {
		t.Errorf("Verification failed for valid backup: %v", err)
	}
}

// TestVerifyBackupCorrupted tests verification of corrupted backup
func TestVerifyBackupCorrupted(t *testing.T) {
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

	// Corrupt the backup file
	if err := os.WriteFile(backup.Destination, []byte("corrupted data"), 0644); err != nil {
		t.Fatalf("Failed to corrupt backup: %v", err)
	}

	// Verify should fail for corrupted backup
	if err := mgr.verifyBackup(backup); err == nil {
		t.Error("Verification should fail for corrupted backup")
	}
}

// TestRestore tests backup restoration
func TestRestore(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	originalContent := []byte("original database content")
	if err := os.WriteFile(dbFile, originalContent, 0644); err != nil {
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

	// Verify restored content
	restoredContent, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read restored file: %v", err)
	}

	if string(restoredContent) != string(originalContent) {
		t.Error("Restored content doesn't match original")
	}
}

// TestRestoreWithCompression tests restoration of compressed backup
func TestRestoreWithCompression(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	originalContent := make([]byte, 1024*100)
	for i := range originalContent {
		originalContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(dbFile, originalContent, 0644); err != nil {
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

	targetPath := filepath.Join(targetDir, "restored.db")
	if err := mgr.Restore(ctx, backup.ID, targetPath); err != nil {
		t.Fatalf("Failed to restore compressed backup: %v", err)
	}

	// Verify restored content
	restoredContent, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read restored file: %v", err)
	}

	if string(restoredContent) != string(originalContent) {
		t.Error("Restored content doesn't match original")
	}
}

// TestRestoreNonExistentBackup tests restoring non-existent backup
func TestRestoreNonExistentBackup(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	ctx := context.Background()
	err := mgr.Restore(ctx, "non_existent_backup", "/tmp/restored.db")
	if err == nil {
		t.Error("Expected error for non-existent backup")
	}
}

// TestCleanupOldBackupsByRetention tests retention-based cleanup
func TestCleanupOldBackupsByRetention(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.RetentionPeriod = 1 * time.Hour // 1 hour retention

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add old backup
	oldBackup := &Backup{
		ID:          "old_backup",
		Destination: filepath.Join(config.BackupDir, "old_backup.db"),
		CompletedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
	}
	os.WriteFile(oldBackup.Destination, []byte("old data"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, oldBackup)

	// Add recent backup
	recentBackup := &Backup{
		ID:          "recent_backup",
		Destination: filepath.Join(config.BackupDir, "recent_backup.db"),
		CompletedAt: time.Now(),
	}
	os.WriteFile(recentBackup.Destination, []byte("recent data"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, recentBackup)

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Old backup should be removed
	if len(mgr.metadata.Backups) != 1 {
		t.Errorf("Expected 1 backup after retention cleanup, got %d", len(mgr.metadata.Backups))
	}

	if mgr.metadata.Backups[0].ID != "recent_backup" {
		t.Error("Recent backup should remain")
	}
}


// TestDeleteBackupNotFound tests deleting non-existent backup
func TestDeleteBackupNotFound(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	err := mgr.DeleteBackup("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent backup")
	}
}

// TestCreateBackupWithCancelledContextImmediate tests immediate cancellation
func TestCreateBackupWithCancelledContextImmediate(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*1024) // 1MB
	if err := os.WriteFile(dbFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

// TestListBackupsAndGetBackup tests listing and retrieving backups
func TestListBackupsAndGetBackup(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Initially should have no backups
	backups := mgr.ListBackups()
	if len(backups) != 0 {
		t.Errorf("Expected 0 backups initially, got %d", len(backups))
	}

	// Create a backup
	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// List should now have one backup
	backups = mgr.ListBackups()
	if len(backups) != 1 {
		t.Errorf("Expected 1 backup, got %d", len(backups))
	}

	// GetBackup should return the backup
	retrieved := mgr.GetBackup(backup.ID)
	if retrieved == nil {
		t.Fatal("GetBackup returned nil for existing backup")
	}
	if retrieved.ID != backup.ID {
		t.Errorf("GetBackup returned wrong backup: %s vs %s", retrieved.ID, backup.ID)
	}

	// GetBackup for non-existent should return nil
	notFound := mgr.GetBackup("non_existent")
	if notFound != nil {
		t.Error("GetBackup should return nil for non-existent backup")
	}
}

// TestRestoreWithWAL tests restore including WAL files
func TestRestoreWithWAL(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()
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

	targetPath := filepath.Join(targetDir, "restored.db")
	if err := mgr.Restore(ctx, backup.ID, targetPath); err != nil {
		t.Fatalf("Failed to restore backup with WAL: %v", err)
	}

	// Verify WAL files were restored
	// Engine convention: WAL directory is at <db-path>.wal
	targetWALPath := filepath.Join(targetPath+".wal", "wal_1.log")
	if _, err := os.Stat(targetWALPath); os.IsNotExist(err) {
		t.Error("WAL file should have been restored")
	}
}

// TestRestoreWithCancelledContext tests restore cancellation
func TestRestoreWithCancelledContext(t *testing.T) {
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

// TestRestoreMissingBackupFile tests restore when backup file is missing
func TestRestoreMissingBackupFile(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Manually add backup metadata without file
	backup := &Backup{
		ID:          "test_backup",
		Destination: filepath.Join(tempDir, "nonexistent.db"),
		CompletedAt: time.Now(),
	}
	mgr.metadata.Backups = append(mgr.metadata.Backups, backup)

	ctx := context.Background()
	err := mgr.Restore(ctx, backup.ID, filepath.Join(tempDir, "restored.db"))
	if err == nil {
		t.Error("Expected error when backup file is missing")
	}
}

// TestDeleteBackupSuccess tests successful backup deletion
func TestDeleteBackupSuccess(t *testing.T) {
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

	// Verify backup file exists
	if _, err := os.Stat(backup.Destination); os.IsNotExist(err) {
		t.Fatal("Backup file should exist before deletion")
	}

	// Delete the backup
	if err := mgr.DeleteBackup(backup.ID); err != nil {
		t.Fatalf("Failed to delete backup: %v", err)
	}

	// Verify backup file is removed
	if _, err := os.Stat(backup.Destination); !os.IsNotExist(err) {
		t.Error("Backup file should be removed after deletion")
	}

	// Verify backup is removed from metadata
	if mgr.GetBackup(backup.ID) != nil {
		t.Error("Backup should be removed from metadata")
	}
}

