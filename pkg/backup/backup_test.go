package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// MockDatabase implements Database interface for testing
type MockDatabase struct {
	dbPath string
	walPath string
	lsn    uint64
}

func (m *MockDatabase) GetDatabasePath() string {
	return m.dbPath
}

func (m *MockDatabase) GetWALPath() string {
	return m.walPath
}

func (m *MockDatabase) Checkpoint() error {
	return nil
}

func (m *MockDatabase) BeginHotBackup() error {
	return nil
}

func (m *MockDatabase) EndHotBackup() error {
	return nil
}

func (m *MockDatabase) GetCurrentLSN() uint64 {
	return m.lsn
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.BackupDir != "./backups" {
		t.Errorf("Expected backup dir './backups', got %s", config.BackupDir)
	}
	if config.CompressionLevel != 6 {
		t.Errorf("Expected compression level 6, got %d", config.CompressionLevel)
	}
}

func TestManagerCreation(t *testing.T) {
	config := DefaultConfig()
	db := &MockDatabase{dbPath: "/tmp/test.db"}

	mgr := NewManager(config, db)
	if mgr == nil {
		t.Fatal("Failed to create manager")
	}

	if mgr.config != config {
		t.Error("Config mismatch")
	}
}

func TestCreateBackup(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create mock database file
	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0 // Disable compression for simplicity

	db := &MockDatabase{
		dbPath:  dbFile,
		walPath: "", // No WAL for this test
		lsn:     100,
	}

	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if backup.ID == "" {
		t.Error("Backup ID should not be empty")
	}

	if backup.Type != TypeFull {
		t.Errorf("Expected type Full, got %v", backup.Type)
	}

	if backup.Size == 0 {
		t.Error("Backup size should not be zero")
	}

	// Verify backup file exists
	if _, err := os.Stat(backup.Destination); os.IsNotExist(err) {
		t.Error("Backup file should exist")
	}
}

func TestListBackups(t *testing.T) {
	config := DefaultConfig()
	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add mock backups
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_1",
		Type:        TypeFull,
		CompletedAt: time.Now(),
	})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_2",
		Type:        TypeIncremental,
		CompletedAt: time.Now(),
	})

	backups := mgr.ListBackups()
	if len(backups) != 2 {
		t.Errorf("Expected 2 backups, got %d", len(backups))
	}
}

func TestGetBackup(t *testing.T) {
	config := DefaultConfig()
	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add mock backup
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_test",
		Type:        TypeFull,
		CompletedAt: time.Now(),
	})

	backup := mgr.GetBackup("backup_test")
	if backup == nil {
		t.Fatal("Should find backup")
	}

	if backup.ID != "backup_test" {
		t.Errorf("Expected ID 'backup_test', got %s", backup.ID)
	}

	// Test non-existent backup
	backup = mgr.GetBackup("non_existent")
	if backup != nil {
		t.Error("Should not find non-existent backup")
	}
}

func TestDeleteBackup(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Create a mock backup file
	backupFile := filepath.Join(tempDir, "test_backup.db")
	if err := os.WriteFile(backupFile, []byte("backup data"), 0644); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	// Add mock backup
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "test_backup",
		Destination: backupFile,
		CompletedAt: time.Now(),
	})

	// Delete backup
	if err := mgr.DeleteBackup("test_backup"); err != nil {
		t.Fatalf("Failed to delete backup: %v", err)
	}

	// Verify backup file is deleted
	if _, err := os.Stat(backupFile); !os.IsNotExist(err) {
		t.Error("Backup file should be deleted")
	}

	// Verify backup is removed from metadata
	if len(mgr.metadata.Backups) != 0 {
		t.Error("Backup should be removed from metadata")
	}
}

func TestBackupInProgress(t *testing.T) {
	config := DefaultConfig()
	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	if mgr.IsBackupInProgress() {
		t.Error("Should not have backup in progress initially")
	}

	// Simulate backup in progress
	mgr.mu.Lock()
	mgr.activeBackup = true
	mgr.mu.Unlock()

	if !mgr.IsBackupInProgress() {
		t.Error("Should have backup in progress")
	}
}

func TestGenerateBackupID(t *testing.T) {
	id1 := generateBackupID()
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	id2 := generateBackupID()

	if id1 == "" {
		t.Error("Backup ID should not be empty")
	}

	if id1 == id2 {
		t.Error("Backup IDs should be unique")
	}

	if len(id1) < 10 {
		t.Error("Backup ID should be reasonably long")
	}
}

func TestCopyFile(t *testing.T) {
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

func TestCleanupOldBackups(t *testing.T) {
	config := DefaultConfig()
	config.MaxBackups = 2
	config.RetentionPeriod = 0 // Disable retention for this test

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add mock backups
	now := time.Now()
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_1",
		CompletedAt: now.Add(-3 * time.Hour),
	})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_2",
		CompletedAt: now.Add(-2 * time.Hour),
	})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "backup_3",
		CompletedAt: now.Add(-1 * time.Hour),
	})

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("Failed to cleanup backups: %v", err)
	}

	// Should have only 2 backups remaining (the newest ones)
	if len(mgr.metadata.Backups) != 2 {
		t.Errorf("Expected 2 backups after cleanup, got %d", len(mgr.metadata.Backups))
	}

	// Verify oldest backup was removed
	for _, b := range mgr.metadata.Backups {
		if b.ID == "backup_1" {
			t.Error("Oldest backup should have been removed")
		}
	}
}
