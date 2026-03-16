package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBackupCreationIncrementalNew tests incremental backup
func TestBackupCreationIncrementalNew(t *testing.T) {
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

// TestBackupCreationDifferentialNew tests differential backup
func TestBackupCreationDifferentialNew(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte("test database content"), 0644); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile, lsn: 200}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeDifferential)
	if err != nil {
		t.Fatalf("Failed to create differential backup: %v", err)
	}

	if backup.Type != TypeDifferential {
		t.Errorf("Expected type Differential, got %v", backup.Type)
	}
}

// TestBackupMaxBackupsCleanupNew tests max backups cleanup
func TestBackupMaxBackupsCleanupNew(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	config.MaxBackups = 3

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Add 5 backups
	for i := 0; i < 5; i++ {
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

	// Should have only 3 backups
	if len(mgr.metadata.Backups) != 3 {
		t.Errorf("Expected 3 backups after max cleanup, got %d", len(mgr.metadata.Backups))
	}
}

// TestBackupCancellationNew tests backup cancellation
func TestBackupCancellationNew(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024*1024*5) // 5MB to ensure cancellation has time
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

// TestBackupRestoreWithWALFilesNew tests restore including WAL files
func TestBackupRestoreWithWALFilesNew(t *testing.T) {
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

	db := &MockDatabase{dbPath: dbFile, walPath: walDir}
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
	targetWALPath := filepath.Join(targetDir, "wal", "wal_1.log")
	if _, err := os.Stat(targetWALPath); os.IsNotExist(err) {
		t.Error("WAL file should have been restored")
	}
}

// TestBackupAlreadyInProgressNew tests backup when one is already running
func TestBackupAlreadyInProgressNew(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test database content here"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Simulate active backup
	mgr.mu.Lock()
	mgr.activeBackup = true
	mgr.mu.Unlock()

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Should fail when backup already in progress")
	}
	if err.Error() != "backup already in progress" {
		t.Errorf("Wrong error message: %v", err)
	}
}

// TestIsBackupInProgressNew tests IsBackupInProgress method
func TestIsBackupInProgressNew(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()

	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	// Initially should not be in progress
	if mgr.IsBackupInProgress() {
		t.Error("IsBackupInProgress should return false initially")
	}

	// Set active backup
	mgr.mu.Lock()
	mgr.activeBackup = true
	mgr.mu.Unlock()

	if !mgr.IsBackupInProgress() {
		t.Error("IsBackupInProgress should return true when backup is active")
	}
}

// TestRestoreNonExistentBackupNew tests restoring non-existent backup
func TestRestoreNonExistentBackupNew(t *testing.T) {
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

// TestRestoreMissingBackupFileNew tests restore when backup file is missing
func TestRestoreMissingBackupFileNew(t *testing.T) {
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
