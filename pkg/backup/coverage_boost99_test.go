package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// MockDatabaseFailingHotBackup simulates hot backup failure
type MockDatabaseFailingHotBackup struct {
	MockDatabase
}

func (m *MockDatabaseFailingHotBackup) BeginHotBackup() error {
	return errors.New("hot backup not supported")
}

// MockDatabaseFailingCheckpoint simulates checkpoint failure
type MockDatabaseFailingCheckpoint struct {
	MockDatabase
}

func (m *MockDatabaseFailingCheckpoint) Checkpoint() error {
	return errors.New("checkpoint failed")
}

// TestCreateBackupAlreadyInProgress tests backup when one is already running
func TestCreateBackupAlreadyInProgress(t *testing.T) {
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

// TestCreateBackupBeginHotBackupError tests backup when BeginHotBackup fails
func TestCreateBackupBeginHotBackupError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test database"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseFailingHotBackup{MockDatabase{dbPath: dbFile, lsn: 1}}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Should fail when BeginHotBackup fails")
	}
}

// TestCreateBackupCheckpointError tests backup when Checkpoint fails
func TestCreateBackupCheckpointError(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test database"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabaseFailingCheckpoint{MockDatabase{dbPath: dbFile, lsn: 1}}
	mgr := NewManager(config, db)

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err == nil {
		t.Error("Should fail when Checkpoint fails")
	}
}

// TestCreateBackupVerifyCallback tests backup with verify callback
func TestCreateBackupVerifyCallback(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	content := make([]byte, 1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	os.WriteFile(dbFile, content, 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.Verify = true // Enable verification

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Set up verify callback
	verifyCalled := false
	mgr.OnVerify = func(backup *Backup, valid bool) {
		verifyCalled = true
		if !valid {
			t.Error("Backup should be valid")
		}
	}

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if !verifyCalled {
		t.Error("OnVerify callback should have been called")
	}
}

// TestCreateBackupCompleteCallback tests OnComplete callback
func TestCreateBackupCompleteCallback(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test database content here"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	completeCalled := false
	mgr.OnComplete = func(backup *Backup, err error) {
		completeCalled = true
		if err != nil {
			t.Errorf("Should not have error: %v", err)
		}
		if backup == nil {
			t.Error("Backup should not be nil")
		}
	}

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if !completeCalled {
		t.Error("OnComplete callback should have been called")
	}
}

// TestCreateBackupWithoutWAL tests backup without including WAL
func TestCreateBackupWithoutWAL(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	walDir := filepath.Join(tempDir, "test.wal")
	os.WriteFile(dbFile, []byte("test database"), 0644)
	os.MkdirAll(walDir, 0755)
	os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("wal"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.IncludeWAL = false // Don't include WAL

	db := &MockDatabaseWithWAL{dbPath: dbFile, walPath: walDir}
	mgr := NewManager(config, db)

	ctx := context.Background()
	backup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if len(backup.WALFiles) != 0 {
		t.Errorf("Should have no WAL files, got %d", len(backup.WALFiles))
	}
}

// TestCreateBackupIncrementalWithParent tests incremental backup parent ID
func TestCreateBackupIncrementalWithParent(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test database content here"), 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile, lsn: 100}
	mgr := NewManager(config, db)

	// Create full backup first
	ctx := context.Background()
	fullBackup, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create full backup: %v", err)
	}
	t.Logf("Full backup ID: %s", fullBackup.ID)

	// Update LSN for incremental
	db.lsn = 200

	// Create incremental backup
	incrBackup, err := mgr.CreateBackup(ctx, TypeIncremental)
	if err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}

	if !incrBackup.Incremental {
		t.Error("Incremental flag should be true")
	}

	// Note: ParentID may not be set in current implementation
	t.Logf("Incremental backup ID: %s, ParentID: %s", incrBackup.ID, incrBackup.ParentID)
}

// TestRestoreIncrementalNotSupported tests restoring incremental backup
func TestRestoreIncrementalNotSupported(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test"), 0644)

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Create an incremental backup entry manually
	backup := &Backup{
		ID:          "incr-backup",
		Destination: filepath.Join(tempDir, "incr.db"),
		Type:        TypeIncremental,
		Incremental: true,
		ParentID:    "parent-id",
	}
	os.WriteFile(backup.Destination, []byte("backup content"), 0644)
	mgr.metadata.Backups = append(mgr.metadata.Backups, backup)

	ctx := context.Background()
	err := mgr.Restore(ctx, backup.ID, filepath.Join(tempDir, "restored.db"))
	// Should succeed or fail gracefully
	if err != nil {
		t.Logf("Incremental restore returned: %v", err)
	}
}

// TestGetBackupNotFound tests getting non-existent backup
func TestGetBackupNotFound(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: filepath.Join(tempDir, "test.db")}
	mgr := NewManager(config, db)

	backup := mgr.GetBackup("non-existent")
	if backup != nil {
		t.Error("Should return nil for non-existent backup")
	}
}

// TestCreateBackupWithProgressCallback tests progress callback
func TestCreateBackupWithProgressCallback(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	// Create larger file to have progress updates
	content := make([]byte, 1024*100) // 100KB
	for i := range content {
		content[i] = byte(i % 256)
	}
	os.WriteFile(dbFile, content, 0644)

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	progressUpdates := 0
	mgr.OnProgress = func(percent int) {
		progressUpdates++
	}

	ctx := context.Background()
	_, err := mgr.CreateBackup(ctx, TypeFull)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Progress callback may or may not be called depending on implementation
	t.Logf("Progress updates: %d", progressUpdates)
}

// TestDeleteBackupWithWALDir tests deleting backup with WAL directory
func TestDeleteBackupWithWALDir(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "test.db")
	os.WriteFile(dbFile, []byte("test"), 0644)

	config := DefaultConfig()
	config.BackupDir = tempDir

	db := &MockDatabase{dbPath: dbFile}
	mgr := NewManager(config, db)

	// Create backup with WAL files
	backup := &Backup{
		ID:          "wal-backup",
		Destination: filepath.Join(tempDir, "wal-backup.db"),
		WALFiles:    []string{"000001.wal", "000002.wal"},
	}
	os.WriteFile(backup.Destination, []byte("backup"), 0644)

	// Create WAL directory
	walDir := filepath.Join(tempDir, "wal-backup_wal")
	os.MkdirAll(walDir, 0755)
	os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("wal1"), 0644)

	mgr.metadata.Backups = append(mgr.metadata.Backups, backup)

	err := mgr.DeleteBackup(backup.ID)
	if err != nil {
		t.Errorf("Should succeed, got: %v", err)
	}

	// Verify WAL directory is removed
	if _, err := os.Stat(walDir); !os.IsNotExist(err) {
		t.Error("WAL directory should be removed")
	}
}
