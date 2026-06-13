package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// MockDatabase implements Database interface for testing
type MockDatabase struct {
	dbPath  string
	walPath string
	lsn     uint64
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

type shortNilWriterDefault struct {
	limit   int
	written int
}

func (w *shortNilWriterDefault) Write(p []byte) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return 0, nil
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, nil
	}
	w.written += len(p)
	return len(p), nil
}

type shortNilWriteAtDefault struct {
	limit   int
	written int
}

func (w *shortNilWriteAtDefault) WriteAt(p []byte, _ int64) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return 0, nil
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, nil
	}
	w.written += len(p)
	return len(p), nil
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

	if mgr.config == config {
		t.Fatal("NewManager should copy caller config")
	}

	config.BackupDir = "/tmp/mutated"
	config.CompressionLevel = 1
	config.MaxBackups = 1
	config.RetentionPeriod = time.Second
	config.IncludeWAL = false
	config.Verify = false
	config.Encrypt = true

	defaults := DefaultConfig()
	if mgr.config.BackupDir != defaults.BackupDir {
		t.Fatalf("BackupDir aliased caller config: %q", mgr.config.BackupDir)
	}
	if mgr.config.CompressionLevel != defaults.CompressionLevel {
		t.Fatalf("CompressionLevel aliased caller config: %d", mgr.config.CompressionLevel)
	}
	if mgr.config.MaxBackups != defaults.MaxBackups {
		t.Fatalf("MaxBackups aliased caller config: %d", mgr.config.MaxBackups)
	}
	if mgr.config.RetentionPeriod != defaults.RetentionPeriod {
		t.Fatalf("RetentionPeriod aliased caller config: %v", mgr.config.RetentionPeriod)
	}
	if mgr.config.IncludeWAL != defaults.IncludeWAL {
		t.Fatalf("IncludeWAL aliased caller config: %v", mgr.config.IncludeWAL)
	}
	if mgr.config.Verify != defaults.Verify {
		t.Fatalf("Verify aliased caller config: %v", mgr.config.Verify)
	}
	if mgr.config.Encrypt != defaults.Encrypt {
		t.Fatalf("Encrypt aliased caller config: %v", mgr.config.Encrypt)
	}
}

func TestManagerCreationWithNilConfig(t *testing.T) {
	mgr := NewManager(nil, &MockDatabase{dbPath: "/tmp/test.db"})
	if mgr == nil {
		t.Fatal("Failed to create manager")
	}
	if mgr.config == nil {
		t.Fatal("Expected default config")
	}
	if mgr.config.BackupDir != DefaultConfig().BackupDir {
		t.Fatalf("BackupDir = %q, want default", mgr.config.BackupDir)
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
	backupInfo, err := os.Stat(backup.Destination)
	if os.IsNotExist(err) {
		t.Error("Backup file should exist")
	} else if err != nil {
		t.Fatalf("Failed to stat backup file: %v", err)
	} else if backupInfo.Mode().Perm() != backupFilePerm {
		t.Errorf("Expected backup file permissions %o, got %o", backupFilePerm, backupInfo.Mode().Perm())
	}

	metadataInfo, err := os.Stat(mgr.metadataPath())
	if err != nil {
		t.Fatalf("Failed to stat metadata file: %v", err)
	}
	if metadataInfo.Mode().Perm() != backupFilePerm {
		t.Errorf("Expected metadata file permissions %o, got %o", backupFilePerm, metadataInfo.Mode().Perm())
	}
}

func TestSaveMetadataLockedUsesRandomTempFile(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{ID: "backup-1"})

	staleTmpPath := mgr.metadataPath() + ".tmp"
	staleData := []byte("stale metadata temp")
	if err := os.WriteFile(staleTmpPath, staleData, backupFilePerm); err != nil {
		t.Fatalf("write stale metadata temp: %v", err)
	}

	if err := mgr.saveMetadataLocked(); err != nil {
		t.Fatalf("saveMetadataLocked: %v", err)
	}

	data, err := os.ReadFile(staleTmpPath)
	if err != nil {
		t.Fatalf("fixed metadata temp should not be consumed: %v", err)
	}
	if string(data) != string(staleData) {
		t.Fatalf("fixed metadata temp changed: got %q, want %q", data, staleData)
	}

	metadataInfo, err := os.Stat(mgr.metadataPath())
	if err != nil {
		t.Fatalf("stat metadata: %v", err)
	}
	if metadataInfo.Mode().Perm() != backupFilePerm {
		t.Fatalf("metadata permissions = %v, want %v", metadataInfo.Mode().Perm(), backupFilePerm)
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

func TestListBackupsReturnsIsolatedBackups(t *testing.T) {
	config := DefaultConfig()
	db := &MockDatabase{dbPath: "/tmp/test.db"}
	mgr := NewManager(config, db)

	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:       "backup_1",
		Type:     TypeFull,
		WALFiles: []string{"wal_1.log"},
	})

	backups := mgr.ListBackups()
	if len(backups) != 1 {
		t.Fatalf("Expected 1 backup, got %d", len(backups))
	}
	backups[0].ID = "mutated"
	backups[0].WALFiles[0] = "mutated.log"

	backup := mgr.GetBackup("backup_1")
	if backup == nil {
		t.Fatal("expected original backup to remain addressable")
	}
	if backup.ID != "backup_1" {
		t.Fatalf("ListBackups returned mutable backup pointer: got ID %q", backup.ID)
	}
	if backup.WALFiles[0] != "wal_1.log" {
		t.Fatalf("ListBackups returned mutable WALFiles slice: got %v", backup.WALFiles)
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
		WALFiles:    []string{"wal_1.log"},
	})

	backup := mgr.GetBackup("backup_test")
	if backup == nil {
		t.Fatal("Should find backup")
	}

	if backup.ID != "backup_test" {
		t.Errorf("Expected ID 'backup_test', got %s", backup.ID)
	}
	backup.ID = "mutated"
	backup.WALFiles[0] = "mutated.log"

	backup = mgr.GetBackup("backup_test")
	if backup == nil {
		t.Fatal("Should still find backup after mutating returned copy")
	}
	if backup.ID != "backup_test" {
		t.Fatalf("GetBackup returned mutable backup pointer: got ID %q", backup.ID)
	}
	if backup.WALFiles[0] != "wal_1.log" {
		t.Fatalf("GetBackup returned mutable WALFiles slice: got %v", backup.WALFiles)
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

func TestDeleteBackupRejectsParentWithDependents(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir

	parentPath := filepath.Join(tempDir, "parent.db")
	childPath := filepath.Join(tempDir, "child.db")
	if err := os.WriteFile(parentPath, []byte("parent"), backupFilePerm); err != nil {
		t.Fatalf("write parent backup: %v", err)
	}
	if err := os.WriteFile(childPath, []byte("child"), backupFilePerm); err != nil {
		t.Fatalf("write child backup: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{})
	mgr.metadata.Backups = append(mgr.metadata.Backups,
		&Backup{ID: "parent", Type: TypeFull, Destination: parentPath, CompletedAt: time.Now().Add(-time.Hour)},
		&Backup{ID: "child", Type: TypeIncremental, ParentID: "parent", Destination: childPath, CompletedAt: time.Now()},
	)

	err := mgr.DeleteBackup("parent")
	if err == nil || !strings.Contains(err.Error(), "required by backup child") {
		t.Fatalf("expected dependent backup rejection, got %v", err)
	}
	if _, err := os.Stat(parentPath); err != nil {
		t.Fatalf("parent backup file should remain: %v", err)
	}
	if mgr.GetBackup("parent") == nil {
		t.Fatal("parent metadata should remain after rejected delete")
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

func TestCleanupOldBackupsPreservesRestoreChainParents(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	config.MaxBackups = 1
	config.RetentionPeriod = 0

	mgr := NewManager(config, &MockDatabase{})
	now := time.Now()
	backups := []*Backup{
		{ID: "full", Type: TypeFull, Destination: filepath.Join(tempDir, "full.db"), CompletedAt: now.Add(-3 * time.Hour)},
		{ID: "inc1", Type: TypeIncremental, ParentID: "full", Destination: filepath.Join(tempDir, "inc1.db"), CompletedAt: now.Add(-2 * time.Hour)},
		{ID: "inc2", Type: TypeIncremental, ParentID: "inc1", Destination: filepath.Join(tempDir, "inc2.db"), CompletedAt: now.Add(-time.Hour)},
	}
	for _, backup := range backups {
		if err := os.WriteFile(backup.Destination, []byte(backup.ID), backupFilePerm); err != nil {
			t.Fatalf("write backup %s: %v", backup.ID, err)
		}
		mgr.metadata.Backups = append(mgr.metadata.Backups, backup)
	}

	if err := mgr.cleanupOldBackups(); err != nil {
		t.Fatalf("cleanupOldBackups failed: %v", err)
	}
	for _, id := range []string{"full", "inc1", "inc2"} {
		if mgr.GetBackup(id) == nil {
			t.Fatalf("backup %s should remain because it is required by the retained restore chain", id)
		}
	}
}

func TestGenerateBackupID(t *testing.T) {
	id1 := generateBackupID()
	time.Sleep(10 * time.Millisecond) // Ensure different timestamp
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

func TestDeleteBackupRejectsDestinationOutsideBackupDir(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	outside := filepath.Join(tempDir, "outside.db")
	original := []byte("outside data")
	if err := os.WriteFile(outside, original, backupFilePerm); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "evil",
		Destination: outside,
		CompletedAt: time.Now(),
	})

	err := mgr.DeleteBackup("evil")
	if err == nil {
		t.Fatal("expected outside backup destination to be rejected")
	}
	if !strings.Contains(err.Error(), "inside backup directory") {
		t.Fatalf("expected inside backup directory rejection, got %v", err)
	}
	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("read outside file: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("outside file was modified, got %q", got)
	}
	if mgr.GetBackup("evil") == nil {
		t.Fatal("failed delete should preserve metadata entry")
	}
}

func TestDeleteBackupRejectsTraversalWALID(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	backupPath := filepath.Join(config.BackupDir, "safe.db")
	if err := os.WriteFile(backupPath, []byte("backup"), backupFilePerm); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	mgr := NewManager(config, &MockDatabase{})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "../outside",
		Destination: backupPath,
		CompletedAt: time.Now(),
		WALFiles:    []string{"wal"},
	})

	err := mgr.DeleteBackup("../outside")
	if err == nil {
		t.Fatal("expected traversal backup ID to be rejected")
	}
	if !strings.Contains(err.Error(), "directory components") {
		t.Fatalf("expected directory component rejection, got %v", err)
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file should remain after failed delete: %v", err)
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

func TestCopyFileRejectsUnsafeSource(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "target.db")
	link := filepath.Join(tempDir, "target-link.db")
	if err := os.WriteFile(target, []byte("backup data"), 0600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := copyFile(link, filepath.Join(tempDir, "copy.db"))
	if err == nil {
		t.Fatal("expected symlink source to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	err = copyFile(tempDir, filepath.Join(tempDir, "copy-dir.db"))
	if err == nil {
		t.Fatal("expected directory source to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestOpenBackupReaderRejectsUnsafeDestination(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "backup.db")
	link := filepath.Join(tempDir, "backup-link.db")
	if err := os.WriteFile(target, []byte("backup data"), 0600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})
	if _, err := mgr.openBackupReader(&Backup{Destination: link}); err == nil {
		t.Fatal("expected symlink backup destination to be rejected")
	} else if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	subdir := filepath.Join(tempDir, "backup-dir")
	if err := os.Mkdir(subdir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}
	if _, err := mgr.openBackupReader(&Backup{Destination: subdir}); err == nil {
		t.Fatal("expected directory backup destination to be rejected")
	} else if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestCreateBackupRejectsSymlinkWALPath(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")
	walFile := filepath.Join(tempDir, "wal.log")
	walLink := filepath.Join(tempDir, "wal-link.log")
	if err := os.WriteFile(dbFile, []byte("database"), 0600); err != nil {
		t.Fatalf("write db: %v", err)
	}
	if err := os.WriteFile(walFile, []byte("wal"), 0600); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	if err := os.Symlink(walFile, walLink); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.IncludeWAL = true
	config.Verify = false
	mgr := NewManager(config, &MockDatabase{dbPath: dbFile, walPath: walLink})

	_, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err == nil {
		t.Fatal("expected symlink WAL path to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL path must not be a symlink") {
		t.Fatalf("expected WAL symlink rejection, got %v", err)
	}
}

func TestCopyWALFilesRejectsSymlinkBackupWALDir(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backups")
	outsideDir := filepath.Join(tempDir, "outside")
	walDir := filepath.Join(tempDir, "wal")
	for _, dir := range []string{backupDir, outsideDir, walDir} {
		if err := os.MkdirAll(dir, backupDirPerm); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(walDir, "wal_1.log"), []byte("wal"), 0600); err != nil {
		t.Fatalf("write WAL file: %v", err)
	}

	walBackupDir := filepath.Join(backupDir, "backup1_wal")
	if err := os.Symlink(outsideDir, walBackupDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = backupDir
	mgr := NewManager(config, &MockDatabase{walPath: walDir})
	backup := &Backup{ID: "backup1"}

	err := mgr.copyWALFiles(context.Background(), backup)
	if err == nil {
		t.Fatal("expected symlink WAL backup directory to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(outsideDir, "wal_1.log")); !os.IsNotExist(statErr) {
		t.Fatalf("WAL file was written through symlink, stat err=%v", statErr)
	}
}

func TestPrepareBackupDirRejectsSymlinkDirectory(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "target")
	link := filepath.Join(tempDir, "backups")
	if err := os.Mkdir(target, backupDirPerm); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := prepareBackupDir(link, true)
	if err == nil {
		t.Fatal("expected symlink backup directory to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestPrepareBackupDirRejectsSymlinkParent(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "target")
	link := filepath.Join(tempDir, "link")
	if err := os.Mkdir(target, backupDirPerm); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := prepareBackupDir(filepath.Join(link, "nested"), true)
	if err == nil {
		t.Fatal("expected symlink backup directory component to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink component rejection, got %v", err)
	}
}

func TestPrepareBackupDirCreatesRestrictiveDirectory(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backups")

	if err := prepareBackupDir(backupDir, true); err != nil {
		t.Fatalf("prepareBackupDir: %v", err)
	}
	info, err := os.Stat(backupDir)
	if err != nil {
		t.Fatalf("stat backup dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("backup path is not a directory")
	}
	if info.Mode().Perm() != backupDirPerm {
		t.Fatalf("backup dir permissions = %v, want %v", info.Mode().Perm(), backupDirPerm)
	}
}

func TestLoadMetadataRejectsUnsafePath(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})

	metadataPath := mgr.metadataPath()
	linkPath := filepath.Join(tempDir, "metadata-link.json")
	if err := os.WriteFile(metadataPath, []byte(`{"Backups":[]}`), backupFilePerm); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := os.Symlink(metadataPath, linkPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}
	if err := os.Remove(metadataPath); err != nil {
		t.Fatalf("remove metadata: %v", err)
	}
	if err := os.Rename(linkPath, metadataPath); err != nil {
		t.Fatalf("rename symlink into metadata path: %v", err)
	}

	err := mgr.loadMetadata()
	if err == nil {
		t.Fatal("expected symlink metadata path to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	if err := os.Remove(metadataPath); err != nil {
		t.Fatalf("remove symlink metadata: %v", err)
	}
	if err := os.Mkdir(metadataPath, 0750); err != nil {
		t.Fatalf("mkdir metadata path: %v", err)
	}
	err = mgr.loadMetadata()
	if err == nil {
		t.Fatal("expected directory metadata path to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestLoadMetadataRestrictsExistingPermissions(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})
	metadataPath := mgr.metadataPath()
	if err := os.WriteFile(metadataPath, []byte(`{"Backups":[]}`), 0644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	if err := mgr.loadMetadata(); err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	info, err := os.Stat(metadataPath)
	if err != nil {
		t.Fatalf("stat metadata: %v", err)
	}
	if info.Mode().Perm() != backupFilePerm {
		t.Fatalf("metadata permissions = %v, want %v", info.Mode().Perm(), backupFilePerm)
	}
}

func TestRestoreMissingWALPreservesExistingTarget(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	backupPath := filepath.Join(config.BackupDir, "backup.db")
	if err := os.WriteFile(backupPath, []byte("restored database"), backupFilePerm); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "missing-wal",
		Type:        TypeFull,
		Destination: backupPath,
		CompletedAt: time.Now(),
		WALFiles:    []string{"missing.log"},
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, backupFilePerm); err != nil {
		t.Fatalf("Failed to create existing target: %v", err)
	}

	if err := mgr.Restore(context.Background(), "missing-wal", targetPath); err == nil {
		t.Fatal("Expected missing WAL restore to fail")
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed WAL restore should preserve existing target, got %q", string(got))
	}
}

func TestRestoreRejectsTraversalWALFileName(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	backupPath := filepath.Join(config.BackupDir, "backup.db")
	payload := []byte("restored database")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	walDir := filepath.Join(config.BackupDir, "full_wal")
	if err := os.Mkdir(walDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir wal dir: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "full",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        int64(len(payload)),
		Checksum:    crc32.ChecksumIEEE(payload),
		CompletedAt: time.Now(),
		WALFiles:    []string{"../outside.wal"},
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	err := mgr.Restore(context.Background(), "full", targetPath)
	if err == nil {
		t.Fatal("expected traversal WAL file name to be rejected")
	}
	if !strings.Contains(err.Error(), "directory components") {
		t.Fatalf("expected directory component rejection, got %v", err)
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("failed restore should not replace target, stat err = %v", err)
	}
}

func TestRestoreRejectsSingleFileWALWithMultipleFiles(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := t.TempDir()

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	backupPath := filepath.Join(config.BackupDir, "backup.db")
	payload := []byte("restored database")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	if err := os.Mkdir(filepath.Join(config.BackupDir, "full_wal"), backupDirPerm); err != nil {
		t.Fatalf("mkdir wal dir: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:            "full",
		Type:          TypeFull,
		Destination:   backupPath,
		Size:          int64(len(payload)),
		Checksum:      crc32.ChecksumIEEE(payload),
		CompletedAt:   time.Now(),
		WALPathIsFile: true,
		WALFiles:      []string{"one.wal", "two.wal"},
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, backupFilePerm); err != nil {
		t.Fatalf("write existing target: %v", err)
	}

	err := mgr.Restore(context.Background(), "full", targetPath)
	if err == nil {
		t.Fatal("expected single-file WAL metadata mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "exactly one WAL file") {
		t.Fatalf("expected single-file WAL count rejection, got %v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed restore should preserve existing target, got %q", string(got))
	}
}

func TestRestoreRejectsSymlinkTargetDirectory(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	payload := []byte("restored database")
	backupPath := filepath.Join(config.BackupDir, "backup.db")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	realTargetDir := filepath.Join(tempDir, "real-target")
	linkTargetDir := filepath.Join(tempDir, "linked-target")
	if err := os.Mkdir(realTargetDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir real target: %v", err)
	}
	if err := os.Symlink(realTargetDir, linkTargetDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "full",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        int64(len(payload)),
		Checksum:    crc32.ChecksumIEEE(payload),
		CompletedAt: time.Now(),
	})

	err := mgr.Restore(context.Background(), "full", filepath.Join(linkTargetDir, "restored.db"))
	if err == nil {
		t.Fatal("expected symlink restore target directory to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(realTargetDir, "restored.db")); !os.IsNotExist(err) {
		t.Fatalf("restore should not write through symlink target dir, stat err = %v", err)
	}
}

func TestRestoreRejectsSymlinkTargetFile(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	payload := []byte("restored database")
	backupPath := filepath.Join(config.BackupDir, "backup.db")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("write backup file: %v", err)
	}

	target := filepath.Join(tempDir, "target.db")
	link := filepath.Join(tempDir, "target-link.db")
	original := []byte("do not replace")
	if err := os.WriteFile(target, original, backupFilePerm); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "full",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        int64(len(payload)),
		Checksum:    crc32.ChecksumIEEE(payload),
		CompletedAt: time.Now(),
	})

	err := mgr.Restore(context.Background(), "full", link)
	if err == nil {
		t.Fatal("expected symlink restore target file to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("restore modified symlink target, got %q", got)
	}
}

func TestRestoreRejectsDirectoryTargetFile(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("mkdir backup dir: %v", err)
	}

	payload := []byte("restored database")
	backupPath := filepath.Join(config.BackupDir, "backup.db")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("write backup file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "full",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        int64(len(payload)),
		Checksum:    crc32.ChecksumIEEE(payload),
		CompletedAt: time.Now(),
	})

	err := mgr.Restore(context.Background(), "full", tempDir)
	if err == nil {
		t.Fatal("expected directory restore target file to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestCreateSecureFileRejectsSymlinkTarget(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "target.db")
	link := filepath.Join(tempDir, "target-link.db")
	original := []byte("do not truncate")
	if err := os.WriteFile(target, original, backupFilePerm); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	file, err := createSecureFile(link)
	if err == nil {
		_ = file.Close()
		t.Fatal("expected symlink target file to be rejected")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("symlink target was modified, got %q", got)
	}
}

func TestRestoreSingleFileWALRejectsDirectoryBeforeReplacingTarget(t *testing.T) {
	tempDir := t.TempDir()

	dbFile := filepath.Join(tempDir, "source.db")
	if err := os.WriteFile(dbFile, []byte("restored database"), backupFilePerm); err != nil {
		t.Fatalf("write source db: %v", err)
	}
	walFile := dbFile + ".wal"
	if err := os.WriteFile(walFile, []byte("restored wal"), backupFilePerm); err != nil {
		t.Fatalf("write source wal: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	config.CompressionLevel = 0
	config.Verify = true

	mgr := NewManager(config, &MockDatabase{dbPath: dbFile, walPath: walFile})
	backup, err := mgr.CreateBackup(context.Background(), TypeFull)
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if !backup.WALPathIsFile {
		t.Fatal("expected single-file WAL backup")
	}

	targetPath := filepath.Join(tempDir, "target.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, backupFilePerm); err != nil {
		t.Fatalf("write existing target: %v", err)
	}
	if err := os.Mkdir(targetPath+".wal", backupDirPerm); err != nil {
		t.Fatalf("create stale WAL directory: %v", err)
	}

	err = mgr.Restore(context.Background(), backup.ID, targetPath)
	if err == nil || !strings.Contains(err.Error(), "restore WAL target must be a regular file") {
		t.Fatalf("expected WAL target directory rejection, got %v", err)
	}

	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed WAL preflight should preserve existing target, got %q", string(got))
	}
}

func TestRestoreRejectsTruncatedBackupPayload(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "target")
	if err := os.MkdirAll(targetDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	payload := []byte("short restored database")
	backupPath := filepath.Join(config.BackupDir, "truncated.db")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "truncated",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        int64(len(payload) + 1),
		Checksum:    crc32.ChecksumIEEE(payload),
		CompletedAt: time.Now(),
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, backupFilePerm); err != nil {
		t.Fatalf("Failed to create existing target: %v", err)
	}

	err := mgr.Restore(context.Background(), "truncated", targetPath)
	if err == nil || !strings.Contains(err.Error(), "restored backup size mismatch") {
		t.Fatalf("Expected restored backup size mismatch, got %v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed restore should preserve existing target, got %q", string(got))
	}
}

func TestRestoreRejectsNonEmptyPayloadWithZeroMetadataSize(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "target")
	if err := os.MkdirAll(targetDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}

	config := DefaultConfig()
	config.BackupDir = filepath.Join(tempDir, "backups")
	if err := os.MkdirAll(config.BackupDir, backupDirPerm); err != nil {
		t.Fatalf("Failed to create backup dir: %v", err)
	}

	payload := []byte("metadata says empty but payload is not")
	backupPath := filepath.Join(config.BackupDir, "zero-size.db")
	if err := os.WriteFile(backupPath, payload, backupFilePerm); err != nil {
		t.Fatalf("Failed to create backup file: %v", err)
	}

	mgr := NewManager(config, &MockDatabase{dbPath: filepath.Join(tempDir, "source.db")})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{
		ID:          "zero-size",
		Type:        TypeFull,
		Destination: backupPath,
		Size:        0,
		Checksum:    0,
		CompletedAt: time.Now(),
	})

	targetPath := filepath.Join(targetDir, "restored.db")
	original := []byte("existing target database")
	if err := os.WriteFile(targetPath, original, backupFilePerm); err != nil {
		t.Fatalf("Failed to create existing target: %v", err)
	}

	err := mgr.Restore(context.Background(), "zero-size", targetPath)
	if err == nil || !strings.Contains(err.Error(), "restored backup size mismatch") {
		t.Fatalf("Expected restored backup size mismatch, got %v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("Failed to read target after failed restore: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("failed restore should preserve existing target, got %q", string(got))
	}
}

func TestApplyDeltaPayloadRejectsUnsafeTarget(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})

	validDelta := `{"chunk_size":1024,"target_size":4}` + "\n" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x04\x00\x00\x00" +
		"safe"

	target := filepath.Join(tempDir, "target.db")
	link := filepath.Join(tempDir, "target-link.db")
	if err := os.WriteFile(target, []byte("orig"), 0600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := mgr.applyDeltaPayload(context.Background(), strings.NewReader(validDelta), link)
	if err == nil {
		t.Fatal("expected symlink restore target to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	err = mgr.applyDeltaPayload(context.Background(), strings.NewReader(validDelta), tempDir)
	if err == nil {
		t.Fatal("expected directory restore target to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestApplyDeltaPayloadRestrictsTargetPermissions(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})

	target := filepath.Join(tempDir, "target.db")
	if err := os.WriteFile(target, []byte("orig"), 0644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	validDelta := `{"chunk_size":1024,"target_size":4}` + "\n" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x04\x00\x00\x00" +
		"safe"

	if err := mgr.applyDeltaPayload(context.Background(), strings.NewReader(validDelta), target); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if info.Mode().Perm() != backupFilePerm {
		t.Fatalf("target permissions = %v, want %v", info.Mode().Perm(), backupFilePerm)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "safe" {
		t.Fatalf("target content = %q, want safe", got)
	}
}

func TestApplyDeltaPayloadRejectsOversizedHeader(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})

	target := filepath.Join(tempDir, "target.db")
	if err := os.WriteFile(target, []byte("orig"), backupFilePerm); err != nil {
		t.Fatalf("write target: %v", err)
	}

	delta := strings.Repeat("x", maxDeltaHeaderLineLen+1) + "\n"
	err := mgr.applyDeltaPayload(context.Background(), strings.NewReader(delta), target)
	if err == nil || !strings.Contains(err.Error(), "delta header too large") {
		t.Fatalf("expected oversized delta header rejection, got %v", err)
	}
}

func TestLoadMetadataRejectsOversizedFile(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})

	metadataPath := mgr.metadataPath()
	file, err := os.OpenFile(metadataPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, backupFilePerm)
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}
	if err := file.Truncate(maxBackupMetadataBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate metadata: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close metadata: %v", err)
	}

	err = mgr.loadMetadata()
	if err == nil || !strings.Contains(err.Error(), "backup metadata is too large") {
		t.Fatalf("expected oversized metadata rejection, got %v", err)
	}
}

func TestLoadMetadataRejectsTrailingJSON(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.BackupDir = tempDir
	mgr := NewManager(config, &MockDatabase{})
	mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{ID: "existing"})

	metadataPath := mgr.metadataPath()
	if err := os.WriteFile(metadataPath, []byte(`{"backups":[]} {"backups":[]}`), backupFilePerm); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	err := mgr.loadMetadata()
	if err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("expected trailing metadata rejection, got %v", err)
	}
	if len(mgr.metadata.Backups) != 1 || mgr.metadata.Backups[0].ID != "existing" {
		t.Fatalf("failed metadata load changed in-memory metadata: %+v", mgr.metadata.Backups)
	}
}

func TestLoadMetadataRejectsInvalidBackupEntries(t *testing.T) {
	tests := []struct {
		name    string
		backups []*Backup
		want    string
	}{
		{
			name: "duplicate ID",
			backups: []*Backup{
				{ID: "dup", Type: TypeFull, Destination: "one.db"},
				{ID: "dup", Type: TypeFull, Destination: "two.db"},
			},
			want: "duplicate backup ID",
		},
		{
			name:    "invalid type",
			backups: []*Backup{{ID: "bad-type", Type: Type(99), Destination: "bad-type.db"}},
			want:    "invalid type",
		},
		{
			name:    "negative size",
			backups: []*Backup{{ID: "negative", Type: TypeFull, Destination: "negative.db", Size: -1}},
			want:    "invalid size",
		},
		{
			name:    "empty destination",
			backups: []*Backup{{ID: "empty-destination", Type: TypeFull}},
			want:    "empty destination",
		},
		{
			name:    "traversal destination",
			backups: []*Backup{{ID: "outside", Type: TypeFull, Destination: "../outside.db"}},
			want:    "inside backup directory",
		},
		{
			name:    "traversal WAL file",
			backups: []*Backup{{ID: "wal-traversal", Type: TypeFull, Destination: "wal-traversal.db", WALFiles: []string{"../wal"}}},
			want:    "invalid WAL file name",
		},
		{
			name:    "dot WAL file",
			backups: []*Backup{{ID: "wal-dot", Type: TypeFull, Destination: "wal-dot.db", WALFiles: []string{"."}}},
			want:    "invalid WAL file name",
		},
		{
			name:    "single-file WAL with multiple files",
			backups: []*Backup{{ID: "multi-file-wal", Type: TypeFull, Destination: "multi-file-wal.db", WALPathIsFile: true, WALFiles: []string{"one.wal", "two.wal"}}},
			want:    "single-file WAL metadata must contain exactly one WAL file",
		},
		{
			name:    "oversized ID",
			backups: []*Backup{{ID: strings.Repeat("a", maxBackupMetadataField+1), Type: TypeFull, Destination: "oversized-id.db"}},
			want:    "ID is too large",
		},
		{
			name:    "oversized parent ID",
			backups: []*Backup{{ID: "oversized-parent", Type: TypeIncremental, ParentID: strings.Repeat("p", maxBackupMetadataField+1), Destination: "oversized-parent.db"}},
			want:    "parent ID is too large",
		},
		{
			name:    "too many WAL files",
			backups: []*Backup{{ID: "too-many-wals", Type: TypeFull, Destination: "too-many-wals.db", WALFiles: make([]string, maxBackupMetadataWALs+1)}},
			want:    "too many WAL files",
		},
		{
			name:    "oversized WAL file name",
			backups: []*Backup{{ID: "oversized-wal", Type: TypeFull, Destination: "oversized-wal.db", WALFiles: []string{strings.Repeat("w", maxBackupMetadataField+1)}}},
			want:    "WAL file name is too large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			config := DefaultConfig()
			config.BackupDir = tempDir
			mgr := NewManager(config, &MockDatabase{})
			mgr.metadata.Backups = append(mgr.metadata.Backups, &Backup{ID: "existing"})

			for _, backup := range tt.backups {
				if backup.Destination != "" && !filepath.IsAbs(backup.Destination) {
					backup.Destination = filepath.Join(tempDir, backup.Destination)
				}
				for i, walFile := range backup.WALFiles {
					if walFile == "" {
						backup.WALFiles[i] = fmt.Sprintf("wal-%d.log", i)
					}
				}
			}
			raw, err := json.Marshal(Metadata{Backups: tt.backups})
			if err != nil {
				t.Fatalf("marshal metadata: %v", err)
			}
			if err := os.WriteFile(mgr.metadataPath(), raw, backupFilePerm); err != nil {
				t.Fatalf("write metadata: %v", err)
			}

			err = mgr.loadMetadata()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected metadata rejection containing %q, got %v", tt.want, err)
			}
			if len(mgr.metadata.Backups) != 1 || mgr.metadata.Backups[0].ID != "existing" {
				t.Fatalf("failed metadata load changed in-memory metadata: %+v", mgr.metadata.Backups)
			}
		})
	}
}

func TestValidateLoadedMetadataRejectsTooManyEntriesBeforeScanning(t *testing.T) {
	config := DefaultConfig()
	config.BackupDir = t.TempDir()
	mgr := NewManager(config, &MockDatabase{})

	metadata := &Metadata{Backups: make([]*Backup, maxBackupMetadataItems+1)}
	err := mgr.validateLoadedMetadata(metadata)
	if err == nil || !strings.Contains(err.Error(), "too many backup metadata entries") {
		t.Fatalf("expected too many metadata entries rejection, got %v", err)
	}
}

func TestSafeChildPathRejectsNonBaseNames(t *testing.T) {
	parent := t.TempDir()
	tests := []string{
		".",
		"..",
		"./wal",
		"wal/../safe",
		"wal/segment",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := safeChildPath(parent, name); err == nil {
				t.Fatalf("expected %q to be rejected", name)
			}
		})
	}

	path, err := safeChildPath(parent, "wal")
	if err != nil {
		t.Fatalf("expected basename to be accepted: %v", err)
	}
	if path != filepath.Join(parent, "wal") {
		t.Fatalf("safeChildPath returned %q, want %q", path, filepath.Join(parent, "wal"))
	}
}

func TestPayloadWriterRejectsShortWriteDefault(t *testing.T) {
	pw := &payloadWriter{writer: &shortNilWriterDefault{limit: 3}, crc: crc32.NewIEEE()}

	n, err := pw.Write([]byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("payloadWriter short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 3 {
		t.Fatalf("payloadWriter wrote %d bytes, want 3", n)
	}
	if pw.written != 3 {
		t.Fatalf("payloadWriter recorded %d bytes, want 3", pw.written)
	}
}

func TestWriteFullRejectsShortWriteDefault(t *testing.T) {
	w := &shortNilWriterDefault{limit: 3}

	n, err := writeFull(w, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 3 {
		t.Fatalf("writeFull wrote %d bytes, want 3", n)
	}
}

func TestWriteFullAtRejectsShortWriteDefault(t *testing.T) {
	w := &shortNilWriteAtDefault{limit: 3}

	n, err := writeFullAt(w, []byte("abcdef"), 42)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFullAt short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 3 {
		t.Fatalf("writeFullAt wrote %d bytes, want 3", n)
	}
}

func TestWriteDeltaRecordRejectsShortDataWriteDefault(t *testing.T) {
	w := &shortNilWriterDefault{limit: 12}
	err := writeDeltaRecord(w, 42, []byte("hello"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeDeltaRecord short data write error = %v, want %v", err, io.ErrShortWrite)
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
