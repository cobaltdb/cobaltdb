package storage

import (
	"os"
	"testing"
	"time"
)

func TestPITRManagerBasic(t *testing.T) {
	// Create temporary directory for archives
	tmpDir, err := os.MkdirTemp("", "pitr_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &PITRConfig{
		Enabled:         true,
		ArchiveDir:      tmpDir,
		RetentionDays:   7,
		CompressionType: "none",
		VerifyChecksums: false,
	}

	// Create mock WAL and backend
	wal := &WAL{}
	backend := NewMemory()

	pm := NewPITRManager(wal, backend, config)

	if !pm.config.Enabled {
		t.Error("Expected PITR to be enabled")
	}

	if pm.config.ArchiveDir != tmpDir {
		t.Errorf("Expected archive dir %s, got %s", tmpDir, pm.config.ArchiveDir)
	}
}

func TestPITRManagerArchiveWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pitr_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &PITRConfig{
		Enabled:         true,
		ArchiveDir:      tmpDir,
		RetentionDays:   7,
		CompressionType: "none",
		VerifyChecksums: false,
	}

	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	// Archive some data
	data := []byte("test wal data")
	err = pm.ArchiveWAL(0, 100, data)
	if err != nil {
		t.Fatalf("Failed to archive WAL: %v", err)
	}

	// Check status
	status := pm.GetArchiveStatus()
	if status.SuccessCount != 1 {
		t.Errorf("Expected 1 success, got %d", status.SuccessCount)
	}

	// Check archives
	archives := pm.ListArchives()
	if len(archives) != 1 {
		t.Fatalf("Expected 1 archive, got %d", len(archives))
	}

	if archives[0].StartLSN != 0 || archives[0].EndLSN != 100 {
		t.Errorf("Expected LSN range [0, 100], got [%d, %d]", archives[0].StartLSN, archives[0].EndLSN)
	}
}

func TestPITRManagerDisabled(t *testing.T) {
	config := &PITRConfig{
		Enabled: false,
	}

	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	// Archiving should be no-op when disabled
	err := pm.ArchiveWAL(0, 100, []byte("data"))
	if err != nil {
		t.Errorf("Expected no error when disabled, got %v", err)
	}

	status := pm.GetArchiveStatus()
	if status.SuccessCount != 0 {
		t.Error("Expected 0 successes when disabled")
	}
}

func TestPITRManagerGetArchivesForRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pitr_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &PITRConfig{
		Enabled:         true,
		ArchiveDir:      tmpDir,
		RetentionDays:   7,
		CompressionType: "none",
		VerifyChecksums: false,
	}

	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	// Archive multiple segments
	pm.ArchiveWAL(0, 100, []byte("data1"))
	pm.ArchiveWAL(100, 200, []byte("data2"))
	pm.ArchiveWAL(200, 300, []byte("data3"))

	// Get archives for recovery to LSN 150
	archives := pm.GetArchivesForRecovery(150)

	// Should return archives 0-100 and 100-200
	if len(archives) != 2 {
		t.Errorf("Expected 2 archives for recovery to 150, got %d", len(archives))
	}
}

func TestPITRManagerCleanupOldArchives(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pitr_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &PITRConfig{
		Enabled:         true,
		ArchiveDir:      tmpDir,
		RetentionDays:   1,
		CompressionType: "none",
		VerifyChecksums: false,
	}

	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	// Create archive with old timestamp
	oldFile := &ArchiveFile{
		StartLSN:  0,
		EndLSN:    100,
		Path:      tmpDir + "/old.wal",
		Size:      100,
		Checksum:  "abc",
		CreatedAt: time.Now().AddDate(0, 0, -7),
	}

	pm.filesMu.Lock()
	pm.archiveFiles[0] = oldFile
	pm.filesMu.Unlock()

	// Create the file
	os.WriteFile(oldFile.Path, []byte("old"), 0644)

	// Run cleanup
	err = pm.CleanupOldArchives()
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// Old archive should be removed
	pm.filesMu.RLock()
	_, exists := pm.archiveFiles[0]
	pm.filesMu.RUnlock()

	if exists {
		t.Error("Expected old archive to be cleaned up")
	}
}

func TestPITRManagerCreateRecoveryPoint(t *testing.T) {
	config := DefaultPITRConfig()
	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	point, err := pm.CreateRecoveryPoint("test_point")
	if err != nil {
		t.Fatalf("Failed to create recovery point: %v", err)
	}

	if point.Label != "test_point" {
		t.Errorf("Expected label 'test_point', got '%s'", point.Label)
	}

	if point.LSN != wal.LSN() {
		t.Error("Expected LSN to match current WAL LSN")
	}
}

func TestCalculateChecksum(t *testing.T) {
	data1 := []byte("test data")
	data2 := []byte("test data")
	data3 := []byte("different data")

	sum1 := calculateChecksum(data1)
	sum2 := calculateChecksum(data2)
	sum3 := calculateChecksum(data3)

	if sum1 != sum2 {
		t.Error("Expected same data to have same checksum")
	}

	if sum1 == sum3 {
		t.Error("Expected different data to have different checksum")
	}
}

func TestDefaultPITRConfig(t *testing.T) {
	config := DefaultPITRConfig()

	if !config.Enabled {
		t.Error("Expected enabled by default")
	}

	if config.ArchiveDir != "archive" {
		t.Errorf("Expected archive dir 'archive', got '%s'", config.ArchiveDir)
	}

	if config.RetentionDays != 7 {
		t.Errorf("Expected retention 7 days, got %d", config.RetentionDays)
	}

	if config.CompressionType != "gzip" {
		t.Errorf("Expected compression 'gzip', got '%s'", config.CompressionType)
	}

	if !config.VerifyChecksums {
		t.Error("Expected checksum verification enabled by default")
	}
}

func TestPITRManagerNilConfig(t *testing.T) {
	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, nil)

	if pm.config == nil {
		t.Fatal("Expected config to be set")
	}

	if !pm.config.Enabled {
		t.Error("Expected enabled by default")
	}
}

func TestArchiveStatus(t *testing.T) {
	config := DefaultPITRConfig()
	wal := &WAL{}
	backend := NewMemory()
	pm := NewPITRManager(wal, backend, config)

	// Initial status
	status := pm.GetArchiveStatus()
	if status.Enabled != true {
		t.Error("Expected archive to be enabled")
	}
	if status.SuccessCount != 0 {
		t.Error("Expected 0 initial successes")
	}
	if status.ErrorCount != 0 {
		t.Error("Expected 0 initial errors")
	}
}

func TestBackupManagerBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "backup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &BackupConfig{
		BackupDir:       tmpDir,
		ConcurrentJobs:  1,
		CompressionType: "none",
		VerifyChecksums: false,
	}

	wal := &WAL{}
	backend := NewMemory()
	pitr := NewPITRManager(wal, backend, nil)
	bm := NewBackupManager(backend, wal, pitr, config)

	if bm == nil {
		t.Fatal("Expected backup manager to be created")
	}

	if bm.config.BackupDir != tmpDir {
		t.Errorf("Expected backup dir %s, got %s", tmpDir, bm.config.BackupDir)
	}
}

func TestBackupManagerDeleteBackup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "backup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &BackupConfig{
		BackupDir: tmpDir,
	}

	wal := &WAL{}
	backend := NewMemory()
	pitr := NewPITRManager(wal, backend, nil)
	bm := NewBackupManager(backend, wal, pitr, config)

	// Create backup directory
	backupDir := tmpDir + "/test_backup"
	os.MkdirAll(backupDir, 0755)
	os.WriteFile(backupDir+"/data.txt", []byte("test"), 0644)

	// Delete backup
	err = bm.DeleteBackup("test_backup")
	if err != nil {
		t.Fatalf("Failed to delete backup: %v", err)
	}

	// Verify deletion
	_, err = os.Stat(backupDir)
	if !os.IsNotExist(err) {
		t.Error("Expected backup directory to be deleted")
	}
}
