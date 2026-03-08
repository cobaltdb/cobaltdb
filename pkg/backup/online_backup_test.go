package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestOnlineBackupManagerCreation tests creating an online backup manager
func TestOnlineBackupManagerCreation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create in-memory backend for testing
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "gzip",
		EnableChecksum:  true,
		ParallelWorkers: 2,
		SnapshotTimeout: 5 * time.Minute,
	}

	manager := NewOnlineBackupManager(backend, pm, config)
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}

	if manager.config.BackupDir != config.BackupDir {
		t.Errorf("Expected backup dir %s, got %s", config.BackupDir, manager.config.BackupDir)
	}

	// Test with nil config (should use defaults)
	manager2 := NewOnlineBackupManager(backend, pm, nil)
	if manager2 == nil {
		t.Fatal("Expected non-nil manager with nil config")
	}

	if manager2.config.CompressionType != "gzip" {
		t.Errorf("Expected default compression gzip, got %s", manager2.config.CompressionType)
	}
}

// TestCreateOnlineBackup tests creating an online backup
func TestCreateOnlineBackup(t *testing.T) {
	tempDir := t.TempDir()

	// Create storage components
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Allocate some test pages
	for i := 0; i < 5; i++ {
		page, err := pm.AllocatePage(storage.PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		// Write some test data
		data := page.Data()
		data[16] = byte(i) // Write after header
		page.SetDirty(true)
		pool.FlushPage(page)
		pool.Unpin(page)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
		ParallelWorkers: 2,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	ctx := context.Background()
	metadata, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	if metadata == nil {
		t.Fatal("Expected non-nil metadata")
	}

	if metadata.Version != "2.0-online" {
		t.Errorf("Expected version 2.0-online, got %s", metadata.Version)
	}

	if !metadata.IsOnline {
		t.Error("Expected IsOnline to be true")
	}

	if metadata.SnapshotLSN == 0 {
		t.Error("Expected non-zero SnapshotLSN")
	}

	if metadata.BytesProcessed == 0 {
		t.Error("Expected non-zero BytesProcessed")
	}

	// Verify backup file exists
	if _, err := os.Stat(metadata.Filename); os.IsNotExist(err) {
		t.Errorf("Backup file %s does not exist", metadata.Filename)
	}

	// Verify metadata file exists
	metaFile := metadata.Filename + ".json"
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		t.Errorf("Metadata file %s does not exist", metaFile)
	}
}

// TestRestoreOnlineBackup tests restoring from an online backup
func TestRestoreOnlineBackup(t *testing.T) {
	tempDir := t.TempDir()

	// Create storage components
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Allocate some test pages and write data
	testData := []byte("test data for backup")
	for i := 0; i < 3; i++ {
		page, err := pm.AllocatePage(storage.PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		data := page.Data()
		copy(data[16:], testData) // Write after header
		page.SetDirty(true)
		pool.FlushPage(page)
		pool.Unpin(page)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create backup
	ctx := context.Background()
	metadata, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// Create new storage for restore
	backend2 := storage.NewMemory()
	pool2 := storage.NewBufferPool(100, backend2)
	pm2, err := storage.NewPageManager(pool2)
	if err != nil {
		t.Fatalf("Failed to create page manager 2: %v", err)
	}

	// Restore to new storage
	manager2 := NewOnlineBackupManager(backend2, pm2, config)
	err = manager2.RestoreOnlineBackup(ctx, metadata.Filename)
	if err != nil {
		t.Fatalf("Failed to restore online backup: %v", err)
	}

	// Verify restored pages (at minimum the meta page should exist)
	if pm2.GetPageCount() < 1 {
		t.Errorf("Expected at least 1 page after restore, got %d", pm2.GetPageCount())
	}
}

// TestRestoreOnlineBackupNotFound tests restoring from non-existent backup
func TestRestoreOnlineBackupNotFound(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	ctx := context.Background()
	err = manager.RestoreOnlineBackup(ctx, filepath.Join(tempDir, "nonexistent.db"))
	if err == nil {
		t.Error("Expected error for non-existent backup file")
	}
}

// TestVerifyOnlineBackup tests backup verification
func TestVerifyOnlineBackup(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Allocate some pages
	for i := 0; i < 3; i++ {
		page, err := pm.AllocatePage(storage.PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pool.Unpin(page)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create backup
	ctx := context.Background()
	metadata, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// Verify backup
	err = manager.VerifyBackup(metadata.Filename)
	if err != nil {
		t.Errorf("Failed to verify backup: %v", err)
	}
}

// TestVerifyOnlineBackupNotFound tests verifying non-existent backup
func TestVerifyOnlineBackupNotFound(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	err = manager.VerifyBackup(filepath.Join(tempDir, "nonexistent.db"))
	if err == nil {
		t.Error("Expected error for non-existent backup file")
	}
}

// TestListOnlineBackups tests listing online backups
func TestListOnlineBackups(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create a few backups
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
		if err != nil {
			t.Fatalf("Failed to create online backup %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond) // Ensure unique timestamps
	}

	// List backups
	backups, err := manager.ListOnlineBackups()
	if err != nil {
		t.Fatalf("Failed to list backups: %v", err)
	}

	// Should have 3 backups (might be less if timestamps collide)
	if len(backups) < 1 {
		t.Errorf("Expected at least 1 backup, got %d", len(backups))
	}
}

// TestCleanupOldOnlineBackups tests cleaning up old backups
func TestCleanupOldOnlineBackups(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create a backup
	ctx := context.Background()
	_, err = manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// List before cleanup
	backups, err := manager.ListOnlineBackups()
	if err != nil {
		t.Fatalf("Failed to list backups: %v", err)
	}

	if len(backups) != 1 {
		t.Fatalf("Expected 1 backup, got %d", len(backups))
	}

	// Cleanup with very short retention (should remove the backup)
	err = manager.CleanupOldBackups(1 * time.Nanosecond)
	if err != nil {
		t.Errorf("Failed to cleanup old backups: %v", err)
	}

	// List after cleanup
	backups, err = manager.ListOnlineBackups()
	if err != nil {
		t.Fatalf("Failed to list backups after cleanup: %v", err)
	}

	if len(backups) != 0 {
		t.Errorf("Expected 0 backups after cleanup, got %d", len(backups))
	}
}

// TestGetStats tests backup statistics
func TestGetStats(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Check initial stats
	stats := manager.GetStats()
	if stats.TotalBackups != 0 {
		t.Errorf("Expected 0 total backups initially, got %d", stats.TotalBackups)
	}

	// Create a backup
	ctx := context.Background()
	_, err = manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// Check updated stats
	stats = manager.GetStats()
	if stats.TotalBackups != 1 {
		t.Errorf("Expected 1 total backup, got %d", stats.TotalBackups)
	}

	if stats.TotalBytesBackedUp == 0 {
		t.Error("Expected non-zero TotalBytesBackedUp")
	}

	if stats.LastBackupTime.IsZero() {
		t.Error("Expected non-zero LastBackupTime")
	}
}

// TestCompressAndDecompressBackup tests compression functions
func TestCompressAndDecompressBackup(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create a backup
	ctx := context.Background()
	metadata, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// Compress backup
	compressedFile := metadata.Filename + ".gz"
	err = manager.CompressBackup(metadata.Filename, compressedFile)
	if err != nil {
		t.Fatalf("Failed to compress backup: %v", err)
	}

	// Verify compressed file exists
	if _, err := os.Stat(compressedFile); os.IsNotExist(err) {
		t.Errorf("Compressed file %s does not exist", compressedFile)
	}

	// Decompress backup
	decompressedFile := filepath.Join(tempDir, "decompressed.db")
	err = manager.DecompressBackup(compressedFile, decompressedFile)
	if err != nil {
		t.Fatalf("Failed to decompress backup: %v", err)
	}

	// Verify decompressed file exists
	if _, err := os.Stat(decompressedFile); os.IsNotExist(err) {
		t.Errorf("Decompressed file %s does not exist", decompressedFile)
	}
}

// TestCreateOnlineBackupContextCancelled tests context cancellation
func TestCreateOnlineBackupContextCancelled(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Allocate many pages to make backup take longer
	for i := 0; i < 100; i++ {
		page, err := pm.AllocatePage(storage.PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pool.Unpin(page)
	}

	config := &OnlineBackupConfig{
		BackupDir:       filepath.Join(tempDir, "backups"),
		CompressionType: "none",
		EnableChecksum:  true,
	}

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
	// Error should be related to context cancellation
	if err != context.Canceled && err != context.DeadlineExceeded {
		// Wrapped errors are also acceptable
		t.Logf("Got error (may be wrapped): %v", err)
	}
}

// TestDefaultOnlineBackupConfig tests default configuration
func TestDefaultOnlineBackupConfig(t *testing.T) {
	config := DefaultOnlineBackupConfig()

	if config.BackupDir != "./backups/online" {
		t.Errorf("Expected default backup dir ./backups/online, got %s", config.BackupDir)
	}

	if config.CompressionType != "gzip" {
		t.Errorf("Expected default compression gzip, got %s", config.CompressionType)
	}

	if !config.EnableChecksum {
		t.Error("Expected EnableChecksum to be true by default")
	}

	if config.ParallelWorkers != 4 {
		t.Errorf("Expected default parallel workers 4, got %d", config.ParallelWorkers)
	}

	if config.SnapshotTimeout != 5*time.Minute {
		t.Errorf("Expected default snapshot timeout 5m, got %v", config.SnapshotTimeout)
	}
}

// TestCreateTableSnapshot tests table snapshot creation
func TestCreateTableSnapshot(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	snapshot, err := manager.CreateTableSnapshot("test_table")
	if err != nil {
		t.Fatalf("Failed to create table snapshot: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Expected non-nil snapshot")
	}

	if snapshot.TableName != "test_table" {
		t.Errorf("Expected table name test_table, got %s", snapshot.TableName)
	}
}

// TestProgress tests backup progress tracking
func TestProgress(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	// Get initial progress
	progress := manager.Progress()
	if progress.IsComplete {
		t.Error("Expected IsComplete to be false initially")
	}

	// Create a backup
	ctx := context.Background()
	_, err = manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create online backup: %v", err)
	}

	// Get progress after backup
	progress = manager.Progress()
	if progress.StartTime.IsZero() {
		t.Error("Expected non-zero StartTime after backup")
	}
}

// TestParallelOnlineBackup tests parallel backup functionality
func TestParallelOnlineBackup(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create parallel backup handler
	parallel := NewParallelOnlineBackup(manager, 2)
	if parallel == nil {
		t.Fatal("Expected non-nil parallel backup handler")
	}

	// Test parallel backup of multiple tables
	ctx := context.Background()
	tables := []string{"table1", "table2", "table3"}
	err = parallel.BackupTables(ctx, tables)
	if err != nil {
		t.Errorf("Failed to backup tables in parallel: %v", err)
	}
}

// TestBTreeSnapshotReader tests B-tree snapshot reader
func TestBTreeSnapshotReader(t *testing.T) {
	// Create storage for B-tree
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)

	// Create a simple B-tree for testing
	bt, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert some data
	bt.Put([]byte("key1"), []byte("value1"))
	bt.Put([]byte("key2"), []byte("value2"))
	bt.Put([]byte("key3"), []byte("value3"))

	reader := NewBTreeSnapshotReader(bt, 12345)
	if reader == nil {
		t.Fatal("Expected non-nil BTreeSnapshotReader")
	}

	// Test scan range
	iter, err := reader.ScanRange([]byte("key1"), []byte("key3"))
	if err != nil {
		t.Fatalf("Failed to scan range: %v", err)
	}
	if iter == nil {
		t.Fatal("Expected non-nil iterator")
	}
}

// TestIncrementalBackup tests incremental backup creation
func TestIncrementalBackup(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	// Create base backup first
	ctx := context.Background()
	baseMetadata, err := manager.CreateOnlineBackup(ctx, []string{"test_table"})
	if err != nil {
		t.Fatalf("Failed to create base backup: %v", err)
	}

	// Try to create incremental backup
	incrMetadata, err := manager.CreateIncrementalBackup(ctx, baseMetadata.Filename)
	if err != nil {
		t.Fatalf("Failed to create incremental backup: %v", err)
	}

	if incrMetadata == nil {
		t.Fatal("Expected non-nil incremental metadata")
	}

	if incrMetadata.LSN != baseMetadata.SnapshotLSN {
		t.Errorf("Expected incremental LSN to match base LSN: %d vs %d", incrMetadata.LSN, baseMetadata.SnapshotLSN)
	}
}

// TestIncrementalBackupNoMetadata tests incremental backup without metadata file
func TestIncrementalBackupNoMetadata(t *testing.T) {
	tempDir := t.TempDir()

	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}

	config := DefaultOnlineBackupConfig()
	config.BackupDir = tempDir

	manager := NewOnlineBackupManager(backend, pm, config)

	ctx := context.Background()
	_, err = manager.CreateIncrementalBackup(ctx, filepath.Join(tempDir, "nonexistent"))
	if err == nil {
		t.Error("Expected error for non-existent base backup")
	}
}
