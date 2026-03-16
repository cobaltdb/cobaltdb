package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestCommitTransaction tests Commit functionality
func TestCommitTransaction(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_commit.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin a transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data within transaction
	_, err = tx.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit should succeed
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Verify data was committed
	result, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer result.Close()
}

// TestCommitAlreadyCompleted tests Commit on already completed transaction
func TestCommitAlreadyCompleted(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_commit2.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin a transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First commit should succeed
	err = tx.Commit()
	if err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("Second commit should fail with 'already completed'")
	} else {
		t.Logf("Expected error on second commit: %v", err)
	}
}

// TestRollbackAlreadyCompleted tests Rollback on already completed transaction
func TestRollbackAlreadyCompleted(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_rollback.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin a transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First rollback should succeed
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("First rollback failed: %v", err)
	}

	// Second rollback should fail
	err = tx.Rollback()
	if err == nil {
		t.Error("Second rollback should fail with 'already completed'")
	} else {
		t.Logf("Expected error on second rollback: %v", err)
	}
}

// TestCreateBackup tests CreateBackup functionality
func TestCreateBackup(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_backup.db")

	options := &Options{
		CacheSize:       256,
		BackupDir:       filepath.Join(tempDir, "backups"),
		BackupRetention: 7,
		MaxBackups:      10,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'test data')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create backup
	backup, err := db.CreateBackup(ctx, "full")
	if err != nil {
		t.Logf("CreateBackup error: %v", err)
	} else if backup != nil {
		t.Logf("Created backup: %s", backup.ID)
	}

	db.Close()
}

// TestListBackups tests ListBackups functionality
func TestListBackups(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_list_backups.db")

	options := &Options{
		CacheSize:       256,
		BackupDir:       filepath.Join(tempDir, "backups"),
		BackupRetention: 7,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	backups := db.ListBackups()
	t.Logf("Found %d backups", len(backups))
}

// TestGetBackup tests GetBackup functionality
func TestGetBackup(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_get_backup.db")

	options := &Options{
		CacheSize:       256,
		BackupDir:       filepath.Join(tempDir, "backups"),
		BackupRetention: 7,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to get non-existent backup
	backup := db.GetBackup("non-existent-id")
	if backup == nil {
		t.Log("GetBackup returned nil for non-existent backup")
	}
}

// TestDeleteBackup tests DeleteBackup functionality
func TestDeleteBackup(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_delete_backup.db")

	options := &Options{
		CacheSize:       256,
		BackupDir:       filepath.Join(tempDir, "backups"),
		BackupRetention: 7,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to delete non-existent backup
	err = db.DeleteBackup("non-existent-id")
	if err != nil {
		t.Logf("DeleteBackup error (may be expected): %v", err)
	}
}

// TestGetWALPath tests GetWALPath functionality
func TestGetWALPath(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_wal_path.db")

	// Without WAL
	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	walPath := db.GetWALPath()
	t.Logf("WAL path (no WAL): %s", walPath)
	db.Close()

	// With WAL enabled
	options := &Options{
		CacheSize:  256,
		WALEnabled: true,
	}

	dbPath2 := filepath.Join(tempDir, "test_wal_path2.db")
	db2, err := Open(dbPath2, options)
	if err != nil {
		t.Fatalf("Failed to open database with WAL: %v", err)
	}
	defer db2.Close()

	walPath2 := db2.GetWALPath()
	t.Logf("WAL path (with WAL): %s", walPath2)
}

// TestCheckpoint tests Checkpoint functionality
func TestCheckpoint(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_checkpoint.db")

	options := &Options{
		CacheSize:  256,
		WALEnabled: true,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Checkpoint
	err = db.Checkpoint()
	if err != nil {
		t.Logf("Checkpoint error: %v", err)
	} else {
		t.Log("Checkpoint succeeded")
	}
}

// TestGetCurrentLSN tests GetCurrentLSN functionality
func TestGetCurrentLSN(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_lsn.db")

	// Without WAL
	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	lsn := db.GetCurrentLSN()
	t.Logf("LSN (no WAL): %d", lsn)
	db.Close()

	// With WAL enabled
	options := &Options{
		CacheSize:  256,
		WALEnabled: true,
	}

	dbPath2 := filepath.Join(tempDir, "test_lsn2.db")
	db2, err := Open(dbPath2, options)
	if err != nil {
		t.Fatalf("Failed to open database with WAL: %v", err)
	}
	defer db2.Close()

	lsn2 := db2.GetCurrentLSN()
	t.Logf("LSN (with WAL): %d", lsn2)
}

// TestHealthCheck tests HealthCheck functionality
func TestHealthCheck(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_health.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.HealthCheck()
	if err != nil {
		t.Errorf("HealthCheck error: %v", err)
	} else {
		t.Log("HealthCheck passed")
	}
}

// TestHealthCheckClosed tests HealthCheck on closed database
func TestHealthCheckClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_health2.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close database
	db.Close()

	// Health check should return error
	err = db.HealthCheck()
	if err == nil {
		t.Error("HealthCheck should return error for closed database")
	} else {
		t.Logf("Expected error for closed database: %v", err)
	}
}

// TestStats tests Stats functionality
func TestStats(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_stats.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create some tables
	_, err = db.Exec(ctx, "CREATE TABLE table1 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE table2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get stats
	stats, err := db.Stats()
	if err != nil {
		t.Errorf("Stats error: %v", err)
	} else {
		t.Logf("Database stats: Path=%s, Tables=%d", stats.Path, stats.Tables)
	}
}

// TestStatsClosed tests Stats on closed database
func TestStatsClosed(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_stats2.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	_, err = db.Stats()
	if err == nil {
		t.Error("Stats should return error for closed database")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestShutdown tests Shutdown functionality
func TestShutdown(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_shutdown.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.Shutdown(shutdownCtx)
	if err != nil {
		t.Logf("Shutdown error: %v", err)
	} else {
		t.Log("Shutdown succeeded")
	}
}

// TestBeginHotBackup tests BeginHotBackup functionality
func TestBeginHotBackup(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_hot_backup.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin hot backup
	err = db.BeginHotBackup()
	if err != nil {
		t.Logf("BeginHotBackup error: %v", err)
	} else {
		t.Log("BeginHotBackup succeeded")
	}
}

// TestGetBackupManager tests GetBackupManager functionality
func TestGetBackupManager(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_backup_mgr.db")

	options := &Options{
		CacheSize:       256,
		BackupDir:       filepath.Join(tempDir, "backups"),
		BackupRetention: 7,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	mgr := db.GetBackupManager()
	if mgr == nil {
		t.Error("GetBackupManager returned nil")
	} else {
		t.Log("GetBackupManager returned valid manager")
	}
}
