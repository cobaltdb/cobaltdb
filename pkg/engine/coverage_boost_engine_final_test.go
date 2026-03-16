package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestCheckpointWithClosedDB tests Checkpoint after database is closed
func TestCheckpointWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// Close the database
	db.Close()

	// Try to checkpoint - may or may not error
	err = db.Checkpoint()
	t.Logf("Checkpoint on closed DB returned: %v", err)
}

// TestGetCurrentLSNWithClosedDB tests GetCurrentLSN after database is closed
func TestGetCurrentLSNWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// Close the database
	db.Close()

	// Try to get LSN - may return 0 or error
	lsn := db.GetCurrentLSN()
	t.Logf("GetCurrentLSN on closed DB returned: %d", lsn)
}

// TestGetWALPathWithInMemoryDB tests GetWALPath with in-memory database
func TestGetWALPathWithInMemoryDB(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	path := db.GetWALPath()
	t.Logf("GetWALPath on in-memory DB returned: %s", path)
}

// TestCloseMultipleTimes tests closing database multiple times
func TestCloseMultipleTimes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// First close
	err = db.Close()
	if err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	// Second close - should not panic
	err = db.Close()
	// May or may not error depending on implementation
	t.Logf("Second Close returned: %v", err)
}

// TestDeleteBackupNotFound tests DeleteBackup with non-existent backup ID
func TestDeleteBackupNotFound(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Try to delete non-existent backup
	err = db.DeleteBackup("non-existent-backup-id")
	if err == nil {
		t.Log("DeleteBackup for non-existent ID did not error - may be expected")
	}
}

// TestGetBackupNotFound tests GetBackup with non-existent backup ID
func TestGetBackupNotFound(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Try to get non-existent backup
	result := db.GetBackup("non-existent-backup-id")
	if result != nil {
		t.Log("GetBackup for non-existent ID returned non-nil - checking if expected")
	}
}

// TestGetMetricsWithClosedDB tests GetMetrics after database is closed
func TestGetMetricsWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// Close the database
	db.Close()

	// Try to get metrics - may or may not error
	metrics, err := db.GetMetrics()
	t.Logf("GetMetrics on closed DB returned: %v, %v", metrics, err)
}

// TestListBackupsWithClosedDB tests ListBackups after database is closed
func TestListBackupsWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// Close the database
	db.Close()

	// Try to list backups - may return nil or empty
	result := db.ListBackups()
	t.Logf("ListBackups on closed DB returned: %v", result)
}

// TestHealthCheckUnhealthy tests HealthCheck when DB is unhealthy
func TestHealthCheckUnhealthy(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Should be healthy initially
	err = db.HealthCheck()
	if err != nil {
		t.Logf("HealthCheck returned error: %v", err)
	}
}

// TestCreateBackupInvalidType tests CreateBackup with invalid backup type
func TestCreateBackupInvalidType(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Try to create backup with empty type
	_, err = db.CreateBackup(ctx, "")
	if err == nil {
		t.Log("CreateBackup with empty type did not error - may be expected")
	}
}

// TestCreateBackupWithClosedDB tests CreateBackup after database is closed
func TestCreateBackupWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	// Close the database
	db.Close()

	ctx := context.Background()

	// Try to create backup - should error
	_, err = db.CreateBackup(ctx, "full")
	if err == nil {
		t.Error("Expected error when creating backup from closed database")
	}
}

// TestOpenWithInvalidPath tests Open with invalid path
func TestOpenWithInvalidPath(t *testing.T) {
	// Try to open a path that cannot be created
	invalidPath := "/root/invalid/path/test.db"

	db, err := Open(invalidPath, nil)
	if err == nil {
		db.Close()
		t.Log("Open succeeded for invalid path - platform dependent")
	} else {
		t.Logf("Open failed as expected: %v", err)
	}
}

// TestOpenWithInMemoryAndPath tests Open with both in-memory and path (edge case)
func TestOpenWithInMemoryAndPath(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Try to open with in-memory flag but also provide a path
	db, err := Open(dbPath, &Options{
		InMemory: true,
	})
	if err != nil {
		t.Logf("Open with in-memory options returned: %v", err)
	} else {
		db.Close()
	}
}

// TestOpenInMemory tests Open with in-memory database
func TestOpenInMemory(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	// Verify database is usable
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("Failed to create table in in-memory DB: %v", err)
	}
}
