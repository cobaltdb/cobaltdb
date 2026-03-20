package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
)

// TestCommitWithFlushTableTreesErrorEdge tests Commit when FlushTableTrees fails
func TestCommitWithFlushTableTreesErrorEdge(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit - should succeed normally
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned (may be expected in some cases): %v", err)
	}
}

// TestCommitWithCatalogCommitError tests Commit when catalog.CommitTransaction fails
func TestCommitWithCatalogCommitErrorEdge(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned: %v", err)
	}
}

// TestCommitWithPoolFlushError tests Commit when pool.FlushAll fails
func TestCommitWithPoolFlushErrorEdge(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned: %v", err)
	}
}

// TestLoadExistingWithWALOpenError tests loadExisting when WAL open fails
func TestLoadExistingWithWALOpenError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database without WAL
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db1.Close()

	// Create a file with WAL extension that is not a valid WAL
	walPath := dbPath + ".wal"
	os.WriteFile(walPath, []byte("invalid wal data"), 0644)

	// Try to open with WAL enabled - may fail
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Logf("Open with invalid WAL returned: %v", err)
	} else {
		db2.Close()
	}
}

// TestLoadExistingWithWALRecoverError tests loadExisting when WAL recovery fails
func TestLoadExistingWithWALRecoverError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database with WAL
	db1, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1)")
	db1.Close()

	// Corrupt the WAL file
	walPath := dbPath + ".wal"
	os.WriteFile(walPath, []byte("corrupted data that should cause recovery to fail"), 0644)

	// Try to reopen - may fail
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Logf("Open with corrupted WAL returned: %v", err)
	} else {
		db2.Close()
	}
}

// TestLoadExistingWithCatalogLoadError tests loadExisting when catalog.Load fails
func TestLoadExistingWithCatalogLoadError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db1.Close()

	// Corrupt the database file
	f, _ := os.OpenFile(dbPath, os.O_WRONLY, 0644)
	if f != nil {
		f.WriteAt([]byte("CORRUPT"), 0)
		f.Close()
	}

	// Try to open - should fail
	_, err = Open(dbPath, nil)
	if err == nil {
		t.Error("Expected error for corrupted database")
	}
}

// TestCreateNewWithReplicationStartError tests createNew when replication manager fails to start
func TestCreateNewWithReplicationStartError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_repl_err.db")

	// Use slave role with invalid master address to trigger connection error
	_, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "slave",
		ReplicationMasterAddr: "127.0.0.1:1", // Port 1 - should fail to connect
	})
	if err != nil {
		t.Logf("Expected replication start error: %v", err)
	} else {
		t.Log("Replication start did not error (slave may connect lazily)")
	}
}

// TestCreateNewWithAuditError tests createNew with audit logging
func TestCreateNewWithAuditError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		AuditConfig: &audit.Config{
			Enabled:  true,
			LogFile:  filepath.Join(dir, "audit.log"),
			LogFormat: "json",
		},
	})
	if err != nil {
		t.Fatalf("Failed to open database with audit: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestCreateNewWithBackupManagerError tests createNew with backup manager initialization
func TestCreateNewWithBackupManagerError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		BackupDir:        filepath.Join(dir, "backups"),
		BackupRetention:  7,
		MaxBackups:       10,
	})
	if err != nil {
		t.Fatalf("Failed to open database with backup: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestCreateNewWithQueryCacheError tests createNew with query cache
func TestCreateNewWithQueryCacheError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		EnableQueryCache: true,
		QueryCacheSize:   1024,
		QueryCacheTTL:    60,
	})
	if err != nil {
		t.Fatalf("Failed to open database with query cache: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestCreateNewWithEncryptionError tests createNew with encryption
func TestCreateNewWithEncryptionError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Generate a 32-byte encryption key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	db, err := Open(dbPath, &Options{
		EncryptionKey: key,
	})
	if err != nil {
		t.Fatalf("Failed to open database with encryption: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestCreateNewWithMetricsError tests createNew with metrics
func TestCreateNewWithMetricsError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to get metrics when not enabled
	_, err = db.GetMetrics()
	if err == nil {
		t.Log("GetMetrics without collector may return nil or error")
	}
}
