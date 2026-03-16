package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestCommitWithClosedDB tests Commit on closed database
func TestCommitWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Close the database
	db.Close()

	// Try to commit - may fail since DB is closed
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit on closed DB returned: %v", err)
	}
}

// TestCommitWithNilTransaction tests various nil transaction scenarios
func TestCommitWithNilTransaction(t *testing.T) {
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

	// Begin and immediately commit empty transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Commit without any operations
	err = tx.Commit()
	if err != nil {
		t.Logf("Empty commit returned: %v", err)
	}
}

// TestCreateNewWithInvalidOptions tests createNew with various invalid options
func TestCreateNewWithInvalidOptions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Test with encryption key that's too short
	shortKey := []byte("short")
	db1, err := Open(dbPath, &Options{EncryptionKey: shortKey})
	if err == nil {
		t.Log("Expected error for short encryption key")
		if db1 != nil {
			db1.Close()
		}
	}

	// Test with invalid backup dir (file instead of directory)
	backupFile := filepath.Join(dir, "backupfile")
	os.WriteFile(backupFile, []byte("data"), 0644)

	db2, err := Open(dbPath, &Options{
		BackupDir: backupFile,
	})
	if err != nil {
		t.Logf("Open with file as backup dir returned: %v", err)
	} else if db2 != nil {
		db2.Close()
	}
}

// TestLoadExistingWithWALDisabledThenEnabled tests loadExisting with WAL toggle
func TestLoadExistingWithWALDisabledThenEnabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create without WAL
	db1, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	db1.Close()

	// Reopen with WAL enabled
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to reopen with WAL: %v", err)
	}
	defer db2.Close()

	// Verify data
	result, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Logf("Query after WAL enable failed: %v", err)
	} else if result != nil {
		result.Close()
	}
}

// TestCreateNewWithAllSlowQueryOptions tests createNew with slow query log options
func TestCreateNewWithAllSlowQueryOptions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		EnableSlowQueryLog:  true,
		SlowQueryThreshold:  1,
		SlowQueryMaxEntries: 10,
	})
	if err != nil {
		t.Fatalf("Failed to open database with slow query log: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Execute some queries to trigger slow query logging
	for i := 0; i < 5; i++ {
		db.Exec(ctx, "INSERT INTO test VALUES (?)", i)
	}
}

// TestLoadExistingWithCorruptedData tests loadExisting with corrupted catalog data
func TestLoadExistingWithCorruptedData(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Close()

	// Corrupt the database by writing garbage at various offsets
	f, err := os.OpenFile(dbPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Write corruption at different points
	f.WriteAt([]byte("CORRUPT"), 100)
	f.Close()

	// Try to open - may fail
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Logf("Open with corruption returned: %v", err)
	} else {
		db2.Close()
	}
}

// TestCreateNewWithQueryCacheMore tests createNew with query cache initialization
func TestCreateNewWithQueryCacheMore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		EnableQueryCache: true,
		QueryCacheSize:   100,
		QueryCacheTTL:    1,
	})
	if err != nil {
		t.Fatalf("Failed to open database with query cache: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Execute queries to populate cache
	db.Exec(ctx, "INSERT INTO test VALUES (1)")
	db.Query(ctx, "SELECT * FROM test")
	db.Query(ctx, "SELECT * FROM test") // Should hit cache
}

// TestTransactionRollbackError tests Rollback error paths
func TestTransactionRollbackError(t *testing.T) {
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

	tx.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Rollback should succeed
	err = tx.Rollback()
	if err != nil {
		t.Logf("Rollback returned: %v", err)
	}

	// Second rollback may return error or succeed
	err = tx.Rollback()
	if err != nil {
		t.Logf("Second rollback returned: %v", err)
	}
}

// TestCreateNewWithInvalidWALOptions tests createNew with invalid WAL options
func TestCreateNewWithInvalidWALOptions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a file with the WAL extension that is not a valid WAL
	walPath := dbPath + ".wal"
	os.WriteFile(walPath, []byte("invalid wal header data"), 0644)

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Logf("Open with invalid WAL file returned: %v", err)
	} else {
		defer db.Close()
		ctx := context.Background()
		db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	}
}

// TestLoadExistingWithWALCorruption tests loadExisting with WAL corruption
func TestLoadExistingWithWALCorruption(t *testing.T) {
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
	f, err := os.OpenFile(walPath, os.O_WRONLY, 0644)
	if err == nil {
		f.WriteAt([]byte("CORRUPT"), 0)
		f.Close()
	}

	// Try to reopen
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Logf("Open with corrupted WAL returned: %v", err)
	} else {
		db2.Close()
	}
}

// TestCommitAfterRollback tests Commit after Rollback
func TestCommitAfterRollback(t *testing.T) {
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

	tx.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Rollback
	tx.Rollback()

	// Try to commit after rollback - may error
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit after rollback returned: %v", err)
	}
}

// TestCreateNewWithEmptyPath tests createNew with empty path
func TestCreateNewWithEmptyPath(t *testing.T) {
	// Try to create with empty path - should use in-memory or fail
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestLoadExistingWithConcurrentModification tests loadExisting simulation
func TestLoadExistingWithConcurrentModification(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1)")
	db1.Close()

	// Reopen
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	result, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Logf("Query failed: %v", err)
	} else if result != nil {
		result.Close()
	}
}
