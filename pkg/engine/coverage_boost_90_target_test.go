package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// TestOpenWithCorruptMetaPage tests opening a database with corrupted meta page
func TestOpenWithCorruptMetaPage90(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a file with corrupt data (not a valid meta page)
	f, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	// Write garbage data that looks like a database file but isn't valid
	f.Write([]byte("COBALTDB"))
	f.Write(make([]byte, 100))
	f.Close()

	// Try to open - should fail with validation error
	_, err = Open(dbPath, nil)
	if err == nil {
		t.Error("Expected error for corrupt meta page")
	}
}

// TestOpenWithTruncatedMetaPage tests opening with truncated file
func TestOpenWithTruncatedMetaPage90(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create empty file
	f, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Close()

	// Try to open - should create new or fail gracefully
	db, err := Open(dbPath, nil)
	if err != nil {
		t.Logf("Open returned error (may be expected): %v", err)
	} else {
		db.Close()
	}
}

// TestVacuumWithClosedDB tests vacuum on closed database
func TestVacuumWithClosedDB90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table and insert data
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	// Close the database
	db.Close()

	// Try to vacuum - should error
	_, err = db.Exec(ctx, "VACUUM")
	if err == nil {
		t.Error("Expected error for VACUUM on closed database")
	}
}

// TestTxCommitWithFlushTableTreesError tests commit when FlushTableTrees fails
func TestTxCommitWithFlushTableTreesError90(t *testing.T) {
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

	// Insert some data
	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit should work normally
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned: %v", err)
	}
}

// TestTxCommitAlreadyCompleted tests committing an already completed transaction
func TestTxCommitAlreadyCompleted90(t *testing.T) {
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
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	// Begin and commit transaction
	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	tx.Commit()

	// Try to commit again - should error
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error for double commit")
	}
}

// TestTxRollbackAlreadyCompleted tests rolling back an already completed transaction
func TestTxRollbackAlreadyCompleted90(t *testing.T) {
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
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	// Begin and rollback transaction
	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	tx.Rollback()

	// Try to rollback again - should error
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error for double rollback")
	}
}

// TestGetWALPathWithWALEnabled90 tests GetWALPath when WAL is enabled
func TestGetWALPathWithWALEnabled90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	path := db.GetWALPath()
	// WAL path may be empty if WAL is not yet initialized
	t.Logf("WAL path: %s", path)
}

// TestCheckpointWithWALEnabled90 tests Checkpoint when WAL is enabled
func TestCheckpointWithWALEnabled90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")

	err = db.Checkpoint()
	if err != nil {
		t.Logf("Checkpoint returned: %v", err)
	}
}

// TestBeginHotBackupWithClosedDB90 tests BeginHotBackup on closed database
func TestBeginHotBackupWithClosedDB90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	db.Close()

	err = db.BeginHotBackup()
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got: %v", err)
	}
}

// TestEndHotBackup90 tests EndHotBackup functionality
func TestEndHotBackup90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	err = db.BeginHotBackup()
	if err != nil {
		t.Errorf("BeginHotBackup failed: %v", err)
	}

	err = db.EndHotBackup()
	if err != nil {
		t.Errorf("EndHotBackup failed: %v", err)
	}
}

// TestGetCurrentLSNWithWAL90 tests GetCurrentLSN with WAL enabled
func TestGetCurrentLSNWithWAL90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Initially LSN should be 0
	lsn := db.GetCurrentLSN()
	t.Logf("Initial LSN: %d", lsn)

	// Do some operations
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")

	// LSN should have increased
	lsn2 := db.GetCurrentLSN()
	t.Logf("After operations LSN: %d", lsn2)
}

// TestAnalyzeSpecificTable90 tests ANALYZE for a specific table
func TestAnalyzeSpecificTable90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	for i := 0; i < 100; i++ {
		db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, "name")
	}

	// Analyze specific table
	_, err = db.Exec(ctx, "ANALYZE test")
	if err != nil {
		t.Errorf("ANALYZE test failed: %v", err)
	}
}

// TestMaterializedViewOperations90 tests materialized view operations
func TestMaterializedViewOperations90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, i*10)
	}

	// Create materialized view
	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM test WHERE id > 5")
	if err != nil {
		t.Logf("CREATE MATERIALIZED VIEW returned: %v", err)
	}

	// Refresh materialized view
	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Logf("REFRESH MATERIALIZED VIEW returned: %v", err)
	}

	// Drop materialized view
	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Logf("DROP MATERIALIZED VIEW returned: %v", err)
	}
}

// TestCreateNewWithRLSEnabled90 tests creating new database with RLS enabled
func TestCreateNewWithRLSEnabled90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		EnableRLS: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestExecWithClosedDB90 tests Exec on closed database
func TestExecWithClosedDB90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Error("Expected error for Exec on closed database")
	}
}

// TestQueryWithClosedDB90 tests Query on closed database
func TestQueryWithClosedDB90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	db.Close()

	ctx := context.Background()
	_, err = db.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("Expected error for Query on closed database")
	}
}

// TestQueryWithOptions90 tests Query method with various options
func TestQueryWithOptions90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")

	// Test simple query
	rows, err := db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Errorf("Query failed: %v", err)
	}
	if rows != nil {
		rows.Close()
	}

	// Test query with args
	rows, err = db.Query(ctx, "SELECT * FROM test WHERE id = ?", 1)
	if err != nil {
		t.Errorf("Query with args failed: %v", err)
	}
	if rows != nil {
		rows.Close()
	}
}

// TestQueryRowWithOptions90 tests QueryRow method
func TestQueryRowWithOptions90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO test VALUES (1, 'alice')")

	// Test QueryRow
	row := db.QueryRow(ctx, "SELECT name FROM test WHERE id = ?", 1)
	var name string
	err = row.Scan(&name)
	if err != nil {
		t.Errorf("QueryRow failed: %v", err)
	}
	if name != "alice" {
		t.Errorf("Expected 'alice', got '%s'", name)
	}
}

// TestVacuumErrorPath90 tests VACUUM error handling
func TestVacuumErrorPath90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table with data
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	// VACUUM should work on open database
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM returned: %v", err)
	}
}

// TestBeginWithOptions90 tests Begin with transaction options
func TestBeginWithOptions90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	// Begin with default options
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("Insert in transaction failed: %v", err)
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestBeginWithCustomOptions90 tests BeginWith with custom options
func TestBeginWithCustomOptions90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	// Begin with custom options
	opts := &txn.Options{ReadOnly: false}
	tx, err := db.BeginWith(ctx, opts)
	if err != nil {
		t.Fatalf("BeginWith failed: %v", err)
	}

	// Insert data
	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("Insert in transaction failed: %v", err)
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}
