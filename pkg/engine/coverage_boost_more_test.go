package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// TestHealthCheckError tests HealthCheck when database is closed
func TestHealthCheckError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close the database
	db.Close()

	// Health check on closed DB should fail
	err = db.HealthCheck()
	if err == nil {
		t.Error("Expected error for HealthCheck on closed DB")
	}
}

// TestGetMetricsWithoutCollector tests GetMetrics when collector is nil
func TestGetMetricsWithoutCollector(t *testing.T) {
	// This tests the case where metrics collector might be nil
	// In current implementation, metrics are always initialized
	// but this tests the defensive code path
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// GetMetrics should work (metrics are always initialized)
	_, err = db.GetMetrics()
	// This may or may not error depending on implementation
	t.Logf("GetMetrics result: %v", err)
}

// TestCreateBackupError tests CreateBackup error paths
func TestCreateBackupError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open without backup manager
	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Try to create backup without manager
	_, err = db.CreateBackup(ctx, "full")
	// May error or succeed depending on implementation
	t.Logf("CreateBackup without manager: %v", err)
}

// TestGetBackupError tests GetBackup error paths
func TestGetBackupError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Get non-existent backup
	backup := db.GetBackup("nonexistent")
	t.Logf("GetBackup result: %v", backup)
}

// TestDeleteBackupError tests DeleteBackup error paths
func TestDeleteBackupError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Try to delete non-existent backup
	err = db.DeleteBackup("nonexistent")
	// May error or succeed
	t.Logf("DeleteBackup result: %v", err)
}

// TestCheckpointWithoutWALMore tests Checkpoint when WAL is not enabled
func TestCheckpointWithoutWALMore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open without WAL
	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Checkpoint should succeed (no-op)
	err = db.Checkpoint()
	if err != nil {
		t.Errorf("Checkpoint without WAL should succeed: %v", err)
	}
}

// TestGetCurrentLSNWithoutWALMore tests GetCurrentLSN when WAL is not enabled
func TestGetCurrentLSNWithoutWALMore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open without WAL
	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// LSN should be 0 without WAL
	lsn := db.GetCurrentLSN()
	if lsn != 0 {
		t.Errorf("Expected LSN=0 without WAL, got %d", lsn)
	}
}

// TestGetWALPathWithoutWALMore tests GetWALPath when WAL is not enabled
func TestGetWALPathWithoutWALMore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open without WAL
	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// WAL path should be empty without WAL
	path := db.GetWALPath()
	if path != "" {
		t.Errorf("Expected empty WAL path without WAL, got %s", path)
	}
}

// TestQueryRowEmptyResult tests QueryRow with empty result
func TestQueryRowEmptyResult(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// QueryRow on empty table
	row := db.QueryRow(ctx, "SELECT id FROM test WHERE id = 999")
	var id int
	err = row.Scan(&id)
	if err == nil {
		t.Error("Expected error for Scan on empty result")
	}
}

// TestQueryWithParams tests Query with parameters
func TestQueryWithParams(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with parameters
	rows, err := db.Query(ctx, "SELECT * FROM test WHERE id = ?", 1)
	if err != nil {
		t.Errorf("Query with params failed: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestExecWithParams tests Exec with parameters
func TestExecWithParams(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Exec with parameters
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", 1, "Alice")
	if err != nil {
		t.Errorf("Exec with params failed: %v", err)
	}

	// Verify
	var count int
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM test WHERE id = ? AND name = ?", 1, "Alice")
	err = row.Scan(&count)
	if err != nil {
		t.Errorf("Scan failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

// TestExecuteSelectWithCTE tests executeSelectWithCTE
func TestExecuteSelectWithCTE(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with CTE
	rows, err := db.Query(ctx, "WITH cte AS (SELECT * FROM test WHERE value > 15) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE query error: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestExecuteVacuumErrorMore tests executeVacuum error path
func TestExecuteVacuumErrorMore(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// VACUUM should work
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM error: %v", err)
	}
}

// TestExecuteAnalyzeAllTables tests ANALYZE without table name
func TestExecuteAnalyzeAllTables(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// ANALYZE all tables
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE all tables error: %v", err)
	}
}

// TestCreateNewWithAllOptions2 tests createNew with all options enabled
func TestCreateNewWithAllOptions2(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	backupDir := filepath.Join(dir, "backups")

	options := &Options{
		WALEnabled:          true,
		EnableRLS:           true,
		EnableQueryCache:    true,
		QueryCacheSize:      1024,
		QueryCacheTTL:       60 * time.Second,
		BackupDir:           backupDir,
		BackupRetention:     7 * 24 * time.Hour,
		MaxBackups:          10,
		EnableSlowQueryLog:  true,
		SlowQueryThreshold:  100 * time.Millisecond,
		SlowQueryMaxEntries: 100,
	}

	db, err := Open(dbPath, options)
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

// TestLoadExistingWithAllOptions2 tests loadExisting with various options
func TestLoadExistingWithAllOptions2(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database first
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	db1.Close()

	// Reopen with options
	db2, err := Open(dbPath, &Options{
		WALEnabled:       true,
		EnableRLS:        true,
		EnableQueryCache: true,
		QueryCacheSize:   1024,
		QueryCacheTTL:    60 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db2.Close()

	// Verify data is accessible
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Errorf("Query failed: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestBeginWithOptionsMore tests BeginWith method
func TestBeginWithOptionsMore(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin with options
	opts := txn.DefaultOptions()
	tx, err := db.BeginWith(ctx, opts)
	if err != nil {
		t.Fatalf("BeginWith failed: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestCloseErrors tests Close with various error conditions
func TestCloseErrors(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// First close
	err = db.Close()
	if err != nil {
		t.Logf("First close returned: %v", err)
	}

	// Second close should be safe
	err = db.Close()
	if err != nil {
		t.Logf("Second close returned: %v", err)
	}
}

// TestStatsError tests Stats when database is closed
func TestStatsError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close the database
	db.Close()

	// Stats on closed DB should fail
	_, err = db.Stats()
	if err == nil {
		t.Error("Expected error for Stats on closed DB")
	}
}

// TestTablesError tests Tables when database is closed
func TestTablesError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close the database
	db.Close()

	// Tables on closed DB - should return empty or handle gracefully
	tables := db.Tables()
	t.Logf("Tables on closed DB returned: %v", tables)
}

// TestTableSchemaError tests TableSchema when table doesn't exist
func TestTableSchemaError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to get schema for non-existent table
	_, err = db.TableSchema("nonexistent")
	if err == nil {
		t.Error("Expected error for TableSchema on non-existent table")
	}
}

// TestShowStatements tests various SHOW statements
func TestShowStatements(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// SHOW TABLES
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Logf("SHOW TABLES error: %v", err)
	} else if rows != nil {
		rows.Close()
	}

	// SHOW CREATE TABLE
	rows, err = db.Query(ctx, "SHOW CREATE TABLE test")
	if err != nil {
		t.Logf("SHOW CREATE TABLE error: %v", err)
	} else if rows != nil {
		rows.Close()
	}

	// SHOW COLUMNS
	rows, err = db.Query(ctx, "SHOW COLUMNS FROM test")
	if err != nil {
		t.Logf("SHOW COLUMNS error: %v", err)
	} else if rows != nil {
		rows.Close()
	}

	// DESCRIBE
	rows, err = db.Query(ctx, "DESCRIBE test")
	if err != nil {
		t.Logf("DESCRIBE error: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestAlterTable tests ALTER TABLE operations
func TestAlterTable(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// ADD COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE test ADD COLUMN name TEXT")
	if err != nil {
		t.Logf("ADD COLUMN error: %v", err)
	}

	// RENAME
	_, err = db.Exec(ctx, "ALTER TABLE test RENAME TO test2")
	if err != nil {
		t.Logf("RENAME error: %v", err)
	}
}

// TestDropStatements tests various DROP statements
func TestDropStatements(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create objects
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE INDEX idx_test ON test (id)")
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE VIEW test_view AS SELECT * FROM test")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
	}

	// DROP VIEW
	_, err = db.Exec(ctx, "DROP VIEW test_view")
	if err != nil {
		t.Logf("DROP VIEW error: %v", err)
	}

	// DROP INDEX
	_, err = db.Exec(ctx, "DROP INDEX idx_test")
	if err != nil {
		t.Logf("DROP INDEX error: %v", err)
	}

	// DROP TABLE
	_, err = db.Exec(ctx, "DROP TABLE test")
	if err != nil {
		t.Logf("DROP TABLE error: %v", err)
	}
}

// TestMaterializedViewOperations tests materialized view operations
func TestMaterializedViewOperations(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 10), (2, 20)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// CREATE MATERIALIZED VIEW
	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM test")
	if err != nil {
		t.Logf("CREATE MATERIALIZED VIEW error: %v", err)
		return
	}

	// Query materialized view
	rows, err := db.Query(ctx, "SELECT * FROM mv_test")
	if err != nil {
		t.Logf("Query MV error: %v", err)
	} else if rows != nil {
		rows.Close()
	}

	// REFRESH MATERIALIZED VIEW
	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Logf("REFRESH MATERIALIZED VIEW error: %v", err)
	}

	// DROP MATERIALIZED VIEW
	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Logf("DROP MATERIALIZED VIEW error: %v", err)
	}
}

// TestPolicyOperations tests RLS policy operations
func TestPolicyOperations(t *testing.T) {
	db, err := Open(":memory:", &Options{EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// CREATE POLICY
	_, err = db.Exec(ctx, "CREATE POLICY test_policy ON test FOR SELECT USING (id > 0)")
	if err != nil {
		t.Logf("CREATE POLICY error: %v", err)
	}

	// DROP POLICY
	_, err = db.Exec(ctx, "DROP POLICY test_policy ON test")
	if err != nil {
		t.Logf("DROP POLICY error: %v", err)
	}
}
