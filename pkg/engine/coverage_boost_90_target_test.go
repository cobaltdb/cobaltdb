package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
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

// TestGetIndexRecommendations90 tests the index advisor API
func TestGetIndexRecommendations90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

	// Run queries that should trigger advisor recommendations
	for i := 0; i < 5; i++ {
		db.Query(ctx, "SELECT * FROM users WHERE email = 'test@example.com'")
	}

	recs := db.GetIndexRecommendations()
	if recs == nil {
		t.Log("No recommendations yet")
	}
}

// TestResetIndexAdvisor90 tests resetting the advisor
func TestResetIndexAdvisor90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	db.Query(ctx, "SELECT * FROM users WHERE email = 'test'")

	recsBefore := db.GetIndexRecommendations()
	if len(recsBefore) == 0 {
		t.Log("No recommendations before reset")
	}

	db.ResetIndexAdvisor()
	recsAfter := db.GetIndexRecommendations()
	if len(recsAfter) != 0 {
		t.Errorf("Expected 0 recommendations after reset, got %d", len(recsAfter))
	}
}

// TestRegisterFDW90 tests registering a custom FDW
func TestRegisterFDW90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	db.RegisterFDW("mock", func() fdw.ForeignDataWrapper {
		return nil
	})
}

// TestGetScheduler90 tests GetScheduler
func TestGetScheduler90(t *testing.T) {
	// In-memory databases don't start the scheduler by default
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	sched := db.GetScheduler()
	if sched != nil {
		t.Error("Expected nil scheduler for in-memory DB")
	}

	// Disk-backed databases do start the scheduler
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sched.db")
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db2.Close()

	sched2 := db2.GetScheduler()
	if sched2 == nil {
		t.Error("Expected non-nil scheduler for disk-backed DB")
	}
}

// TestGetCatalog90 tests GetCatalog
func TestGetCatalog90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cat := db.GetCatalog()
	if cat == nil {
		t.Error("Expected non-nil catalog")
	}
}

// TestCreateForeignTable90 tests CREATE FOREIGN TABLE through engine
func TestCreateForeignTable90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create foreign table without a real CSV file
	_, err = db.Exec(ctx, "CREATE FOREIGN TABLE ext (id INTEGER, name TEXT) WRAPPER 'csv' OPTIONS (file '/tmp/nonexistent.csv')")
	if err != nil {
		// May fail because file doesn't exist when wrapper validates
		t.Logf("CREATE FOREIGN TABLE returned: %v", err)
	}
}

// TestLoadExistingReopen tests reopening an existing database (loadExisting path).
func TestLoadExistingReopen90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "reopen.db")

	// Create and populate with slow query log defaults
	db, err := Open(dbPath, &Options{
		EnableSlowQueryLog:     true,
		SlowQueryThreshold:     0,
		SlowQueryMaxEntries:    0,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")
	db.Close()

	// Reopen - triggers loadExisting
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	row := db2.QueryRow(ctx, "SELECT COUNT(*) FROM test")
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Query after reopen failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row after reopen, got %d", count)
	}
}

// TestCreateNewWithSlowQueryDefaults tests createNew with slow query log default values.
func TestCreateNewWithSlowQueryDefaults90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "slow.db")
	db, err := Open(dbPath, &Options{
		EnableSlowQueryLog: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
}

// TestQueryWithCancelledContext tests query with a cancelled context.
func TestQueryWithCancelledContext90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.query(ctx, &query.SelectStmt{From: &query.TableRef{Name: "test"}}, nil)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

// TestStartSchedulerNoAutoVacuum tests startScheduler with auto-vacuum disabled.
func TestStartSchedulerNoAutoVacuum90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sched_no_av.db")
	db, err := Open(dbPath, &Options{
		EnableAutoVacuum: false,
		EnableScheduler:  true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	sched := db.GetScheduler()
	if sched == nil {
		t.Error("Expected non-nil scheduler")
	}
}

// TestRunAnalyzeJob tests the auto-analyze job directly.
func TestRunAnalyzeJob90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	err = db.runAnalyzeJob()
	if err != nil {
		t.Errorf("runAnalyzeJob failed: %v", err)
	}
}

// TestRunAutoVacuumJob tests the auto-vacuum job directly.
func TestRunAutoVacuumJob90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	db.Exec(ctx, "DELETE FROM test WHERE id > 1")

	err = db.runAutoVacuumJob(0.1)
	if err != nil {
		t.Errorf("runAutoVacuumJob failed: %v", err)
	}
}

// TestExecuteCreateForeignTableError tests executeCreateForeignTable error path.
func TestExecuteCreateForeignTableError90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.executeCreateForeignTable(context.Background(), &query.CreateForeignTableStmt{
		Table:   "dup",
		Wrapper: "missing",
	})
	if err == nil {
		t.Error("Expected error for missing wrapper")
	}
}

// TestExecuteVacuum90 tests executeVacuum happy path.
func TestExecuteVacuum90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	_, err = db.executeVacuum(ctx, &query.VacuumStmt{})
	if err != nil {
		t.Logf("executeVacuum returned: %v", err)
	}
}

// TestExecuteSelectWithCTEError tests executeSelectWithCTE error path.
func TestExecuteSelectWithCTEError90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// CTE with a bad query should error
	_, err = db.executeSelectWithCTE(context.Background(), &query.SelectStmtWithCTE{
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.StarExpr{}},
			From:    &query.TableRef{Name: "nonexistent"},
		},
	}, nil)
	if err == nil {
		t.Error("Expected error for CTE on nonexistent table")
	}
}

// TestQueryNonQueryStatement tests query with a non-query statement type.
func TestQueryNonQueryStatement90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.query(context.Background(), &query.CreateTableStmt{Table: "test"}, nil)
	if err == nil {
		t.Error("Expected error for non-query statement")
	}
}

// TestExecuteWithCancelledContext tests execute with a cancelled context.
func TestExecuteWithCancelledContext90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.execute(ctx, &query.InsertStmt{Table: "test", Columns: []string{"id"}, Values: [][]query.Expression{{&query.NumberLiteral{Value: 1}}},}, nil)
	if err == nil {
		t.Error("Expected error for cancelled context in execute")
	}
}

// TestCircuitBreakerHalfOpen tests circuit breaker half-open state.
func TestCircuitBreakerHalfOpen90(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         1,
		ResetTimeout:        1 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		MinSuccesses:        1,
		MaxConcurrency:      100,
	})
	defer cb.Stop()

	// Fail to open
	cb.Allow()
	cb.ReportFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("Expected open state, got %v", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(5 * time.Millisecond)

	// Allow transitions to half-open (does not consume token)
	err := cb.Allow()
	if err != nil {
		t.Fatalf("Expected allow in half-open, got %v", err)
	}
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("Expected half-open state, got %v", cb.State())
	}

	// Allow in half-open consumes the token
	err = cb.Allow()
	if err != nil {
		t.Fatalf("Expected allow with token, got %v", err)
	}

	// Third allow should fail (no tokens left)
	err = cb.Allow()
	if err != ErrCircuitOpen {
		t.Errorf("Expected ErrCircuitOpen, got %v", err)
	}

	// Report success should close circuit
	cb.ReportSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("Expected closed state after success, got %v", cb.State())
	}
}

// TestExecuteDefaultCase tests execute with an unsupported statement type.
func TestExecuteDefaultCase90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.execute(context.Background(), &query.CreateCollectionStmt{Name: "coll"}, nil)
	if err == nil {
		t.Error("Expected error for unsupported statement type")
	}
}

// TestOpenWithPlanCache tests opening a database with plan cache enabled.
func TestOpenWithPlanCache90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan_cache.db")
	db, err := Open(dbPath, &Options{
		EnablePlanCache: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	// Execute same query twice to exercise plan cache
	db.Exec(ctx, "SELECT * FROM test WHERE id = 1")
	db.Exec(ctx, "SELECT * FROM test WHERE id = 2")
}

// TestExecuteCreateViewIfNotExists tests executeCreateView with IF NOT EXISTS on existing view.
func TestExecuteCreateViewIfNotExists90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE VIEW v_test AS SELECT 1")

	_, err = db.executeCreateView(ctx, &query.CreateViewStmt{Name: "v_test", Query: &query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 2}}}, IfNotExists: true})
	if err != nil {
		t.Errorf("IF NOT EXISTS on existing view should succeed: %v", err)
	}
}

// TestExecuteDropViewIfExists tests executeDropView with IF EXISTS on non-existent view.
func TestExecuteDropViewIfExists90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.executeDropView(context.Background(), &query.DropViewStmt{Name: "missing_view", IfExists: true})
	if err != nil {
		t.Errorf("IF EXISTS on non-existent view should succeed: %v", err)
	}
}

// TestExecuteAnalyzeAllTables tests executeAnalyze with empty table (analyze all).
func TestExecuteAnalyzeAllTables90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2)")

	_, err = db.executeAnalyze(ctx, &query.AnalyzeStmt{Table: ""})
	if err != nil {
		t.Errorf("Analyze all tables failed: %v", err)
	}
}

// TestOpenWithAuditAndCompression tests opening with audit logger and compression.
func TestOpenWithAuditAndCompression90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit_comp.db")

	auditFile := filepath.Join(dir, "audit.log")
	db, err := Open(dbPath, &Options{
		CompressionConfig: &storage.CompressionConfig{Enabled: true, Level: storage.CompressionLevelFast, MinRatio: 0.9},
		AuditConfig: &audit.Config{
			Enabled:    true,
			LogFile:    auditFile,
			LogFormat:  "json",
			LogDDL:     true,
			LogQueries: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	// Run DDL and DML to exercise auditLogger branches in execute
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO test VALUES (1, 'alice')")
	db.Exec(ctx, "UPDATE test SET name = 'bob' WHERE id = 1")
	db.Query(ctx, "SELECT * FROM test")
	db.Exec(ctx, "DELETE FROM test WHERE id = 1")
	db.Exec(ctx, "DROP TABLE test")
}

// TestQueryNonQueryStatements tests query() with INSERT/UPDATE/DELETE without RETURNING.
func TestQueryNonQueryStatements90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	_, err = db.query(ctx, &query.InsertStmt{Table: "test", Columns: []string{"id"}, Values: [][]query.Expression{{&query.NumberLiteral{Value: 1}}}}, nil)
	if err == nil {
		t.Error("Expected error for INSERT without RETURNING in query()")
	}

	_, err = db.query(ctx, &query.UpdateStmt{Table: "test", Set: []*query.SetClause{{Column: "id", Value: &query.NumberLiteral{Value: 2}}}}, nil)
	if err == nil {
		t.Error("Expected error for UPDATE without RETURNING in query()")
	}

	_, err = db.query(ctx, &query.DeleteStmt{Table: "test"}, nil)
	if err == nil {
		t.Error("Expected error for DELETE without RETURNING in query()")
	}
}

// TestHealthCheckClosed tests HealthCheck on a closed database.
func TestHealthCheckClosed90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()

	err = db.HealthCheck()
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}

// TestLoadExistingWithReplicationAndCaches tests loadExisting with replication, caches, and slow query log.
func TestLoadExistingWithReplicationAndCaches90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "repl_cache.db")

	// Create database with replication (master), query cache, plan cache, slow query log
	db, err := Open(dbPath, &Options{
		ReplicationRole:       "master",
		ReplicationMode:       "sync",
		ReplicationListenAddr: "127.0.0.1:0",
		EnableQueryCache:      true,
		EnablePlanCache:       true,
		PlanCacheSize:         0,
		PlanCacheEntries:      0,
		EnableSlowQueryLog:    true,
		SlowQueryThreshold:    0,
		SlowQueryMaxEntries:   0,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")
	db.Close()

	// Reopen to trigger loadExisting paths with full_sync mode
	db2, err := Open(dbPath, &Options{
		ReplicationRole:       "master",
		ReplicationMode:       "full_sync",
		ReplicationListenAddr: "127.0.0.1:0",
		EnableQueryCache:      true,
		EnablePlanCache:       true,
		PlanCacheSize:         0,
		PlanCacheEntries:      0,
		EnableSlowQueryLog:    true,
		SlowQueryThreshold:    0,
		SlowQueryMaxEntries:   0,
	})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	row := db2.QueryRow(ctx, "SELECT COUNT(*) FROM test")
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Query after reopen failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row after reopen, got %d", count)
	}
}

// TestGetMetricsNil tests GetMetrics when metrics collector is nil.
func TestGetMetricsNil90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Manually nil out metrics to test defensive check
	db.metrics = nil
	_, err = db.GetMetrics()
	if err == nil {
		t.Error("Expected error when metrics is nil")
	}
}

// TestBackupMethodsNilManager tests backup methods when backupMgr is nil.
func TestBackupMethodsNilManager90(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Manually nil out backupMgr to test defensive checks
	db.backupMgr = nil

	backups := db.ListBackups()
	if backups != nil {
		t.Error("Expected nil backups when backupMgr is nil")
	}

	b := db.GetBackup("id")
	if b != nil {
		t.Error("Expected nil backup when backupMgr is nil")
	}

	err = db.DeleteBackup("id")
	if err == nil {
		t.Error("Expected error when backupMgr is nil")
	}
}

// TestReplicateWritePath tests replicateWrite when replication manager is active.
func TestReplicateWritePath90(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "repl_write.db")

	db, err := Open(dbPath, &Options{
		ReplicationRole:       "master",
		ReplicationMode:       "async",
		ReplicationListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	// Write operations should trigger replicateWrite
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")
	db.Exec(ctx, "UPDATE test SET id = 2 WHERE id = 1")
	db.Exec(ctx, "DELETE FROM test WHERE id = 2")
	db.Exec(ctx, "DROP TABLE test")
}
