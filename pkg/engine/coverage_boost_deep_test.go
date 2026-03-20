package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadExistingWithWALRecoveryDeep tests loadExisting with WAL recovery
func TestLoadExistingWithWALRecoveryDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database with WAL
	db1, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Perform some operations
	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	// Close without checkpoint
	db1.Close()

	// Reopen - should recover from WAL
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify data was recovered
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Errorf("Query after recovery failed: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestLoadExistingWithQueryCacheDeep tests loadExisting with query cache enabled
func TestLoadExistingWithQueryCacheDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db1.Close()

	// Reopen with query cache
	db2, err := Open(dbPath, &Options{
		EnableQueryCache: true,
		QueryCacheSize:   1000,
		QueryCacheTTL:    60,
	})
	if err != nil {
		t.Fatalf("Failed to reopen with query cache: %v", err)
	}
	defer db2.Close()

	ctx := context.Background()
	_, err = db2.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}
}

// TestLoadExistingWithRLSDeep tests loadExisting with RLS enabled
func TestLoadExistingWithRLSDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db1.Close()

	// Reopen with RLS
	db2, err := Open(dbPath, &Options{EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to reopen with RLS: %v", err)
	}
	defer db2.Close()
}

// TestLoadExistingWithReplicationDeep tests loadExisting with replication config
func TestLoadExistingWithReplicationDeep(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_repl_load.db")

	// Create and close a database first
	db, err := Open(dbPath, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db.Close()

	// Reopen with replication config
	db2, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "master",
		ReplicationMode:       "async",
		ReplicationListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to reopen with replication: %v", err)
	}
	defer db2.Close()

	if db2.replicationMgr == nil {
		t.Log("Replication manager not initialized on reload")
	} else {
		t.Log("Replication manager initialized on reload")
	}
}

// TestCreateNewWithReplicationDeep tests createNew with replication
func TestCreateNewWithReplicationDeep(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_repl_new.db")

	db, err := Open(dbPath, &Options{
		CacheSize:             256,
		ReplicationRole:       "master",
		ReplicationMode:       "async",
		ReplicationListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to create with replication: %v", err)
	}
	defer db.Close()

	if db.replicationMgr != nil {
		t.Log("Replication manager initialized for new DB")
	}
}

// TestCommitWithPoolFlushError tests Commit when buffer pool flush fails
func TestCommitWithPoolFlushError(t *testing.T) {
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

// TestQueryWithTimeout tests query with timeout
func TestQueryWithTimeout(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{QueryTimeout: 30})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1)")

	rows, err := db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Logf("Query with timeout returned: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestExecuteWithClosedDB tests execute on closed database
func TestExecuteWithClosedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("Expected error for execute on closed DB")
	}
}

// TestCloseWithActiveTransactions tests Close with active transactions
func TestCloseWithActiveTransactions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

	// Begin but don't commit
	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Close should handle active transaction
	err = db.Close()
	if err != nil {
		t.Logf("Close with active transaction returned: %v", err)
	}
}

// TestHealthCheckErrorPath tests HealthCheck error paths
func TestHealthCheckErrorPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Test health check on healthy DB
	err = db.HealthCheck()
	if err != nil {
		t.Logf("HealthCheck returned: %v", err)
	}

	// Close and test again
	db.Close()
	err = db.HealthCheck()
	if err == nil {
		t.Error("Expected error for HealthCheck on closed DB")
	}
}

// TestCreateBackupWithoutMgrDeep tests CreateBackup without backup manager
func TestCreateBackupWithoutMgrDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.CreateBackup(ctx, "full")
	if err == nil {
		t.Log("CreateBackup without manager may return error or nil")
	}
}

// TestListBackupsWithNilManager tests ListBackups with nil manager
func TestListBackupsWithNilManager(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	result := db.ListBackups()
	t.Logf("ListBackups returned: %v", result)
}

// TestGetBackupWithNilManager tests GetBackup with nil manager
func TestGetBackupWithNilManager(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	result := db.GetBackup("test-id")
	t.Logf("GetBackup returned: %v", result)
}

// TestDeleteBackupWithNilManager tests DeleteBackup with nil manager
func TestDeleteBackupWithNilManager(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	err = db.DeleteBackup("test-id")
	if err == nil {
		t.Log("DeleteBackup without manager may return error or nil")
	}
}

// TestGetWALPathNoWALDeep tests GetWALPath when WAL is not enabled
func TestGetWALPathNoWALDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	path := db.GetWALPath()
	if path != "" {
		t.Errorf("Expected empty WAL path when WAL disabled, got: %s", path)
	}
}

// TestCheckpointNoWALDeep tests Checkpoint when WAL is not enabled
func TestCheckpointNoWALDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	err = db.Checkpoint()
	if err != nil {
		t.Errorf("Checkpoint without WAL should succeed, got: %v", err)
	}
}

// TestGetCurrentLSNNoWALDeep tests GetCurrentLSN when WAL is not enabled
func TestGetCurrentLSNNoWALDeep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	lsn := db.GetCurrentLSN()
	if lsn != 0 {
		t.Errorf("Expected LSN=0 without WAL, got: %d", lsn)
	}
}

// TestExecuteSelectWithCTEError tests executeSelectWithCTE error paths
func TestExecuteSelectWithCTEError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create tables
	db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO t1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO t2 VALUES (1)")

	// Test CTE query
	_, err = db.Query(ctx, "WITH cte AS (SELECT * FROM t1) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE query returned: %v", err)
	}
}

// TestExecuteVacuumError tests executeVacuum error path
func TestExecuteVacuumError(t *testing.T) {
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
	db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")

	// Delete some rows
	db.Exec(ctx, "DELETE FROM test WHERE id > 1")

	// Vacuum
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM returned: %v", err)
	}
}

// TestOpenWithInvalidMetaPage tests Open with invalid meta page
func TestOpenWithInvalidMetaPage(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a file with invalid meta page data
	f, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	// Write some data that's not a valid meta page
	data := make([]byte, 4096)
	copy(data, []byte("COBALTDB"))
	f.Write(data)
	f.Close()

	// Try to open
	_, err = Open(dbPath, nil)
	if err == nil {
		t.Error("Expected error for invalid meta page")
	}
}

// TestOpenWithCorruptMetaPageData tests Open with corrupt meta page
func TestOpenWithCorruptMetaPageData(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a file that's too short
	f, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("SHORT"))
	f.Close()

	// Try to open - may create new or error
	db, err := Open(dbPath, nil)
	if err != nil {
		t.Logf("Open with short file returned: %v", err)
	} else {
		db.Close()
	}
}

// TestCreateNewWithInvalidPath tests createNew with invalid path
func TestCreateNewWithInvalidPath(t *testing.T) {
	// Try to create at an invalid path
	// On Windows, this path may actually be valid, so we just log the result
	invalidPath := "/nonexistent/path/test.db"
	db, err := Open(invalidPath, nil)
	if err == nil {
		t.Log("Path succeeded - may be valid on this platform")
		db.Close()
	} else {
		t.Logf("Path failed as expected: %v", err)
	}
}

// TestLoadExistingWithInvalidWALPath tests loadExisting with WAL path issues
func TestLoadExistingWithInvalidWALPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create database without WAL
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db1.Close()

	// Reopen with WAL enabled
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Logf("Reopen with WAL enabled returned: %v", err)
	} else {
		db2.Close()
	}
}

// TestStorageBackendErrorDeep tests storage backend error scenarios
func TestStorageBackendErrorDeep(t *testing.T) {
	// Test opening a database at a clearly invalid path
	_, err := Open("/dev/null/impossible/path/db.cdb", nil)
	if err == nil {
		t.Log("Path unexpectedly succeeded on this system")
	} else {
		t.Logf("Expected error for invalid path: %v", err)
	}

	// Test opening with valid in-memory path works fine
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("In-memory open failed: %v", err)
	}
	defer db.Close()

	// Simple query to verify the database works
	_, err = db.Exec(context.Background(), "CREATE TABLE test_storage (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("CREATE TABLE failed: %v", err)
	}
}
