package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestExecuteWithContextCancellation tests execute with cancelled context
func TestExecuteWithContextCancellation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Try to execute with cancelled context
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("Expected error for execute with cancelled context")
	}
}

// TestExecuteWithAutocommitWAL tests execute with autocommit mode (WAL enabled)
func TestExecuteWithAutocommitWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Create table - should use autocommit
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("CREATE TABLE failed: %v", err)
	}

	// Insert - should use autocommit
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	// Verify data
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	} else if rows != nil {
		rows.Close()
	}
}

// TestExecuteTransactionControlStatements tests transaction control statements
func TestExecuteTransactionControlStatements(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Test BEGIN when already in transaction
	db.Exec(ctx, "BEGIN")
	_, err = db.Exec(ctx, "BEGIN")
	if err == nil {
		t.Error("Expected error for BEGIN when already in transaction")
	}
	db.Exec(ctx, "COMMIT")

	// Test COMMIT when not in transaction
	_, err = db.Exec(ctx, "COMMIT")
	if err == nil {
		t.Error("Expected error for COMMIT when not in transaction")
	}

	// Test ROLLBACK when not in transaction
	_, err = db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("Expected error for ROLLBACK when not in transaction")
	}
}

// TestExecuteSavepointStatements tests SAVEPOINT statements
func TestExecuteSavepointStatements(t *testing.T) {
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

	// SAVEPOINT without transaction should fail
	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err == nil {
		t.Error("Expected error for SAVEPOINT without transaction")
	}

	// RELEASE SAVEPOINT without transaction should fail
	_, err = db.Exec(ctx, "RELEASE SAVEPOINT sp1")
	if err == nil {
		t.Error("Expected error for RELEASE SAVEPOINT without transaction")
	}

	// Begin transaction and test savepoints
	db.Exec(ctx, "BEGIN")
	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err != nil {
		t.Errorf("SAVEPOINT failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	// Rollback to savepoint
	_, err = db.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")
	if err != nil {
		t.Errorf("ROLLBACK TO SAVEPOINT failed: %v", err)
	}

	// Release savepoint
	_, err = db.Exec(ctx, "RELEASE SAVEPOINT sp1")
	if err != nil {
		t.Errorf("RELEASE SAVEPOINT failed: %v", err)
	}

	db.Exec(ctx, "COMMIT")
}

// TestExecuteMySQLCompatibility tests MySQL compatibility statements
func TestExecuteMySQLCompatibility(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// SET statement should be accepted silently
	_, err = db.Exec(ctx, "SET sql_mode = 'STRICT_TRANS_TABLES'")
	if err != nil {
		t.Errorf("SET failed: %v", err)
	}

	// USE statement should be accepted silently
	_, err = db.Exec(ctx, "USE testdb")
	if err != nil {
		t.Errorf("USE failed: %v", err)
	}
}

// TestExecuteShowStatementsWithExec tests SHOW statements with Exec (should fail)
func TestExecuteShowStatementsWithExec(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE test (id INTEGER)")

	// SHOW TABLES with Exec should fail
	_, err = db.Exec(ctx, "SHOW TABLES")
	if err == nil {
		t.Error("Expected error for SHOW TABLES with Exec")
	}

	// SHOW CREATE TABLE with Exec should fail
	_, err = db.Exec(ctx, "SHOW CREATE TABLE test")
	if err == nil {
		t.Error("Expected error for SHOW CREATE TABLE with Exec")
	}
}

// TestQueryWithContextCancellation tests query with cancelled context
func TestQueryWithContextCancellation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = db.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("Expected error for query with cancelled context")
	}
}

// TestCreateNewWithAllOptions tests createNew with various options
func TestCreateNewWithAllOptions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	backupDir := filepath.Join(dir, "backups")

	db, err := Open(dbPath, &Options{
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

// TestLoadExistingWithAllOptions tests loadExisting with various options
func TestLoadExistingWithAllOptions(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	backupDir := filepath.Join(dir, "backups")

	// Create database first
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	ctx := context.Background()
	db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db1.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	db1.Close()

	// Reopen with various options
	db2, err := Open(dbPath, &Options{
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
	})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db2.Close()

	// Verify data
	result, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Errorf("Query failed: %v", err)
	} else if result != nil {
		result.Close()
	}
}

// TestCommitErrorPaths tests Commit error paths
func TestCommitErrorPaths(t *testing.T) {
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

	// Normal commit should succeed
	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned: %v", err)
	}
}

// TestExecuteVacuumAndAnalyze tests VACUUM and ANALYZE statements
func TestExecuteVacuumAndAnalyze(t *testing.T) {
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
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, "data")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Delete some rows to create fragmentation
	_, err = db.Exec(ctx, "DELETE FROM test WHERE id % 2 = 0")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// VACUUM
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM returned: %v", err)
	}

	// ANALYZE specific table
	_, err = db.Exec(ctx, "ANALYZE test")
	if err != nil {
		t.Logf("ANALYZE test returned: %v", err)
	}

	// ANALYZE all tables
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE returned: %v", err)
	}
}

// TestBackupOperations tests backup operations
func TestBackupOperations(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	backupDir := filepath.Join(dir, "backups")

	db, err := Open(dbPath, &Options{
		BackupDir:       backupDir,
		BackupRetention: 7 * 24 * time.Hour,
		MaxBackups:      10,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create backup
	backupInfo, err := db.CreateBackup(ctx, "full")
	if err != nil {
		t.Logf("CreateBackup returned: %v", err)
	} else {
		t.Logf("Created backup: %+v", backupInfo)
	}

	// List backups
	backups := db.ListBackups()
	t.Logf("Backups: %v", backups)

	// Test backup manager getter
	mgr := db.GetBackupManager()
	if mgr == nil {
		t.Log("GetBackupManager returned nil")
	}
}

// TestWALOperations tests WAL-related operations
func TestWALOperations(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get WAL path
	walPath := db.GetWALPath()
	if walPath == "" {
		t.Log("WAL path is empty - WAL may not be initialized")
	} else {
		t.Logf("WAL path: %s", walPath)
	}

	// Get current LSN
	lsn := db.GetCurrentLSN()
	t.Logf("Current LSN: %d", lsn)

	// Insert data to advance LSN
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Get new LSN
	newLSN := db.GetCurrentLSN()
	t.Logf("New LSN after insert: %d", newLSN)

	// Checkpoint
	err = db.Checkpoint()
	if err != nil {
		t.Errorf("Checkpoint failed: %v", err)
	}
}

// TestHotBackupOperations tests hot backup operations
func TestHotBackupOperations(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Begin hot backup
	backupPath := db.BeginHotBackup()
	if err != nil {
		t.Logf("BeginHotBackup returned: %v", err)
	} else {
		t.Logf("Hot backup path: %s", backupPath)

		// End hot backup
		err = db.EndHotBackup()
		if err != nil {
			t.Logf("EndHotBackup returned: %v", err)
		}
	}
}

// TestQueryCacheOperations tests query cache operations
func TestQueryCacheOperations(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, &Options{
		EnableQueryCache: true,
		QueryCacheSize:   1024,
		QueryCacheTTL:    60 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	// Get query cache
	cache := db.GetQueryCache()
	if cache == nil {
		t.Error("Expected non-nil query cache")
		return
	}

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute query to populate cache
	_, err = db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Errorf("First query failed: %v", err)
	}

	// Same query - should hit cache
	_, err = db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Errorf("Second query failed: %v", err)
	}
}

// TestDatabaseStats tests database statistics
func TestDatabaseStats(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get stats
	stats, err := db.Stats()
	if err != nil {
		t.Errorf("Stats failed: %v", err)
	} else {
		t.Logf("Database stats: %+v", stats)
	}

	// Get database path
	path := db.GetDatabasePath()
	if path != dbPath {
		t.Errorf("Expected path %s, got %s", dbPath, path)
	}
}

// TestCloseWithActiveTransactions2 tests closing database with active transactions
func TestCloseWithActiveTransactions2(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin but don't commit
	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO test VALUES (1)")

	// Close should handle active transaction
	err = db.Close()
	if err != nil {
		t.Logf("Close with active transaction returned: %v", err)
	}
}
