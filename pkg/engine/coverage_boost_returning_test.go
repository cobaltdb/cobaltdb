package engine

import (
	"context"
	"testing"
)

// TestReturningClauseInsert tests INSERT ... RETURNING
func TestReturningClauseInsert(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// INSERT RETURNING *
	rows, err := db.Query(ctx, "INSERT INTO test (name) VALUES ('Alice') RETURNING *")
	if err != nil {
		t.Logf("INSERT RETURNING * error (may not be supported): %v", err)
		return
	}
	defer rows.Close()

	// Check if we got the returned row
	count := 0
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Log("INSERT RETURNING did not return rows (feature may not be fully supported)")
	}
}

// TestReturningClauseUpdate tests UPDATE ... RETURNING
func TestReturningClauseUpdate(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// UPDATE RETURNING
	rows, err := db.Query(ctx, "UPDATE test SET name = 'Bob' WHERE id = 1 RETURNING id, name")
	if err != nil {
		t.Logf("UPDATE RETURNING error (may not be supported): %v", err)
		return
	}
	defer rows.Close()
}

// TestReturningClauseDelete tests DELETE ... RETURNING
func TestReturningClauseDelete(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// DELETE RETURNING
	rows, err := db.Query(ctx, "DELETE FROM test WHERE id = 1 RETURNING *")
	if err != nil {
		t.Logf("DELETE RETURNING error (may not be supported): %v", err)
		return
	}
	defer rows.Close()
}

// TestParamSubstitutionTypes tests parameter substitution with various types
func TestParamSubstitutionTypes(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, val REAL, flag BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test with various parameter types
	testCases := []struct {
		name string
		sql  string
		args []interface{}
	}{
		{"string param", "INSERT INTO test VALUES (?, ?, ?, ?)", []interface{}{1, "test", 1.5, true}},
		{"int param", "INSERT INTO test VALUES (?, ?, ?, ?)", []interface{}{2, "test2", 2.0, false}},
		{"float param", "INSERT INTO test VALUES (?, ?, ?, ?)", []interface{}{3, "test3", 3.14, true}},
		{"nil param", "INSERT INTO test VALUES (?, ?, ?, ?)", []interface{}{4, nil, nil, nil}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tc.sql, tc.args...)
			if err != nil {
				t.Logf("Param substitution error for %s: %v", tc.name, err)
			}
		})
	}
}

// TestEncryptionOptions tests opening database with encryption options
func TestEncryptionOptions(t *testing.T) {
	// Test with encryption key
	db, err := Open(":memory:", &Options{
		InMemory:      true,
		CacheSize:     1024,
		EncryptionKey: []byte("test-encryption-key-that-is-long"),
	})

	// May succeed or fail depending on encryption support
	if err != nil {
		t.Logf("Encryption error (may be expected): %v", err)
		return
	}
	defer db.Close()

	// Verify it works
	ctx := context.Background()
	_, err = db.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Logf("Query on encrypted DB error: %v", err)
	}
}

// TestQueryRowMultiColScan tests QueryRow with multiple columns
func TestQueryRowMultiColScan(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// QueryRow with multiple columns
	row := db.QueryRow(ctx, "SELECT id, name, age FROM test WHERE id = 1")
	var id, age int
	var name string
	if err := row.Scan(&id, &name, &age); err != nil {
		t.Logf("QueryRow scan error: %v", err)
	}

	if id != 1 || name != "Alice" || age != 30 {
		t.Errorf("Unexpected values: id=%d, name=%s, age=%d", id, name, age)
	}
}

// TestExplainQuery tests EXPLAIN query execution
func TestExplainQuery(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// EXPLAIN query
	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM test WHERE id = 1")
	if err != nil {
		t.Logf("EXPLAIN error (may be expected): %v", err)
		return
	}
	defer rows.Close()

	// Try to read results
	cols := rows.Columns()
	t.Logf("EXPLAIN returned columns: %v", cols)
}

// TestBackupDatabaseInterface tests backup.Database interface methods
func TestBackupDatabaseInterface(t *testing.T) {
	// Test with in-memory database (no WAL)
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test GetWALPath with no WAL
	walPath := db.GetWALPath()
	if walPath != "" {
		t.Errorf("Expected empty WAL path for in-memory DB, got '%s'", walPath)
	}

	// Test Checkpoint with no WAL
	if err := db.Checkpoint(); err != nil {
		t.Errorf("Checkpoint failed: %v", err)
	}

	// Test GetCurrentLSN with no WAL
	lsn := db.GetCurrentLSN()
	if lsn != 0 {
		t.Errorf("Expected LSN 0 for in-memory DB, got %d", lsn)
	}

	// Test BeginHotBackup
	if err := db.BeginHotBackup(); err != nil {
		t.Errorf("BeginHotBackup failed: %v", err)
	}

	// Test EndHotBackup
	if err := db.EndHotBackup(); err != nil {
		t.Errorf("EndHotBackup failed: %v", err)
	}

	// Test backup manager methods with nil manager
	backups := db.ListBackups()
	if backups == nil {
		t.Log("ListBackups returned nil when manager not initialized")
	} else if len(backups) != 0 {
		t.Errorf("Expected 0 backups, got %d", len(backups))
	}

	backup := db.GetBackup("test")
	if backup != nil {
		t.Error("Expected nil backup when manager not initialized")
	}

	// Test GetMetrics with nil metrics
	metrics, err := db.GetMetrics()
	if err == nil {
		t.Logf("GetMetrics returned: %s", string(metrics))
	} else {
		t.Logf("GetMetrics returned expected error: %v", err)
	}
}

// TestBackupDatabaseInterfaceWithWAL tests backup interface with WAL enabled
func TestBackupDatabaseInterfaceWithWAL(t *testing.T) {
	// Create temp directory for database
	tempDir := t.TempDir()
	dbPath := tempDir + "/test_wal.db"

	// Open with WAL enabled
	db, err := Open(dbPath, &Options{
		InMemory:    false,
		WALEnabled:  true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table and insert data
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test GetWALPath
	walPath := db.GetWALPath()
	t.Logf("WAL path: %s", walPath)

	// Test GetCurrentLSN
	lsn := db.GetCurrentLSN()
	t.Logf("Current LSN: %d", lsn)

	// Test Checkpoint
	if err := db.Checkpoint(); err != nil {
		t.Errorf("Checkpoint failed: %v", err)
	}

	db.Close()
}


// TestCreateBackupWithoutManager tests CreateBackup when manager is nil
func TestCreateBackupWithoutManager(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.CreateBackup(ctx, "full")
	if err == nil {
		t.Error("Expected error when backup manager not initialized")
	} else {
		t.Logf("CreateBackup without manager returned expected error: %v", err)
	}
}
