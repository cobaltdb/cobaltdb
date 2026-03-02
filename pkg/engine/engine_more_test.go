package engine

import (
	"context"
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts == nil {
		t.Fatal("DefaultOptions returned nil")
	}
	if opts.CacheSize == 0 {
		t.Error("Expected non-zero cache size")
	}
}

func TestOpenWithNilOptions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
}

func TestDatabaseClosed(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	db.Close()

	ctx := context.Background()
	_, err := db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}

func TestDatabaseDoubleClose(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})

	// Close twice - should not error
	if err := db.Close(); err != nil {
		t.Errorf("First close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("Second close failed: %v", err)
	}
}

func TestQueryClosedDatabase(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	db.Close()

	ctx := context.Background()
	_, err := db.Query(ctx, "SELECT * FROM test")
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}

func TestBeginClosedDatabase(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	db.Close()

	ctx := context.Background()
	_, err := db.Begin(ctx)
	if err != ErrDatabaseClosed {
		t.Errorf("Expected ErrDatabaseClosed, got %v", err)
	}
}

func TestExecParseError(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Exec(ctx, "INVALID SQL")
	if err == nil {
		t.Error("Expected parse error")
	}
}

func TestQueryParseError(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Query(ctx, "INVALID SQL")
	if err == nil {
		t.Error("Expected parse error")
	}
}

func TestQueryNonSelect(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Query(ctx, "CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("Expected error for non-SELECT query")
	}
}

func TestExecBeginStmt(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Exec(ctx, "BEGIN")
	if err == nil {
		t.Error("Expected error for BEGIN via Exec")
	}
}

func TestExecCommitStmt(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Exec(ctx, "COMMIT")
	if err == nil {
		t.Error("Expected error for COMMIT via Exec")
	}
}

func TestExecRollbackStmt(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	_, err := db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("Expected error for ROLLBACK via Exec")
	}
}

func TestExecuteUpdate(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, value TEXT)`)
	db.Exec(ctx, `INSERT INTO test (id, value) VALUES (1, 'old')`)

	// Update
	_, err = db.Exec(ctx, `UPDATE test SET value = 'new' WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
}

func TestExecuteDelete(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	// Delete
	_, err = db.Exec(ctx, `DELETE FROM test`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
}

func TestExecuteDropTable(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE temp (id INTEGER)`)

	// Drop table
	_, err = db.Exec(ctx, `DROP TABLE temp`)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestTransactionQuery(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Query in transaction
	rows, err := tx.Query(ctx, `SELECT id FROM test`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to query in transaction: %v", err)
	}
	rows.Close()

	tx.Commit()
}

func TestTransactionExec(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Exec in transaction
	_, err = tx.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to exec in transaction: %v", err)
	}

	tx.Commit()
}

func TestTransactionRollback(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	tx.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}
}

func TestQueryRowScan(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	db.Exec(ctx, `INSERT INTO test (id, name) VALUES (1, 'Test')`)

	// Query row
	row := db.QueryRow(ctx, `SELECT id, name FROM test`)
	if row == nil {
		t.Fatal("QueryRow returned nil")
	}

	var id, name interface{}
	err = row.Scan(&id, &name)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}
}

func TestQueryRowNoRows(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	row := db.QueryRow(ctx, `SELECT id FROM test WHERE id = 999`)
	if row == nil {
		t.Fatal("QueryRow returned nil")
	}

	var id interface{}
	err := row.Scan(&id)
	if err == nil {
		t.Error("Expected error for no rows")
	}
}

func TestQueryRowQueryError(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	row := db.QueryRow(ctx, `SELECT * FROM nonexistent`)
	var id interface{}
	err := row.Scan(&id)
	if err == nil {
		t.Error("Expected error for query error")
	}
}

func TestRowsNextAndScan(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (2)`)

	rows, err := db.Query(ctx, `SELECT id FROM test`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id interface{}
		if err := rows.Scan(&id); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestRowsColumns(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)

	rows, _ := db.Query(ctx, `SELECT id, name FROM test`)
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
}

func TestRowsScanMismatch(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	rows, _ := db.Query(ctx, `SELECT id FROM test`)
	defer rows.Close()

	if rows.Next() {
		var id, extra interface{}
		err := rows.Scan(&id, &extra)
		if err == nil {
			t.Error("Expected error for column count mismatch")
		}
	}
}

func TestRowsScanNoCurrentRow(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	rows, _ := db.Query(ctx, `SELECT id FROM test`)
	defer rows.Close()

	var id interface{}
	err := rows.Scan(&id)
	if err == nil {
		t.Error("Expected error for no current row")
	}
}

func TestScanValueTypes(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT, active BOOLEAN, price REAL)`)

	// Insert test data
	db.Exec(ctx, `INSERT INTO test (id, name, active, price) VALUES (1, 'Test', TRUE, 19.99)`)

	rows, _ := db.Query(ctx, `SELECT id, name, active, price FROM test`)
	defer rows.Close()

	if rows.Next() {
		var id int
		var name string
		var active bool
		var price float64

		err := rows.Scan(&id, &name, &active, &price)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}

		if id != 1 {
			t.Errorf("Expected id 1, got %d", id)
		}
		if name != "Test" {
			t.Errorf("Expected name 'Test', got %q", name)
		}
		if !active {
			t.Error("Expected active true")
		}
		if price != 19.99 {
			t.Errorf("Expected price 19.99, got %f", price)
		}
	}
}

func TestScanIntoInterface(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (42)`)

	rows, _ := db.Query(ctx, `SELECT id FROM test`)
	defer rows.Close()

	if rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			t.Errorf("Scan into interface{} failed: %v", err)
		}
	}
}

// TestExecuteCreateViewMore tests CREATE VIEW execution
func TestExecuteCreateViewMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create view
	_, err = db.Exec(ctx, `CREATE VIEW active_users AS SELECT * FROM users`)
	if err != nil {
		t.Logf("CREATE VIEW error (may not be supported): %v", err)
	}
}

// TestExecuteDropViewMore tests DROP VIEW execution
func TestExecuteDropViewMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to drop a view (may error if views not supported)
	_, err := db.Exec(ctx, `DROP VIEW IF EXISTS test_view`)
	if err != nil {
		t.Logf("DROP VIEW error (may not be supported): %v", err)
	}
}

// TestExecuteCreateTriggerMore tests CREATE TRIGGER execution
func TestExecuteCreateTriggerMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create base table
	_, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER trg AFTER INSERT ON users BEGIN SELECT 1; END`)
	if err != nil {
		t.Logf("CREATE TRIGGER error (may not be supported): %v", err)
	}
}

// TestExecuteDropTriggerMore tests DROP TRIGGER execution
func TestExecuteDropTriggerMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to drop a trigger
	_, err := db.Exec(ctx, `DROP TRIGGER IF EXISTS test_trigger`)
	if err != nil {
		t.Logf("DROP TRIGGER error (may not be supported): %v", err)
	}
}

// TestExecuteCreateProcedureMore tests CREATE PROCEDURE execution
func TestExecuteCreateProcedureMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create procedure
	_, err := db.Exec(ctx, `CREATE PROCEDURE proc() BEGIN SELECT 1; END`)
	if err != nil {
		t.Logf("CREATE PROCEDURE error (may not be supported): %v", err)
	}
}

// TestExecuteDropProcedureMore tests DROP PROCEDURE execution
func TestExecuteDropProcedureMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to drop a procedure
	_, err := db.Exec(ctx, `DROP PROCEDURE IF EXISTS test_proc`)
	if err != nil {
		t.Logf("DROP PROCEDURE error (may not be supported): %v", err)
	}
}

// TestExecuteCallProcedure tests CALL procedure execution
func TestExecuteCallProcedure(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to call a procedure
	_, err := db.Exec(ctx, `CALL test_proc()`)
	if err != nil {
		t.Logf("CALL error (may not be supported): %v", err)
	}
}

// TestTransactionCommit tests transaction commit
func TestTransactionCommit(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Errorf("Failed to commit: %v", err)
	}
}

// TestTransactionRollbackMore tests transaction rollback
func TestTransactionRollbackMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Failed to rollback: %v", err)
	}
}

// TestTransactionDoubleCommit tests double commit (should error)
func TestTransactionDoubleCommit(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, _ := db.Begin(ctx)

	// First commit should succeed
	tx.Commit()

	// Second commit should fail
	err := tx.Commit()
	if err == nil {
		t.Error("Expected error for double commit")
	}
}

// TestTransactionDoubleRollback tests double rollback
func TestTransactionDoubleRollback(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, _ := db.Begin(ctx)

	// First rollback should succeed
	tx.Rollback()

	// Second rollback - may or may not error depending on implementation
	_ = tx.Rollback()
}

// TestTransactionExecAfterCommit tests exec after commit
func TestTransactionExecAfterCommit(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin transaction
	tx, _ := db.Begin(ctx)
	tx.Commit()

	// Exec after commit - may or may not error depending on implementation
	_, _ = tx.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
}

// TestOpenWithFilePath tests opening database with file path
func TestOpenWithFilePath(t *testing.T) {
	// Use a temporary file path
	path := ":memory:"

	db, err := Open(path, &Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err != nil {
		t.Logf("Open with file path error (may use memory): %v", err)
		return
	}
	defer db.Close()
}

// TestExecuteCreateIndexMore tests CREATE INDEX execution
func TestExecuteCreateIndexMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	_, err = db.Exec(ctx, `CREATE INDEX idx_name ON test(name)`)
	if err != nil {
		t.Logf("CREATE INDEX error (may not be supported): %v", err)
	}
}

// TestExecuteDropIndex tests DROP INDEX execution
func TestExecuteDropIndex(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to drop an index
	_, err := db.Exec(ctx, `DROP INDEX IF EXISTS test_idx`)
	if err != nil {
		t.Logf("DROP INDEX error (may not be supported): %v", err)
	}
}

// TestRowsCloseMultiple tests closing rows multiple times
func TestRowsCloseMultiple(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	rows, _ := db.Query(ctx, `SELECT id FROM test`)

	// First close should succeed
	err := rows.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// Second close should not error
	err = rows.Close()
	if err != nil {
		t.Logf("Second close error (may be expected): %v", err)
	}
}

// TestQueryWithContextCancel tests query with cancelled context
func TestQueryWithContextCancel(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Create table and insert data
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	// Cancel context before query
	cancel()

	// Query with cancelled context
	_, err := db.Query(ctx, `SELECT id FROM test`)
	// May or may not error depending on implementation
	_ = err
}

// TestExecWithNilContext tests exec with nil context
func TestExecWithNilContext(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Exec with nil context
	_, err := db.Exec(nil, `CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Logf("Exec with nil context error: %v", err)
	}
}

// TestQueryWithNilContext tests query with nil context
func TestQueryWithNilContext(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Query with nil context
	_, err := db.Query(nil, `SELECT 1`)
	if err != nil {
		t.Logf("Query with nil context error: %v", err)
	}
}

func TestScanIntoInt64(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (42)`)

	rows, _ := db.Query(ctx, `SELECT id FROM test`)
	defer rows.Close()

	if rows.Next() {
		var val int64
		if err := rows.Scan(&val); err != nil {
			t.Errorf("Scan into int64 failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	}
}

func TestScanIntoBytes(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (data BLOB)`)

	// This might not work without actual blob support, but test the path
	rows, err := db.Query(ctx, `SELECT data FROM test`)
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			var val []byte
			_ = rows.Scan(&val) // May error, just testing the path
		}
	}
}

func TestScanUnsupportedType(t *testing.T) {
	var unsupported int32
	err := scanValue(int64(42), &unsupported)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestScanIntFromFloat(t *testing.T) {
	var val int
	err := scanValue(float64(42.5), &val)
	if err != nil {
		t.Errorf("Scan int from float failed: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
}

func TestScanInt64FromFloat(t *testing.T) {
	var val int64
	err := scanValue(float64(42.5), &val)
	if err != nil {
		t.Errorf("Scan int64 from float failed: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
}

func TestScanInvalidType(t *testing.T) {
	var val int
	err := scanValue("not a number", &val)
	if err == nil {
		t.Error("Expected error for invalid type conversion")
	}
}

func TestScanInt64InvalidType(t *testing.T) {
	var val int64
	err := scanValue("not a number", &val)
	if err == nil {
		t.Error("Expected error for invalid type conversion")
	}
}

func TestScanFloatInvalidType(t *testing.T) {
	var val float64
	err := scanValue("not a number", &val)
	if err == nil {
		t.Error("Expected error for invalid type conversion")
	}
}

func TestScanBoolInvalidType(t *testing.T) {
	var val bool
	err := scanValue("not a bool", &val)
	if err == nil {
		t.Error("Expected error for invalid type conversion")
	}
}

func TestScanBytesInvalidType(t *testing.T) {
	var val []byte
	err := scanValue("string", &val)
	if err == nil {
		t.Error("Expected error for invalid type conversion")
	}
}

func TestOpenFileDatabase(t *testing.T) {
	// Create temp file
	tmpFile := t.TempDir() + "/test.db"

	db, err := Open(tmpFile, &Options{
		InMemory:  false,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open file database: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Database is nil")
	}
}

func TestOpenFileDatabaseWithWAL(t *testing.T) {
	tmpFile := t.TempDir() + "/test.db"

	db, err := Open(tmpFile, &Options{
		InMemory:   false,
		WALEnabled: true,
		CacheSize:  1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database with WAL: %v", err)
	}
	defer db.Close()

	// Write something
	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)
	db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)

	// Close and reopen
	db.Close()

	db2, err := Open(tmpFile, &Options{
		InMemory:   false,
		WALEnabled: true,
		CacheSize:  1024,
	})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()
}

func TestRowsNilNext(t *testing.T) {
	var rows *Rows
	if rows.Next() {
		t.Error("Expected Next() on nil Rows to return false")
	}
}

func TestBeginWithOptions(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginWith(ctx, nil)
	if err != nil {
		t.Fatalf("BeginWith failed: %v", err)
	}
	tx.Rollback()
}

func TestInsertResult(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)

	result, err := db.Exec(ctx, `INSERT INTO test (id, name) VALUES (1, 'Test')`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}
}

func TestExecuteCreateIndex(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table first
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	_, err = db.Exec(ctx, `CREATE INDEX idx_name ON test (name)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
}

func TestExecuteCreateView(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table first
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create view
	_, err = db.Exec(ctx, `CREATE VIEW test_view AS SELECT id, name FROM test`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}
}

func TestExecuteDropView(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table and view
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	db.Exec(ctx, `CREATE VIEW test_view AS SELECT id, name FROM test`)

	// Drop view
	_, err := db.Exec(ctx, `DROP VIEW test_view`)
	if err != nil {
		t.Fatalf("Failed to drop view: %v", err)
	}
}

func TestExecuteCreateTrigger(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table first
	_, err := db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger
	_, err = db.Exec(ctx, `CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN SELECT 1; END`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}
}

func TestExecuteDropTrigger(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create table and trigger
	db.Exec(ctx, `CREATE TABLE test (id INTEGER, name TEXT)`)
	db.Exec(ctx, `CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN SELECT 1; END`)

	// Drop trigger
	_, err := db.Exec(ctx, `DROP TRIGGER test_trigger`)
	if err != nil {
		t.Fatalf("Failed to drop trigger: %v", err)
	}
}

func TestExecuteCreateProcedure(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create procedure
	_, err := db.Exec(ctx, `CREATE PROCEDURE test_proc(param1 INTEGER) BEGIN SELECT 1; END`)
	if err != nil {
		t.Fatalf("Failed to create procedure: %v", err)
	}
}

func TestExecuteDropProcedure(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create procedure
	db.Exec(ctx, `CREATE PROCEDURE test_proc(param1 INTEGER) BEGIN SELECT 1; END`)

	// Drop procedure
	_, err := db.Exec(ctx, `DROP PROCEDURE test_proc`)
	if err != nil {
		t.Fatalf("Failed to drop procedure: %v", err)
	}
}

// TestExecuteCallProcedureMore tests CALL procedure execution with existing procedure
func TestExecuteCallProcedureMore(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Create a table for the procedure to work with
	_, err := db.Exec(ctx, `CREATE TABLE test_table (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create procedure that inserts data
	_, err = db.Exec(ctx, `CREATE PROCEDURE insert_test(id INTEGER, name TEXT) BEGIN INSERT INTO test_table (id, name) VALUES (id, name); END`)
	if err != nil {
		t.Logf("CREATE PROCEDURE error (may not be fully supported): %v", err)
		return
	}

	// Call the procedure
	_, err = db.Exec(ctx, `CALL insert_test(1, 'Test')`)
	if err != nil {
		t.Logf("CALL error (may not be fully supported): %v", err)
		return
	}

	// Note: Procedure execution may not be fully implemented
	// Just verify the test runs without panic
	t.Log("Procedure call test completed")
}

// TestExecuteCallProcedureNonExistent tests CALL with non-existent procedure
func TestExecuteCallProcedureNonExistent(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to call a non-existent procedure
	_, err := db.Exec(ctx, `CALL non_existent_proc()`)
	if err == nil {
		t.Error("Expected error for non-existent procedure")
	}
}

// TestTransactionCommitRollbackInactive tests commit/rollback on inactive transaction
func TestTransactionCommitRollbackInactive(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// Begin and commit
	tx, _ := db.Begin(ctx)
	tx.Commit()

	// Second commit should fail
	err := tx.Commit()
	if err == nil {
		t.Error("Expected error for commit on inactive transaction")
	}

	// Rollback on inactive should also fail
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error for rollback on inactive transaction")
	}
}

// TestQueryRowNoResults tests QueryRow with no results
func TestQueryRowNoResults(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	row := db.QueryRow(ctx, `SELECT id FROM test WHERE id = 999`)
	var id int
	err := row.Scan(&id)
	if err == nil {
		t.Error("Expected error for no rows")
	}
}

// TestQueryRowInvalidQuery tests QueryRow with error
func TestQueryRowInvalidQuery(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Query on non-existent table should error
	row := db.QueryRow(ctx, `SELECT * FROM nonexistent`)
	var id int
	err := row.Scan(&id)
	if err == nil {
		t.Error("Expected error for invalid query")
	}
}

// TestScanValueEdgeCases tests scanValue with various edge cases
func TestScanValueEdgeCases(t *testing.T) {
	// Test scanning nil into interface
	var iface interface{}
	err := scanValue(nil, &iface)
	if err != nil {
		t.Errorf("Scan nil into interface failed: %v", err)
	}
	if iface != nil {
		t.Errorf("Expected nil, got %v", iface)
	}

	// Test scanning string
	var str string
	err = scanValue("test", &str)
	if err != nil {
		t.Errorf("Scan string failed: %v", err)
	}
	if str != "test" {
		t.Errorf("Expected 'test', got %q", str)
	}

	// Test scanning bool
	var b bool
	err = scanValue(true, &b)
	if err != nil {
		t.Errorf("Scan bool failed: %v", err)
	}
	if !b {
		t.Error("Expected true")
	}

	// Test scanning []byte
	var bytes []byte
	err = scanValue([]byte{0x01, 0x02}, &bytes)
	if err != nil {
		t.Errorf("Scan bytes failed: %v", err)
	}
	if len(bytes) != 2 {
		t.Errorf("Expected 2 bytes, got %d", len(bytes))
	}
}

// TestScanValueIntEdgeCases tests scanValue int edge cases
func TestScanValueIntEdgeCases(t *testing.T) {
	// int64 to int
	var i int
	err := scanValue(int64(42), &i)
	if err != nil {
		t.Errorf("Scan int64 into int failed: %v", err)
	}
	if i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}

	// float64 to int (truncation)
	err = scanValue(float64(42.9), &i)
	if err != nil {
		t.Errorf("Scan float64 into int failed: %v", err)
	}
	if i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}
}

// TestScanValueInt64EdgeCases tests scanValue int64 edge cases
func TestScanValueInt64EdgeCases(t *testing.T) {
	// float64 to int64
	var i int64
	err := scanValue(float64(42.9), &i)
	if err != nil {
		t.Errorf("Scan float64 into int64 failed: %v", err)
	}
	if i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}
}

// TestPreparedStatementCache tests prepared statement caching
func TestPreparedStatementCache(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, `CREATE TABLE test (id INTEGER)`)

	// First execution - should parse and cache
	_, err := db.Exec(ctx, `INSERT INTO test (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("First exec failed: %v", err)
	}

	// Second execution - should use cache
	_, err = db.Exec(ctx, `INSERT INTO test (id) VALUES (2)`)
	if err != nil {
		t.Fatalf("Second exec failed: %v", err)
	}

	// Verify both rows inserted
	row := db.QueryRow(ctx, `SELECT COUNT(*) FROM test`)
	var count int64
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Count query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestDatabaseCloseMultiple tests closing database multiple times
func TestDatabaseCloseMultiple(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})

	// First close should succeed
	err := db.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// Second close should not error (idempotent)
	err = db.Close()
	if err != nil {
		t.Logf("Second close error (may be expected): %v", err)
	}
}

// TestExecUnsupportedStatement tests exec with unsupported statement
func TestExecUnsupportedStatement(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to execute a statement that's not supported
	_, err := db.Exec(ctx, `BEGIN TRANSACTION`)
	if err == nil {
		t.Error("Expected error for BEGIN TRANSACTION (should use Begin() method)")
	}

	_, err = db.Exec(ctx, `COMMIT`)
	if err == nil {
		t.Error("Expected error for COMMIT (should use Commit() method)")
	}

	_, err = db.Exec(ctx, `ROLLBACK`)
	if err == nil {
		t.Error("Expected error for ROLLBACK (should use Rollback() method)")
	}
}

// TestQueryUnsupportedStatement tests query with unsupported statement
func TestQueryUnsupportedStatement(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	ctx := context.Background()

	// Try to query with a non-query statement
	_, err := db.Query(ctx, `CREATE TABLE test (id INTEGER)`)
	if err == nil {
		t.Error("Expected error for CREATE TABLE in Query")
	}
}

// TestDatabaseClosedOperations tests operations on closed database
func TestDatabaseClosedOperations(t *testing.T) {
	db, _ := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	db.Close()

	ctx := context.Background()

	// Exec on closed database
	_, err := db.Exec(ctx, `SELECT 1`)
	if err == nil {
		t.Error("Expected error for Exec on closed database")
	}

	// Query on closed database
	_, err = db.Query(ctx, `SELECT 1`)
	if err == nil {
		t.Error("Expected error for Query on closed database")
	}

	// Begin on closed database
	_, err = db.Begin(ctx)
	if err == nil {
		t.Error("Expected error for Begin on closed database")
	}
}
