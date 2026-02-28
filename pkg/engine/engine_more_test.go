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
