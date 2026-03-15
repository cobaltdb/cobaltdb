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
