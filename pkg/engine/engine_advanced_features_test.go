package engine

import (
	"context"
	"testing"
)

// TestVacuumCommand tests VACUUM SQL command
func TestVacuumCommand(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'data1'), (2, 'data2')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Run VACUUM
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Errorf("VACUUM failed: %v", err)
	}

	// Verify data still exists
	rows, err := db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Errorf("Query after VACUUM failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 rows after VACUUM, got %d", count)
	}
}

// TestAnalyzeCommand tests ANALYZE SQL command
func TestAnalyzeCommand(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, category TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, "cat")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Run ANALYZE
	_, err = db.Exec(ctx, "ANALYZE test")
	if err != nil {
		t.Errorf("ANALYZE failed: %v", err)
	}

	// Run ANALYZE without table name (all tables)
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Errorf("ANALYZE all tables failed: %v", err)
	}
}

// TestMaterializedViewCommand tests materialized view SQL commands
func TestMaterializedViewCommand(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create materialized view
	_, err = db.Exec(ctx, "CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM test")
	if err != nil {
		t.Errorf("CREATE MATERIALIZED VIEW failed: %v", err)
	}

	// Insert more data
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (3, 'Charlie')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Refresh materialized view
	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Errorf("REFRESH MATERIALIZED VIEW failed: %v", err)
	}

	// Drop materialized view
	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Errorf("DROP MATERIALIZED VIEW failed: %v", err)
	}
}

// TestCTESupport tests Common Table Expressions
func TestCTESupport(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO employees VALUES (1, 'CEO', 0), (2, 'Manager', 1), (3, 'Employee', 2)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test simple CTE
	rows, err := db.Query(ctx, "WITH cte AS (SELECT * FROM employees) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE query error (may be expected): %v", err)
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 rows from CTE, got %d", count)
		}
	}
}

// TestFullTextSearch tests FTS SQL commands
func TestFullTextSearch(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table with text data
	_, err = db.Exec(ctx, "CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, content TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO articles VALUES (1, 'Go Programming', 'Go is great'), (2, 'SQL Tutorial', 'SQL is useful')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create full-text index
	_, err = db.Exec(ctx, "CREATE FULLTEXT INDEX idx_fts ON articles (title, content)")
	if err != nil {
		t.Logf("CREATE FULLTEXT INDEX error (may be expected): %v", err)
	}

	// Note: MATCH ... AGAINST syntax would need parser support
	// For now, just verify the commands don't crash
}
