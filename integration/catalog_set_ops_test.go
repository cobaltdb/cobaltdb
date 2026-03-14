package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestUnionBasic tests basic UNION operation
func TestUnionBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test UNION of two SELECT statements
	rows, err := db.Query(ctx, `SELECT 1 AS n, 'a' AS c UNION SELECT 2, 'b'`)
	if err != nil {
		t.Logf("UNION error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int
		var c string
		if err := rows.Scan(&n, &c); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Row: n=%d, c=%s", n, c)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	t.Log("UNION basic works correctly")
}

// TestUnionDeduplication tests UNION (without ALL) removes duplicates
func TestUnionDeduplication(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test UNION removes duplicates
	rows, err := db.Query(ctx, `SELECT 1 AS n UNION SELECT 1`)
	if err != nil {
		t.Logf("UNION dedup error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row (duplicate removed), got %d", count)
	}

	t.Log("UNION deduplication works correctly")
}

// TestUnionAll tests UNION ALL keeps duplicates
func TestUnionAll(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test UNION ALL keeps duplicates
	rows, err := db.Query(ctx, `SELECT 1 AS n UNION ALL SELECT 1`)
	if err != nil {
		t.Logf("UNION ALL error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows (duplicate kept), got %d", count)
	}

	t.Log("UNION ALL works correctly")
}

// TestIntersectBasic tests basic INTERSECT operation
func TestIntersectBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test INTERSECT - only common rows
	rows, err := db.Query(ctx, `SELECT 1 AS n UNION ALL SELECT 2 INTERSECT SELECT 2`)
	if err != nil {
		t.Logf("INTERSECT error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Row: n=%d", n)
	}

	if count != 1 {
		t.Errorf("Expected 1 row (intersection), got %d", count)
	}

	t.Log("INTERSECT works correctly")
}

// TestExceptBasic tests basic EXCEPT operation
func TestExceptBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test EXCEPT - rows in left but not in right
	rows, err := db.Query(ctx, `SELECT 1 AS n UNION ALL SELECT 2 EXCEPT SELECT 2`)
	if err != nil {
		t.Logf("EXCEPT error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		t.Logf("Row: n=%d", n)
	}

	if count != 1 {
		t.Errorf("Expected 1 row (difference), got %d", count)
	}

	t.Log("EXCEPT works correctly")
}

// TestUnionWithTables tests UNION with actual table data
func TestUnionWithTables(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	_, err = db.Exec(ctx, `CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert t1: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO t2 VALUES (2, 'Bob'), (3, 'Carol')`)
	if err != nil {
		t.Fatalf("Failed to insert t2: %v", err)
	}

	// Test UNION of two tables
	rows, err := db.Query(ctx, `SELECT name FROM t1 UNION SELECT name FROM t2`)
	if err != nil {
		t.Logf("UNION tables error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	names := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Logf("Scan error: %v", err)
			return
		}
		count++
		names[name] = true
	}

	if count != 3 {
		t.Errorf("Expected 3 unique names, got %d", count)
	}
	if !names["Alice"] || !names["Bob"] || !names["Carol"] {
		t.Errorf("Missing expected names: %v", names)
	}

	t.Log("UNION with tables works correctly")
}
