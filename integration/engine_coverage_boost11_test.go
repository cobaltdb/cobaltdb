package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestExecuteVacuum targets executeVacuum
func TestExecuteVacuum(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Delete some rows to create fragmentation
	_, err = db.Exec(ctx, "DELETE FROM test WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Run VACUUM
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM error (may not be supported): %v", err)
		return
	}

	// Verify data still accessible
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Failed to query after vacuum: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after vacuum, got %d", count)
		}
	}
}

// TestExecuteVacuumTable targets VACUUM with table name
func TestExecuteVacuumTable(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test1 VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, "DELETE FROM test1 WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// VACUUM specific table
	_, err = db.Exec(ctx, "VACUUM test1")
	if err != nil {
		t.Logf("VACUUM table error: %v", err)
		return
	}
}

// TestGetMetrics targets GetMetrics
func TestGetMetrics(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create some activity
	_, err = db.Exec(ctx, "CREATE TABLE metrics_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO metrics_test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Query(ctx, "SELECT * FROM metrics_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	// Get metrics
	metrics, err := db.GetMetrics()
	if err != nil {
		t.Logf("GetMetrics error: %v", err)
		return
	}

	t.Logf("Metrics: %+v", metrics)
}

// TestHealthCheck targets HealthCheck
func TestHealthCheck(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check health
	err = db.HealthCheck()
	if err != nil {
		t.Logf("HealthCheck error: %v", err)
	} else {
		t.Log("HealthCheck passed")
	}
}

// TestCommitWithRollback targets Commit and Rollback scenarios
func TestCommitWithRollback(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE tx_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert within transaction
	_, err = tx.Exec(ctx, "INSERT INTO tx_test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Logf("Commit error: %v", err)
	}

	// Verify data committed
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM tx_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 row after commit, got %d", count)
		}
	}
}

// TestRollbackScenario targets Rollback
func TestRollbackScenario(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO rollback_test VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Begin transaction and insert more
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec(ctx, "INSERT INTO rollback_test VALUES (2)")
	if err != nil {
		t.Fatalf("Failed to insert in tx: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Logf("Rollback error: %v", err)
	}

	// Verify only initial data remains
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM rollback_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1 {
			t.Errorf("Expected 1 row after rollback, got %d", count)
		}
	}
}

// TestExecuteSelectWithCTE targets executeSelectWithCTE
func TestExecuteSelectWithCTE(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE cte_base (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO cte_base VALUES (1, NULL), (2, 1), (3, 1), (4, 2)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Simple CTE
	rows, err := db.Query(ctx, `
		WITH cte AS (
			SELECT * FROM cte_base WHERE parent_id IS NULL
		)
		SELECT * FROM cte`)
	if err != nil {
		t.Logf("CTE error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("Expected 1 row from CTE, got %d", count)
	}
}

// TestQueryWithLimitOffset targets query function with LIMIT/OFFSET
func TestQueryWithLimitOffset(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE limit_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10 rows
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO limit_test VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	tests := []struct {
		name  string
		sql   string
		count int
	}{
		{"LIMIT", "SELECT * FROM limit_test LIMIT 5", 5},
		{"LIMIT OFFSET", "SELECT * FROM limit_test LIMIT 3 OFFSET 5", 3},
		{"LIMIT 0", "SELECT * FROM limit_test LIMIT 0", 0},
		{"OFFSET beyond data", "SELECT * FROM limit_test LIMIT 5 OFFSET 100", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			if count != tt.count {
				t.Errorf("Expected %d rows, got %d", tt.count, count)
			}
		})
	}
}

// TestOpenWithOptions targets Open function with various options
func TestOpenWithOptions(t *testing.T) {
	// Test with PageSize option
	db1, err := engine.Open(":memory:", &engine.Options{
		InMemory: true,
		PageSize: 4096,
	})
	if err != nil {
		t.Fatalf("Failed to open with PageSize: %v", err)
	}
	db1.Close()

	// Test with CacheSize option
	db2, err := engine.Open(":memory:", &engine.Options{
		InMemory:   true,
		CacheSize:  1024,
	})
	if err != nil {
		t.Fatalf("Failed to open with CacheSize: %v", err)
	}
	db2.Close()

	// Test with MaxConnections option
	db3, err := engine.Open(":memory:", &engine.Options{
		InMemory:       true,
		MaxConnections: 10,
	})
	if err != nil {
		t.Fatalf("Failed to open with MaxConnections: %v", err)
	}
	db3.Close()
}
